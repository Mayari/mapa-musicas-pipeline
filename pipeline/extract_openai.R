suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(tidyverse)
  library(glue)
})

# --- config via env (optional) ---
openai_model <- Sys.getenv("OPENAI_MODEL", unset = "gpt-4o-mini")
throttle_sec <- suppressWarnings(as.numeric(Sys.getenv("OPENAI_THROTTLE_SEC", unset = "2")))
if (is.na(throttle_sec)) throttle_sec <- 2

# Safe base64 for local image files (handles unknown/zero size)
img_to_data_uri <- function(path){
  if (!file.exists(path)) stop("Image not found: ", path)
  ext  <- tolower(tools::file_ext(path))
  mime <- ifelse(ext %in% c("jpg","jpeg"), "image/jpeg", "image/png")

  size <- suppressWarnings(as.integer(file.info(path)$size))
  if (!is.na(size) && size > 0) {
    raw <- readBin(path, what = "raw", n = size)
  } else {
    message("Warning: unknown file size, streaming read: ", path)
    con <- file(path, "rb"); on.exit(close(con), add = TRUE)
    raw <- readBin(con, what = "raw", n = 1e8)  # up to 100MB
  }
  if (length(raw) == 0) stop("Zero-length image bytes for: ", path)
  paste0("data:", mime, ";base64,", jsonlite::base64_enc(raw))
}

# Simple retry helper for 429/5xx with exponential backoff
perform_with_retry <- function(req, max_tries = 5){
  delay <- 1
  last <- NULL
  for (i in seq_len(max_tries)){
    resp <- tryCatch(req_perform(req), error = function(e) e)
    last <- resp
    if (inherits(resp, "httr2_response")){
      st <- resp_status(resp)
      if (!st %in% c(429, 500:599)) return(resp) # success or non-retryable
      ra <- resp_header(resp, "retry-after")
      if (!is.null(ra)) delay <- suppressWarnings(as.numeric(ra))
    }
    Sys.sleep(delay)
    delay <- min(delay * 2, 20)
  }
  if (inherits(last, "httr2_response")) return(last)
  stop(last)
}

`%||%` <- function(a,b) if (!is.null(a)) a else b

extract_openai_events <- function(image_path, venue_name, year, month){
  if (!nzchar(Sys.getenv("OPENAI_API_KEY"))) return(tibble())
  if (throttle_sec > 0) Sys.sleep(throttle_sec)

  img <- img_to_data_uri(image_path)

  prompt <- glue(
'Devuelve SOLO JSON minificado:
{{"venue":"{venue_name}","year":{year},"month":{month},"events":[{{"date":"YYYY-MM-DD","band":"<artista/banda>"}}]}}
Reglas: usa month={month} y year={year} si el cartel solo muestra días. Ignora precios/horas.'
  )

  body <- list(
    model = openai_model,
    input = list(list(
      role = "user",
      content = list(
        list(type = "input_text",  text = prompt),
        list(type = "input_image", image_url = img)
      )
    ))
  )

  req <- request("https://api.openai.com/v1/responses") |>
    req_headers(Authorization = paste("Bearer", Sys.getenv("OPENAI_API_KEY"))) |>
    req_body_json(body, auto_unbox = TRUE)

  resp <- perform_with_retry(req, max_tries = 5)

  if (resp_status(resp) >= 300){
    msg <- tryCatch(resp_body_string(resp), error = function(e) paste("status", resp_status(resp)))
    message("OpenAI error for ", image_path, ": ", msg)
    return(tibble())
  }

  resj <- resp_body_json(resp)
  out_txt <- resj$output_text %||% (tryCatch(resj$choices[[1]]$message$content[[1]]$text, error = function(e) NA))
  if (is.na(out_txt)) return(tibble())
  parsed <- tryCatch(jsonlite::fromJSON(out_txt, simplifyVector = TRUE), error=function(e) NULL)
  if (is.null(parsed) || is.null(parsed$events)) return(tibble())

  tibble(
    venue        = parsed$venue %||% venue_name,
    venue_id     = NA_character_,
    event_date   = suppressWarnings(as.Date(parsed$events$date)),
    band_name    = parsed$events$band,
    source       = "openai",
    source_image = image_path
  )
}
