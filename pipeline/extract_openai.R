suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(tidyverse)
  library(glue)
})

message("[extract_openai.R] v2025-08-31+chatjson")

# --- config via env (optional) ---
get_num <- function(x, fallback) {
  v <- suppressWarnings(as.numeric(Sys.getenv(x, unset = as.character(fallback))))
  if (is.null(v) || length(v) == 0 || is.na(v) || !is.finite(v)) return(fallback)
  v
}
openai_model  <- Sys.getenv("OPENAI_MODEL", unset = "gpt-4o-mini")   # you can override with a secret
throttle_sec  <- get_num("OPENAI_THROTTLE_SEC", 2)

# Safe base64 for local image files (handles unknown/zero size)
img_to_data_uri <- function(path){
  if (!file.exists(path)) stop("Image not found: ", path)
  ext  <- tolower(tools::file_ext(path))
  mime <- ifelse(ext %in% c("jpg","jpeg"), "image/jpeg", "image/png")

  sz <- suppressWarnings(as.integer(file.info(path)$size))
  message(sprintf("[img] %s size=%s bytes ext=%s", path, ifelse(is.na(sz), "NA", as.character(sz)), ext))

  raw <- NULL
  if (!is.na(sz) && sz > 0) {
    raw <- readBin(path, what = "raw", n = sz)
  } else {
    message("[img] unknown size → streaming read")
    con <- file(path, "rb"); on.exit(close(con), add = TRUE)
    raw <- readBin(con, what = "raw", n = 1e8)  # up to 100MB
  }
  if (length(raw) == 0) stop("Zero-length image bytes for: ", path)
  paste0("data:", mime, ";base64,", jsonlite::base64_enc(raw))
}

# Retry helper for 429/5xx
perform_with_retry <- function(req, max_tries = 5){
  delay <- 1; last <- NULL
  for (i in seq_len(max_tries)){
    resp <- tryCatch(req_perform(req), error = function(e) e)
    last <- resp
    if (inherits(resp, "httr2_response")){
      st <- resp_status(resp)
      if (!st %in% c(429, 500:599)) return(resp)
      ra <- resp_header(resp, "retry-after"); if (!is.null(ra)) delay <- suppressWarnings(as.numeric(ra))
    }
    Sys.sleep(delay); delay <- min(delay * 2, 20)
  }
  if (inherits(last, "httr2_response")) return(last)
  stop(last)
}

`%||%` <- function(a,b) if (!is.null(a)) a else b

extract_openai_events <- function(image_path, venue_name, year, month){
  key <- Sys.getenv("OPENAI_API_KEY")
  if (!nzchar(key)) { message("[extract] no OPENAI_API_KEY → skip"); return(tibble()) }

  # gentle throttle to avoid 429
  if (!is.null(throttle_sec) && is.finite(throttle_sec) && throttle_sec > 0) Sys.sleep(throttle_sec)

  message("[extract] start: ", image_path)
  img <- img_to_data_uri(image_path)
  message("[extract] data_uri_len=", nchar(img))

  # System+user messages; force JSON mode
  sys <- "Eres un extractor que responde SOLO con JSON valido y minificado, sin explicaciones."
  user_prompt <- glue(
'Devuelve SOLO JSON minificado con este esquema:
{{"venue":"{venue_name}","year":{year},"month":{month},"events":[{{"date":"YYYY-MM-DD","band":"<artista/banda>"}}]}}
Reglas: si el día no tiene mes/año explícitos, usa month={month}, year={year}. Ignora precios, horas y patrocinadores.'
  )

  messages <- list(
    list(role="system", content=sys),
    list(
      role="user",
      content = list(
        list(type="text", text=user_prompt),
        list(type="image_url", image_url=list(url=img))
      )
    )
  )

  body <- list(
    model = openai_model,
    messages = messages,
    temperature = 0,
    max_tokens = 600,
    response_format = list(type="json_object")
  )

  payload <- jsonlite::toJSON(body, auto_unbox = TRUE, null = "null")
  message("[extract] payload_len=", nchar(payload))

  req <- request("https://api.openai.com/v1/chat/completions") |>
    req_headers(
      Authorization = paste("Bearer", key),
      "Content-Type" = "application/json"
    ) |>
    req_body_raw(charToRaw(payload))

  resp <- perform_with_retry(req, max_tries = 5)

  if (!inherits(resp, "httr2_response")) stop("HT error (no response object)")
  st <- resp_status(resp); message("[extract] http_status=", st)
  if (st >= 300){
    msg <- tryCatch(resp_body_string(resp), error = function(e) paste("status", st))
    message("OpenAI error for ", image_path, ": ", msg)
    return(tibble())
  }

  res <- resp_body_json(resp)
  out_txt <- tryCatch(res$choices[[1]]$message$content, error=function(e) NA_character_)

  if (is.null(out_txt) || length(out_txt)==0 || is.na(out_txt) || !nzchar(out_txt)) {
    message("[extract] empty chat content")
    return(tibble())
  }

  parsed <- tryCatch(jsonlite::fromJSON(out_txt, simplifyVector = TRUE), error=function(e) NULL)
  if (is.null(parsed) || is.null(parsed$events)) {
    message("[extract] JSON parse failed or no events")
    return(tibble())
  }

  tibble(
    venue        = parsed$venue %||% venue_name,
    venue_id     = NA_character_,
    event_date   = suppressWarnings(as.Date(parsed$events$date)),
    band_name    = parsed$events$band,
    source       = "openai",
    source_image = image_path
  )
}
