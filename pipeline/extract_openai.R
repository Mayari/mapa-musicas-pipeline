suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(tidyverse)
  library(glue)
})

message("[extract_openai.R] vBASE-image (vision only; no OCR)")

OPENAI_API_KEY <- Sys.getenv("OPENAI_API_KEY")
IMG_MODEL      <- Sys.getenv("OPENAI_IMAGE_MODEL")
FALLBACK       <- Sys.getenv("OPENAI_MODEL")
if (!nzchar(IMG_MODEL)) IMG_MODEL <- FALLBACK
if (!nzchar(IMG_MODEL)) IMG_MODEL <- "gpt-4o"

`%||%` <- function(a,b) if (!is.null(a) && !is.na(a) && length(a)>0 && nzchar(a)) a else b

img_to_data_uri <- function(path){
  ext <- tolower(tools::file_ext(path))
  if (!ext %in% c("jpg","jpeg","png")) {
    message(sprintf("[extract:image] skipping non-image file: %s", basename(path)))
    return(NA_character_)
  }
  mime <- ifelse(ext %in% c("jpg","jpeg"), "image/jpeg", "image/png")
  raw <- readBin(path, what = "raw", n = file.info(path)$size)
  paste0("data:", mime, ";base64,", jsonlite::base64_enc(raw))
}

make_prompt_image <- function(venue_name, year, month){
  glue(
"Convierte la cartelera mensual en JSON *minificado* con este esquema exacto:
{{\"venue\":\"{venue_name}\",\"year\":{year},\"month\":{month},\"events\":[{{\"date\":\"YYYY-MM-DD\",\"band\":\"<artista>\",\"time\":null,\"event\":null}}]}}

Reglas:
- Crea una entrada por cada **actuación con día del mes visible** y nombre claro.
- Si hay hora (21:00, 9pm, 20:30) ponla en \"time\"; si no, usa null.
- Si hay un título de evento (p. ej., \"Valentine’s Jazz Day\"), colócalo en \"event\"; si no, null.
- **No inventes**. Si dudas de la fecha o del nombre, omite esa entrada.
- Responde **solo** con el objeto JSON (sin comentarios/markdown)."
  )
}

extract_json_snippet <- function(x){
  if (is.null(x) || is.na(x) || !nzchar(x)) return(NA_character_)
  ok <- tryCatch({ fromJSON(x); TRUE }, error = function(e) FALSE)
  if (ok) return(x)
  starts <- gregexpr("[\\[{]", x, perl = TRUE)[[1]]
  if (starts[1] == -1) return(NA_character_)
  for (s in starts){
    snippet <- substring(x, s, nchar(x))
    for (e in seq(nchar(snippet), max(1, nchar(snippet)-4000), by=-1)){
      cand <- substring(snippet, 1, e)
      ok <- tryCatch({ fromJSON(cand); TRUE }, error = function(e) FALSE)
      if (ok) return(cand)
    }
  }
  NA_character_
}

json_to_tibble_image <- function(x, venue_name, image_path){
  if (is.null(x) || is.na(x)) return(tibble())
  obj <- tryCatch(fromJSON(x, simplifyVector = TRUE), error=function(e) NULL)
  if (is.null(obj) || is.null(obj$events)) return(tibble())

  ev <- if (is.data.frame(obj$events)) obj$events else tibble::as_tibble(obj$events)
  if (!("date" %in% names(ev) && "band" %in% names(ev))) return(tibble())

  ev %>%
    mutate(
      venue        = obj$venue %||% "",
      event_date   = suppressWarnings(as.Date(.data$date)),
      band_name    = as.character(.data$band),
      event_time   = if ("time"  %in% names(.)) as.character(.data$time)  else NA_character_,
      event_title  = if ("event" %in% names(.)) as.character(.data$event) else NA_character_,
      source_image = image_path,
      venue_id     = NA_character_
    ) %>%
    filter(!is.na(event_date), nzchar(band_name)) %>%
    select(venue, venue_id, event_date, band_name, event_time, event_title, source_image)
}

call_openai_chat_vision <- function(prompt, data_uri){
  body <- list(
    model = IMG_MODEL,
    messages = list(list(
      role = "user",
      content = list(
        list(type = "text", text = prompt),
        list(type = "image_url", image_url = list(url = data_uri))
      )
    )),
    response_format = list(type = "json_object")
  )

  req <- request("https://api.openai.com/v1/chat/completions") |>
    req_headers(Authorization = paste("Bearer", OPENAI_API_KEY),
                "Content-Type" = "application/json") |>
    req_body_json(body, auto_unbox = TRUE)

  resp <- tryCatch(req_perform(req), error=function(e) e)
  if (inherits(resp, "error")) return(list(status = NA_integer_, text = NA_character_))
  st <- resp_status(resp)
  txt <- tryCatch({
    rj <- resp_body_json(resp)
    msg <- rj$choices[[1]]$message
    if (is.character(msg$content)) msg$content else
      if (is.list(msg$content) && length(msg$content)>0 && !is.null(msg$content[[1]]$text))
        paste0(vapply(msg$content, function(p) p$text %||% "", character(1L)), collapse="\n")
      else jsonlite::toJSON(msg, auto_unbox = TRUE)
  }, error=function(e) NA_character_)
  list(status = st, text = txt)
}

# Public API: extract from image directly (no OCR)
extract_openai_events <- function(image_path, venue_name, year, month){
  if (!nzchar(OPENAI_API_KEY)) {
    message("[extract:image] OPENAI_API_KEY missing; skipping.")
    return(tibble())
  }
  data_uri <- img_to_data_uri(image_path)
  if (is.na(data_uri)) return(tibble())
  prompt <- make_prompt_image(venue_name, year, month)

  r <- call_openai_chat_vision(prompt, data_uri)
  message(sprintf("[image] http_status=%s | %s", r$status %||% NA_integer_, basename(image_path)))

  if (is.na(r$status) || r$status < 200 || r$status >= 300) return(tibble())
  snip <- extract_json_snippet(r$text %||% "")
  out  <- json_to_tibble_image(snip, venue_name, image_path)
  distinct(out, venue, event_date, band_name, event_time, event_title, source_image, .keep_all = TRUE)
}
