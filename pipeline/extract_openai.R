suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(tidyverse)
  library(glue)
})

message("[extract_openai.R] v2025-09-02 env-models + skip-image + schema-fallback + robust JSON parse")

# ---- API & model selection (env-driven) --------------------------------------
OPENAI_API_KEY <- Sys.getenv("OPENAI_API_KEY")

IMAGE_MODEL <- Sys.getenv("OPENAI_IMAGE_MODEL")
TEXT_MODEL  <- Sys.getenv("OPENAI_TEXT_MODEL")  # used by extract_openai_text.R, but we print it here for clarity
FALLBACK    <- Sys.getenv("OPENAI_MODEL")

if (!nzchar(IMAGE_MODEL)) IMAGE_MODEL <- FALLBACK
if (!nzchar(TEXT_MODEL))  TEXT_MODEL  <- FALLBACK
if (!nzchar(IMAGE_MODEL)) IMAGE_MODEL <- "gpt-4o"   # default for posters (vision)
if (!nzchar(TEXT_MODEL))  TEXT_MODEL  <- "gpt-4.1"  # default for OCR text

# Allow skipping the image pass entirely (recommended while we harden venue rules)
SKIP_IMAGE <- {
  v <- tolower(Sys.getenv("EXTRACT_IMAGE_PASS"))
  # disable on: off/0/false/no/skip (default is ON if unset)
  v %in% c("off","0","false","no","skip")
}

message(sprintf("[extract_openai.R] image_model=%s text_model=%s skip_image=%s",
                IMAGE_MODEL, TEXT_MODEL, SKIP_IMAGE))

# ---- Helpers -----------------------------------------------------------------
`%||%` <- function(a,b) if (!is.null(a) && !is.na(a) && (length(a) > 0)) a else b

img_to_data_uri <- function(path){
  ext <- tolower(tools::file_ext(path))
  mime <- if (ext %in% c("jpg","jpeg")) "image/jpeg" else if (ext %in% c("png")) "image/png" else "image/jpeg"
  raw <- readBin(path, what = "raw", n = file.info(path)$size)
  paste0("data:", mime, ";base64,", jsonlite::base64_enc(raw))
}

# Extract the first valid JSON object or array from a string
extract_json_snippet <- function(x){
  if (is.null(x) || is.na(x) || !nzchar(x)) return(NA_character_)
  ok <- tryCatch({ fromJSON(x); TRUE }, error = function(e) FALSE)
  if (ok) return(x)
  starts <- gregexpr("[\\[{]", x, perl = TRUE)[[1]]
  if (starts[1] == -1) return(NA_character_)
  for (s in starts){
    snippet <- substring(x, s, nchar(x))
    for (e in seq(nchar(snippet), max(1, nchar(snippet) - 4000), by = -1)){
      cand <- substring(snippet, 1, e)
      ok <- tryCatch({ fromJSON(cand); TRUE }, error = function(e) FALSE)
      if (ok) return(cand)
    }
  }
  NA_character_
}

make_prompt <- function(venue_name, year, month){
  glue(
"Devuelve SOLO JSON válido y minificado con este esquema exacto:
{{\"venue\":\"{venue_name}\",\"year\":{year},\"month\":{month},\"events\":[{{\"date\":\"YYYY-MM-DD\",\"band\":\"<artista/banda>\",\"event\":null,\"time\":null}}]}}

Reglas:
- Usa el mes={month} y año={year} si el día aparece sin mes/año explícitos.
- Extrae SOLO actuaciones (no precios, promos, horarios de puertas).
- Si hay residencias (p.ej. \"Miércoles de Salsa\"), usa las fechas EXACTAS junto al nombre de cada banda.
- Campos opcionales:
  - \"event\": nombre del ciclo/evento (p.ej. \"Miércoles de Salsa\", \"Valentine's Jazz Day\") si aplica.
  - \"time\": hora del show si aparece (formato libre como 9pm, 21:00, 8:30 PM).
- No incluyas texto, ni Markdown, ni comentarios. SOLO el objeto JSON."
  )
}

json_to_tibble <- function(x, image_path, default_venue){
  if (is.null(x) || is.na(x)) return(tibble())
  obj <- tryCatch(fromJSON(x, simplifyVector = TRUE), error = function(e) NULL)
  if (is.null(obj) || is.null(obj$events)) return(tibble())

  ven <- obj$venue %||% default_venue
  ev <- if (is.data.frame(obj$events)) obj$events else tibble::as_tibble(obj$events)
  if (!all(c("date","band") %in% names(ev))) return(tibble())

  ev %>%
    mutate(
      venue        = ven %||% default_venue,
      event_date   = suppressWarnings(as.Date(.data$date)),
      band_name    = as.character(.data$band),
      event_title  = if ("event" %in% names(.)) as.character(.data$event) else NA_character_,
      event_time   = if ("time"  %in% names(.)) as.character(.data$time)  else NA_character_,
      source_image = image_path,
      venue_id     = NA_character_
    ) %>%
    filter(!is.na(.data$event_date)) %>%
    select(venue, venue_id, event_date, band_name, event_title, event_time, source_image)
}

call_openai_chat_vis <- function(img_data_uri, prompt, model, enforce_json = TRUE){
  content <- list(
    list(type = "text", text = prompt),
    list(type = "image_url", image_url = list(url = img_data_uri))
  )
  body <- list(
    model = model,
    messages = list(list(role = "user", content = content))
  )
  if (enforce_json) body$response_format <- list(type = "json_object")

  req <- request("https://api.openai.com/v1/chat/completions") |>
    req_headers(Authorization = paste("Bearer", OPENAI_API_KEY),
                "Content-Type" = "application/json") |>
    req_body_json(body, auto_unbox = TRUE)

  resp <- tryCatch(req_perform(req), error = function(e) e)
  if (inherits(resp, "error")) return(list(status = NA_integer_, text = NA_character_))

  st  <- resp_status(resp)
  txt <- tryCatch({
    rj <- resp_body_json(resp)
    msg <- rj$choices[[1]]$message
    if (is.character(msg$content)) {
      msg$content
    } else if (is.list(msg$content) && length(msg$content) > 0 && !is.null(msg$content[[1]]$text)) {
      paste0(vapply(msg$content, function(p) p$text %||% "", character(1L)), collapse = "\n")
    } else {
      jsonlite::toJSON(msg, auto_unbox = TRUE)
    }
  }, error = function(e) NA_character_)
  list(status = st, text = txt)
}

# ---- Public: extract from ONE image (vision) ---------------------------------
extract_openai_events <- function(image_path, venue_name, year, month){
  if (!nzchar(OPENAI_API_KEY)) {
    message("[extract] OPENAI_API_KEY missing; skipping OpenAI image extraction.")
    return(tibble())
  }
  if (SKIP_IMAGE) {
    message("[extract] image pass disabled via EXTRACT_IMAGE_PASS; skipping.")
    return(tibble())
  }

  message(sprintf("[extract] start: %s", image_path))
  sz  <- tryCatch(file.info(image_path)$size, error = function(e) NA_real_)
  ext <- tolower(tools::file_ext(image_path) %||% "jpg")
  message(sprintf("[img] %s size=%s bytes ext=%s", image_path, format(sz, big.mark=","), ext))

  img <- tryCatch(img_to_data_uri(image_path), error = function(e) NA_character_)
  if (!nzchar(img)) { message("[extract] could not load/encode image; skipping."); return(tibble()) }
  message(sprintf("[extract] data_uri_len=%s", nchar(img)))

  prompt <- make_prompt(venue_name, year, month)

  message(sprintf("[extract] try #1 api=chat model=%s schema=TRUE", IMAGE_MODEL))
  r1 <- call_openai_chat_vis(img, prompt, IMAGE_MODEL, enforce_json = TRUE)
  message(sprintf("[extract] http_status=%s", r1$status %||% NA_integer_))
  if (!is.na(r1$status) && r1$status >= 200 && r1$status < 300) {
    txt  <- r1$text %||% ""
    snip <- extract_json_snippet(txt) %||% txt
    out  <- json_to_tibble(snip, image_path, venue_name)
    if (nrow(out)) return(out)
  } else {
    if (is.na(r1$status)) message("[extract] no HTTP status (transport/build error), moving to next mode")
  }

  message(sprintf("[extract] try #2 api=chat model=%s schema=FALSE", IMAGE_MODEL))
  r2 <- call_openai_chat_vis(img, prompt, IMAGE_MODEL, enforce_json = FALSE)
  message(sprintf("[extract] http_status=%s", r2$status %||% NA_integer_))
  if (!is.na(r2$status) && r2$status >= 200 && r2$status < 300) {
    txt  <- r2$text %||% ""
    snip <- extract_json_snippet(txt)
    out  <- json_to_tibble(snip, image_path, venue_name)
    if (nrow(out)) return(out)
  }

  tibble()
}
