suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(tidyverse)
  library(glue)
})

message("[extract_openai.R] v2025-09-01 env-models + schema-fallback + robust JSON parse")

# ---- API & model selection (env-driven) --------------------------------------
OPENAI_API_KEY <- Sys.getenv("OPENAI_API_KEY")

IMAGE_MODEL <- Sys.getenv("OPENAI_IMAGE_MODEL")
TEXT_MODEL  <- Sys.getenv("OPENAI_TEXT_MODEL")  # used by extract_openai_text.R, but we print it here for clarity
FALLBACK    <- Sys.getenv("OPENAI_MODEL")

if (!nzchar(IMAGE_MODEL)) IMAGE_MODEL <- FALLBACK
if (!nzchar(TEXT_MODEL))  TEXT_MODEL  <- FALLBACK
if (!nzchar(IMAGE_MODEL)) IMAGE_MODEL <- "gpt-4o"   # default for posters (vision)
if (!nzchar(TEXT_MODEL))  TEXT_MODEL  <- "gpt-4.1"  # default for OCR text

message(sprintf("[extract_openai.R] image_model=%s text_model=%s", IMAGE_MODEL, TEXT_MODEL))

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
  # Try direct parse
  ok <- tryCatch({ fromJSON(x); TRUE }, error = function(e) FALSE)
  if (ok) return(x)

  # Greedy scan for the first {...} or [...]
  # This is simplistic but works well enough for assistant replies that wrap JSON with text.
  starts <- gregexpr("[\\[{]", x, perl = TRUE)[[1]]
  if (starts[1] == -1) return(NA_character_)
  for (s in starts){
    snippet <- substring(x, s, nchar(x))
    # try to find a matching end by decreasing tail
    for (e in seq(nchar(snippet), max(1, nchar(snippet) - 4000), by = -1)){
      cand <- substring(snippet, 1, e)
      ok <- tryCatch({ fromJSON(cand); TRUE }, error = function(e) FALSE)
      if (ok) return(cand)
    }
  }
  NA_character_
}

# Build the common instruction for posters → JSON
make_prompt <- function(venue_name, year, month){
  glue(
"Devuelve SOLO JSON válido y minificado con este esquema exacto:
{{\"venue\":\"{venue_name}\",\"year\":{year},\"month\":{month},\"events\":[{{\"date\":\"YYYY-MM-DD\",\"band\":\"<artista/banda>\",\"event\":null,\"time\":null}}]}}

Reglas:
- Usa el mes={month} y año={year} si el día aparece sin mes/año explícitos.
- Extrae SOLO actuaciones (no precios, promos, horarios de puertas).
- Si aparece un bloque de residencias (p.ej. \"Miércoles de Salsa\"), asigna las fechas EXACTAS listadas junto al nombre de cada banda; no inventes días.
- Campos opcionales:
  - \"event\": nombre del ciclo/evento (p.ej. \"Miércoles de Salsa\", \"Valentine's Jazz Day\") si aplica.
  - \"time\": hora del show si aparece (formato libre como 9pm, 21:00, 8:30 PM).
- No incluyas texto, ni Markdown, ni comentarios. SOLO el objeto JSON."
  )
}

# Convert the model JSON into a tibble with the expected columns
json_to_tibble <- function(x, image_path, default_venue){
  if (is.null(x) || is.na(x)) return(tibble())
  obj <- tryCatch(fromJSON(x, simplifyVector = TRUE), error = function(e) NULL)
  if (is.null(obj)) return(tibble())
  if (is.null(obj$events)) return(tibble())

  # Accept either top-level venue/year/month or just events[]
  ven <- obj$venue %||% default_venue
  yr  <- suppressWarnings(as.integer(obj$year))
  mo  <- suppressWarnings(as.integer(obj$month))

  ev <- obj$events
  # normalize to data frame
  if (is.data.frame(ev)) {
    df <- ev
  } else if (is.list(ev)) {
    df <- tibble::as_tibble(ev)
  } else {
    return(tibble())
  }

  # Allow columns: date, band, event (optional), time (optional)
  if (!("date" %in% names(df)) || !("band" %in% names(df))) {
    return(tibble())
  }

  # Coerce and clean
  df <- df %>%
    mutate(
      venue        = ven %||% default_venue,
      event_date   = suppressWarnings(as.Date(.data$date)),
      band_name    = if ("band"  %in% names(.)) as.character(.data$band)  else NA_character_,
      event_title  = if ("event" %in% names(.)) as.character(.data$event) else NA_character_,
      event_time   = if ("time"  %in% names(.)) as.character(.data$time)  else NA_character_,
      source_image = image_path
    ) %>%
    select(venue, venue_id = dplyr::everything() %>% {NULL}, # placeholder; will add NA below
           event_date, band_name, event_title, event_time, source_image)

  if (!("venue_id" %in% names(df))) df$venue_id <- NA_character_

  # Keep only rows with at least a date (band may be completed by downstream rules)
  df <- df %>% filter(!is.na(.data$event_date))
  df
}

# Low-level request via Chat Completions (vision)
call_openai_chat_vis <- function(img_data_uri, prompt, model, enforce_json = TRUE){
  # Build messages payload
  content <- list(
    list(type = "text", text = prompt),
    list(type = "image_url", image_url = list(url = img_data_uri))
  )
  body <- list(
    model = model,
    messages = list(list(role = "user", content = content))
  )
  if (enforce_json) {
    body$response_format <- list(type = "json_object")
  }

  req <- request("https://api.openai.com/v1/chat/completions") |>
    req_headers(
      Authorization = paste("Bearer", OPENAI_API_KEY),
      "Content-Type" = "application/json"
    ) |>
    req_body_json(body, auto_unbox = TRUE)

  # Perform with graceful error capture
  resp <- tryCatch(req_perform(req), error = function(e) e)
  if (inherits(resp, "error")) {
    return(list(status = NA_integer_, text = NA_character_))
  }

  st <- resp_status(resp)
  txt <- tryCatch({
    rj <- resp_body_json(resp)
    # content may be a string or an array of content parts; prefer the string
    choice <- rj$choices[[1]]$message
    if (is.character(choice$content)) {
      choice$content
    } else if (is.list(choice$content) && length(choice$content) > 0 && !is.null(choice$content[[1]]$text)) {
      paste0(vapply(choice$content, function(p) p$text %||% "", character(1L)), collapse = "\n")
    } else {
      # last resort: collapse to raw JSON text
      jsonlite::toJSON(choice, auto_unbox = TRUE)
    }
  }, error = function(e) NA_character_)

  list(status = st, text = txt)
}

# ---- Public function: extract from ONE image ---------------------------------
# Returns a tibble with: venue, venue_id, event_date, band_name, event_title, event_time, source_image
extract_openai_events <- function(image_path, venue_name, year, month){
  if (!nzchar(OPENAI_API_KEY)) {
    message("[extract] OPENAI_API_KEY missing; skipping OpenAI image extraction.")
    return(tibble())
  }

  # Log + prepare image
  message(sprintf("[extract] start: %s", image_path))
  sz <- tryCatch(file.info(image_path)$size, error = function(e) NA_real_)
  ext <- tolower(tools::file_ext(image_path) %||% "jpg")
  message(sprintf("[img] %s size=%s bytes ext=%s", image_path, format(sz, big.mark=","), ext))

  img <- tryCatch(img_to_data_uri(image_path), error = function(e) NA_character_)
  if (!nzchar(img)) {
    message("[extract] could not load/encode image; skipping.")
    return(tibble())
  }
  message(sprintf("[extract] data_uri_len=%s", nchar(img)))

  prompt <- make_prompt(venue_name, year, month)

  # --- Try 1: chat + response_format=json_object (schema=TRUE)
  message(sprintf("[extract] try #1 api=chat model=%s schema=TRUE", IMAGE_MODEL))
  r1 <- call_openai_chat_vis(img, prompt, IMAGE_MODEL, enforce_json = TRUE)
  message(sprintf("[extract] http_status=%s", r1$status %||% NA_integer_))
  if (!is.na(r1$status) && r1$status >= 200 && r1$status < 300) {
    # First try to parse direct; else extract JSON snippet
    txt <- r1$text %||% ""
    snip <- extract_json_snippet(txt) %||% txt
    out <- json_to_tibble(snip, image_path, venue_name)
    if (nrow(out)) return(out)
  } else {
    if (is.na(r1$status)) message("[extract] no HTTP status (transport/build error), moving to next mode")
  }

  # --- Try 2: chat without forced JSON (schema=FALSE), then scrape JSON from text
  message(sprintf("[extract] try #2 api=chat model=%s schema=FALSE", IMAGE_MODEL))
  r2 <- call_openai_chat_vis(img, prompt, IMAGE_MODEL, enforce_json = FALSE)
  message(sprintf("[extract] http_status=%s", r2$status %||% NA_integer_))
  if (!is.na(r2$status) && r2$status >= 200 && r2$status < 300) {
    txt <- r2$text %||% ""
    snip <- extract_json_snippet(txt)
    out <- json_to_tibble(snip, image_path, venue_name)
    if (nrow(out)) return(out)
  }

  # If nothing worked, return empty tibble (downstream steps can still parse OCR/text)
  tibble()
}
