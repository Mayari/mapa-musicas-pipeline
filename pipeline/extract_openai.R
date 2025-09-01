suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(tidyverse)
  library(glue)
})

message("[extract_openai.R] v2025-08-31+chatjson+residencias+time")

# --- config via env (optional) ---
get_num <- function(x, fallback) {
  v <- suppressWarnings(as.numeric(Sys.getenv(x, unset = as.character(fallback))))
  if (is.null(v) || length(v) == 0 || is.na(v) || !is.finite(v)) return(fallback)
  v
}
openai_model  <- Sys.getenv("OPENAI_MODEL", unset = "gpt-4o-mini")
throttle_sec  <- get_num("OPENAI_THROTTLE_SEC", 2)

# Read image → base64 data URI (robust to unknown sizes)
img_to_data_uri <- function(path){
  if (!file.exists(path)) stop("Image not found: ", path)
  ext  <- tolower(tools::file_ext(path))
  mime <- ifelse(ext %in% c("jpg","jpeg"), "image/jpeg", "image/png")
  sz <- suppressWarnings(as.integer(file.info(path)$size))
  raw <- if (!is.na(sz) && sz > 0) readBin(path, "raw", n = sz) else {
    con <- file(path, "rb"); on.exit(close(con), add = TRUE); readBin(con, "raw", n = 1e8)
  }
  if (length(raw) == 0) stop("Zero-length image bytes for: ", path)
  paste0("data:", mime, ";base64,", jsonlite::base64_enc(raw))
}

# Retry helper for 429/5xx
perform_with_retry <- function(req, max_tries = 5){
  delay <- 1; last <- NULL
  for (i in seq_len(max_tries)){
    resp <- tryCatch(req_perform(req), error = function(e) e); last <- resp
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
  key <- Sys.getenv("OPENAI_API_KEY"); if (!nzchar(key)) return(tibble())
  if (!is.null(throttle_sec) && is.finite(throttle_sec) && throttle_sec > 0) Sys.sleep(throttle_sec)

  img <- img_to_data_uri(image_path)

  # SYSTEM: JSON only; expand residencias; return 24h time
  sys <- "Eres un extractor. Responde SOLO con JSON válido y minificado, sin texto extra."

  # USER PROMPT: rules for residencias + times
  user_prompt <- glue(
"Devuelve SOLO JSON minificado con este esquema:
{{\"venue\":\"{venue_name}\",\"year\":{year},\"month\":{month},\"events\":[{{\"date\":\"YYYY-MM-DD\",\"band\":\"<artista/banda>\",\"time\":\"HH:MM\"}}]}}
Reglas:
- Extrae TODOS los conciertos del cartel.
- Si dice \"cada martes\"/\"todos los miércoles\"/\"todos los <día>\", crea un evento por CADA fecha de ese día en el mes indicado.
- Si hay varias bandas para un mismo día fijo (p.ej. \"miércoles de salsa\" con fechas repartidas), usa los números de día cerca de cada banda para asignar.
- Si un texto de residencia no trae números y hay UNA banda, asígnala a todos esos días.
- Si hay DOS o más bandas pero NO hay números que repartan, no inventes: omite esos casos ambiguos.
- Hora: detecta formatos como \"8 pm\", \"20:30\", \"20 h\", \"20 hrs\", \"20:30h\". Devuelve HH:MM en 24h. Si la hora está en la misma línea o cerca del artista/fecha, úsala; si hay varias, elige la más cercana. Si no hay hora, omite el campo en ese evento.
- Ignora precios/cover, reservaciones, hashtags y patrocinadores.")

  messages <- list(
    list(role="system", content=sys),
    list(role="user", content=list(
      list(type="text", text=user_prompt),
      list(type="image_url", image_url=list(url=img))
    ))
  )

  body <- list(
    model = openai_model,
    messages = messages,
    temperature = 0,
    max_tokens = 900,
    response_format = list(type="json_object")
  )

  payload <- jsonlite::toJSON(body, auto_unbox = TRUE, null = "null")
  req <- request("https://api.openai.com/v1/chat/completions") |>
    req_headers(Authorization = paste("Bearer", key), "Content-Type" = "application/json") |>
    req_body_raw(charToRaw(payload))

  resp <- perform_with_retry(req, max_tries = 5)
  if (!inherits(resp, "httr2_response")) stop("HTTP error (no response)")
  if (resp_status(resp) >= 300){
    message("OpenAI error for ", image_path, ": ", tryCatch(resp_body_string(resp), error=function(e) ""))
    return(tibble())
  }

  res <- resp_body_json(resp)
  out_txt <- tryCatch(res$choices[[1]]$message$content, error=function(e) NA_character_)
  if (is.null(out_txt) || length(out_txt)==0 || is.na(out_txt) || !nzchar(out_txt)) return(tibble())

  parsed <- tryCatch(jsonlite::fromJSON(out_txt, simplifyVector = TRUE), error=function(e) NULL)
  if (is.null(parsed) || is.null(parsed$events)) return(tibble())

  # allow missing time
  tibble(
    venue        = parsed$venue %||% venue_name,
    venue_id     = NA_character_,
    event_date   = suppressWarnings(as.Date(parsed$events$date)),
    band_name    = parsed$events$band,
    event_time   = parsed$events$time %||% NA_character_,
    source       = "openai",
    source_image = image_path
  ) |> filter(!is.na(event_date), nzchar(band_name))
}
