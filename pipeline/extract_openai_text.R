suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(tidyverse)
  library(glue)
})

message("[extract_openai_text.R] v2025-08-31+gpt41+jsonobject+text")

sanitize_model <- function(x){
  x <- Sys.getenv(x, unset = "gpt-4.1")
  x <- trimws(x); x <- gsub('^"+|"+$', "", x)
  x
}
openai_model <- sanitize_model("OPENAI_MODEL")
`%||%` <- function(a,b) if (!is.null(a)) a else b

extract_openai_events_from_text <- function(ocr_text, venue_name, year, month){
  key <- Sys.getenv("OPENAI_API_KEY"); if (!nzchar(key)) return(tibble())
  if (!nzchar(ocr_text)) return(tibble())

  sys <- "Eres un extractor. Responde SOLO con JSON válido y minificado."
  user <- glue(
"Texto OCR de un cartel musical (mes {month}/{year} en {venue_name}). 
Devuelve este JSON:
{{\"venue\":\"{venue_name}\",\"year\":{year},\"month\":{month},\"events\":[{{\"date\":\"YYYY-MM-DD\",\"band\":\"<artista>\",\"event_title\":\"<opcional>\",\"time\":\"HH:MM\"}}]}}
Reglas:
- 'band' es el artista/banda, NO títulos genéricos (esos a 'event_title').
- Expande 'cada martes' etc.; reparte días cuando hay números por banda.
- Hora en 24h HH:MM si está clara; si no, omite 'time'.
- Ignora precios, reservas, patrocinadores."
  )

  body <- list(
    model = openai_model,
    messages = list(
      list(role="system", content=sys),
      list(role="user",   content=list(list(type="text", text=user))),
      list(role="user",   content=list(list(type="text", text=ocr_text)))
    ),
    temperature = 0,
    max_tokens = 1200,
    response_format = list(type="json_object")
  )

  req <- request("https://api.openai.com/v1/chat/completions") |>
    req_headers(Authorization = paste("Bearer", key), "Content-Type" = "application/json") |>
    req_body_json(body, auto_unbox = TRUE)

  resp <- tryCatch(req_perform(req), error=function(e) e)
  if (!inherits(resp, "httr2_response")) return(tibble())
  if (resp_status(resp) >= 300) return(tibble())

  res <- resp_body_json(resp)
  out_txt <- tryCatch(res$choices[[1]]$message$content, error=function(e) NA_character_)
  if (is.null(out_txt) || length(out_txt)==0 || is.na(out_txt) || !nzchar(out_txt)) return(tibble())
  parsed <- tryCatch(jsonlite::fromJSON(out_txt, simplifyVector = TRUE), error=function(e) NULL)
  if (is.null(parsed) || is.null(parsed$events)) return(tibble())

  tibble(
    venue        = parsed$venue %||% venue_name,
    venue_id     = NA_character_,
    event_date   = suppressWarnings(as.Date(parsed$events$date)),
    band_name    = parsed$events$band,
    event_title  = parsed$events$event_title %||% NA_character_,
    event_time   = parsed$events$time %||% NA_character_,
    source       = "openai-text",
    source_image = NA_character_
  ) |> filter(!is.na(event_date), nzchar(band_name))
}
