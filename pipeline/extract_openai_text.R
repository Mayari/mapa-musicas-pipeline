suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(tidyverse)
})

message("[extract_openai_text.R] vBASE-no-glue (text-only; band/date/time; optional event_title)")

OPENAI_API_KEY <- Sys.getenv("OPENAI_API_KEY")
TEXT_MODEL     <- Sys.getenv("OPENAI_TEXT_MODEL")
FALLBACK       <- Sys.getenv("OPENAI_MODEL")
if (!nzchar(TEXT_MODEL)) TEXT_MODEL <- FALLBACK
if (!nzchar(TEXT_MODEL)) TEXT_MODEL <- "gpt-4.1"

`%||%` <- function(a,b) if (!is.null(a) && !is.na(a) && length(a)>0 && nzchar(a)) a else b

make_prompt_text <- function(venue_name, year, month, ocr_chars){
  ocr <- substr(ocr_chars %||% "", 1, 12000)
  sprintf(
'Convierte este texto de una cartelera en JSON minificado con:
{"venue":"%s","year":%d,"month":%d,"events":[{"date":"YYYY-MM-DD","band":"<artista>","time":null,"event":null}]}
Reglas: una entrada por actuación con día visible y nombre claro. Si hay hora, ponla en "time"; si no, null. Si hay título de evento, en "event"; si no, null. No inventes; si dudas, omite. SOLO devuelve ese JSON.
---
%s
---',
    venue_name, as.integer(year), as.integer(month), ocr
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

json_to_tibble_text <- function(x, venue_name){
  if (is.null(x) || is.na(x)) return(tibble())
  obj <- tryCatch(fromJSON(x, simplifyVector = TRUE), error=function(e) NULL)
  if (is.null(obj) || is.null(obj$events)) return(tibble())

  ev <- if (is.data.frame(obj$events)) obj$events else tibble::as_tibble(obj$events)
  if (!("date" %in% names(ev) && "band" %in% names(ev))) return(tibble())

  ev %>%
    mutate(
      venue        = obj$venue %||% venue_name,
      event_date   = suppressWarnings(as.Date(.data$date)),
      band_name    = as.character(.data$band),
      event_time   = if ("time"  %in% names(.)) as.character(.data$time)  else NA_character_,
      event_title  = if ("event" %in% names(.)) as.character(.data$event) else NA_character_,
      venue_id     = NA_character_
    ) %>%
    filter(!is.na(event_date), nzchar(band_name)) %>%
    select(venue, venue_id, event_date, band_name, event_time, event_title)
}

call_openai_chat_text <- function(prompt){
  body <- list(
    model = TEXT_MODEL,
    messages = list(list(role="user", content=list(list(type="text", text=prompt)))),
    response_format = list(type="json_object")
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

# Public: text-only extraction from OCR string
extract_openai_events_from_text <- function(ocr_text, venue_name, year, month){
  if (!nzchar(OPENAI_API_KEY)) {
    message("[extract_text] OPENAI_API_KEY missing; skipping.")
    return(tibble())
  }
  prmpt <- make_prompt_text(venue_name, year, month, ocr_text %||% "")
  r <- call_openai_chat_text(prmpt)
  message(sprintf("[extract_text] http_status=%s", r$status %||% NA_integer_))
  if (is.na(r$status) || r$status < 200 || r$status >= 300) return(tibble())
  snip <- extract_json_snippet(r$text %||% "")
  out  <- json_to_tibble_text(snip, venue_name)
  distinct(out, venue, event_date, band_name, event_time, event_title, .keep_all = TRUE)
}

# alias for callers that used the older name
extract_openai_text_events <- extract_openai_events_from_text
