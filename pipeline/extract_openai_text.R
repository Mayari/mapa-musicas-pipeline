# pipeline/extract_openai_text.R
# v3.0.0 text-only parse of OCR into {date, band, time}; strict month/year
suppressPackageStartupMessages({
  library(httr2); library(jsonlite); library(cli); library(tibble)
  library(dplyr); library(purrr); library(lubridate); library(stringr)
})

`%||%` <- function(a,b) if (!is.null(a)) a else b
.debug_dir <- function() getOption("mapa.debug_dir", NULL)

.month_es_to_num <- function(m) {
  if (is.null(m) || is.na(m)) return(NA_integer_)
  m <- tolower(str_trim(m))
  dict <- c("enero"=1,"febrero"=2,"marzo"=3,"abril"=4,"mayo"=5,"junio"=6,
            "julio"=7,"agosto"=8,"septiembre"=9,"setiembre"=9,"octubre"=10,"noviembre"=11,"diciembre"=12)
  dict[[m]] %||% NA_integer_
}

.strip_code <- function(x) {
  x <- gsub("^\\s*```(json)?\\s*", "", x); gsub("\\s*```\\s*$", "", x)
}
.parse_items <- function(raw) {
  if (is.null(raw) || !nzchar(raw)) return(NULL)
  raw2 <- .strip_code(raw)
  parsed <- tryCatch(jsonlite::fromJSON(raw2), error=function(e) NULL)
  if (is.null(parsed)) {
    # salvage largest {...}
    open <- regexpr("\\{", raw2); closes <- gregexpr("\\}", raw2)[[1]]
    if (open[1] != -1 && length(closes)>0 && closes[1]!=-1) {
      maybe <- substr(raw2, open[1], closes[length(closes)])
      parsed <- tryCatch(jsonlite::fromJSON(maybe), error=function(e) NULL)
    }
  }
  if (is.null(parsed)) return(NULL)
  if (!is.null(parsed$items)) return(parsed$items)
  if (is.list(parsed) && !is.null(parsed[[1]]$date)) return(parsed)
  NULL
}

# Prefer Responses API for text-only; fall back to Chat if needed
.extract_one <- function(key, text, mo_es, yr, text_model) {
  mo_num <- .month_es_to_num(mo_es)

  system_msg <- "Eres un extractor de cartelera musical. Devuelve solo datos confiables; omite filas dudosas."
  user_text <- paste(
    "Texto OCR de una cartelera (es/en):\n---\n", text, "\n---\n",
    if (!is.na(yr) && !is.na(mo_num))
      sprintf("Asume que el mes es '%s' y el año es %d. Incluye solo fechas dentro de ese mes/año.", mo_es, yr)
    else if (!is.na(mo_num))
      sprintf("Asume que el mes es '%s'. Incluye solo fechas dentro de ese mes.", mo_es)
    else
      "Si no hay mes/año claro, intenta deducirlo; si no estás seguro, omite la fila.",
    "\n\nDevuelve JSON estricto:\n{ \"items\": [ { \"date\": \"YYYY-MM-DD\", \"band\": \"...\", \"time\": \"HH:MM\" | \"\" } ] }",
    sep=""
  )

  # 1) Responses API
  req1 <- request("https://api.openai.com/v1/responses") |>
    req_headers(Authorization=paste("Bearer", key), `Content-Type`="application/json") |>
    req_body_json(list(
      model = text_model,
      temperature = 0,
      max_output_tokens = 1200,
      response_format = list(type="json_object"),
      input = list(list(role="system", content=list(list(type="input_text", text=system_msg))),
                   list(role="user",   content=list(list(type="input_text", text=user_text))))
    ), auto_unbox = TRUE)
  resp1 <- tryCatch(req_perform(req1), error=function(e) NULL)

  raw <- ""
  if (!is.null(resp1)) {
    js <- tryCatch(resp_body_json(resp1, simplifyVector=TRUE), error=function(e) NULL)
    raw <- js$output_text %||% ""
    if (!is.null(.debug_dir()) && nzchar(raw))
      writeLines(raw, file.path(.debug_dir(), paste0("openai_text_resp_", substr(digest::digest(text),1,8), ".txt")))
    items <- .parse_items(raw)
    if (!is.null(items)) return(items)
  }

  # 2) Fallback: Chat
  req2 <- request("https://api.openai.com/v1/chat/completions") |>
    req_headers(Authorization=paste("Bearer", key), `Content-Type`="application/json") |>
    req_body_json(list(
      model = text_model,
      temperature = 0,
      max_tokens = 1200,
      response_format = list(type="json_object"),
      messages = list(
        list(role="system", content=system_msg),
        list(role="user",   content=user_text)
      )
    ), auto_unbox = TRUE)
  resp2 <- tryCatch(req_perform(req2), error=function(e) NULL)
  if (is.null(resp2)) return(NULL)
  js2 <- tryCatch(resp_body_json(resp2, simplifyVector=TRUE), error=function(e) NULL)
  raw2 <- tryCatch(js2$choices[[1]]$message$content, error=function(e) "")
  if (!is.null(.debug_dir()) && nzchar(raw2))
    writeLines(raw2, file.path(.debug_dir(), paste0("openai_text_chat_", substr(digest::digest(text),1,8), ".txt")))
  .parse_items(raw2)
}

extract_openai_text <- function(df,
                                text_model = Sys.getenv("OPENAI_TEXT_MODEL","gpt-4.1")) {
  # df columns: source_image, ocr_text, month_name_es, year
  if (nrow(df)==0) return(tibble(source_image=character(), event_date=as.Date(character()), band_name=character(), event_time=character()))
  key <- Sys.getenv("OPENAI_API_KEY","")
  use_regex <- FALSE
  if (!nzchar(key)) {
    cli::cli_alert_warning("OPENAI_API_KEY not set. Text-only extraction will be skipped.")
    return(tibble(source_image=character(), event_date=as.Date(character()), band_name=character(), event_time=character()))
  }

  rows <- purrr::pmap_dfr(df, function(source_image, ocr_text, month_name_es, year) {
    items <- .extract_one(key, ocr_text, month_name_es, year, text_model)
    if (is.null(items) || length(items)==0) {
      return(tibble(source_image = source_image,
                    event_date   = as.Date(character()),
                    band_name    = character(),
                    event_time   = character()))
    }
    tibble::as_tibble(items) %>%
      transmute(
        source_image = !!source_image,
        event_date   = suppressWarnings(lubridate::as_date(.data$date)),
        band_name    = .data$band %||% "",
        event_time   = .data$time %||% ""
      ) %>%
      filter(!is.na(event_date) & nzchar(band_name))
  })

  # De-dup per poster/date/band/time
  rows %>% distinct(source_image, event_date, band_name, event_time, .keep_all = TRUE)
}
