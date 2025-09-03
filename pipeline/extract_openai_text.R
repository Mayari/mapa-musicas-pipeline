# pipeline/extract_openai_text.R
# v3.3.0 text-only parse of OCR into {date, band, time}; strict month/year; retries; throttle
suppressPackageStartupMessages({
  library(httr2); library(jsonlite); library(cli); library(tibble)
  library(dplyr); library(purrr); library(lubridate); library(stringr); library(digest)
})

`%||%` <- function(a,b) if (!is.null(a)) a else b
.debug_dir <- function() getOption("mapa.debug_dir", NULL)
.throttle <- function() {
  sl <- suppressWarnings(as.numeric(Sys.getenv("OPENAI_THROTTLE_SEC","0")))
  if (!is.na(sl) && sl > 0) Sys.sleep(sl)
}

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

.parse_items_from_string <- function(raw) {
  # Accept either {"items":[...]} or a bare array of objects
  if (is.null(raw) || !nzchar(raw)) return(NULL)
  raw2 <- .strip_code(raw)
  parsed <- tryCatch(jsonlite::fromJSON(raw2), error=function(e) NULL)
  if (is.null(parsed)) {
    # try to trim to largest {...} block
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

# Build a strict JSON Schema for the Responses API
.schema_items <- function() {
  list(
    name = "gig_items",
    schema = list(
      type = "object",
      additionalProperties = FALSE,
      properties = list(
        items = list(
          type  = "array",
          items = list(
            type = "object",
            additionalProperties = FALSE,
            properties = list(
              date = list(type="string", pattern="^\\d{4}-\\d{2}-\\d{2}$"),
              band = list(type="string", minLength=1),
              time = list(type="string")  # allow "" when missing
            ),
            required = list("date","band","time")
          )
        )
      ),
      required = list("items")
    )
  )
}

# ---- OpenAI calls -------------------------------------------------------------

.call_responses_json <- function(key, model, user_text, system_msg, tag) {
  req <- request("https://api.openai.com/v1/responses") |>
    req_headers(Authorization=paste("Bearer", key), `Content-Type`="application/json") |>
    req_body_json(list(
      model = model,
      temperature = 0,
      max_output_tokens = 1800,
      response_format = list(type="json_schema", json_schema=.schema_items()),
      input = list(
        list(role="system", content=list(list(type="input_text", text=system_msg))),
        list(role="user",   content=list(list(type="input_text", text=user_text)))
      )
    ), auto_unbox = TRUE)

  resp <- tryCatch(req_perform(req), error=function(e) NULL)
  .throttle()

  if (is.null(resp)) return(NULL)
  body <- resp_body_json(resp, simplifyVector=TRUE)
  if (!is.null(.debug_dir())) {
    fp <- file.path(.debug_dir(), paste0("openai_text_responses_", tag, ".json"))
    writeLines(jsonlite::toJSON(body, auto_unbox=TRUE, pretty=TRUE), fp, useBytes=TRUE)
  }
  # Responses JSON: prefer output_text; if absent, scan output parts
  raw <- body$output_text %||% ""
  if (!nzchar(raw)) {
    raw <- tryCatch({
      parts <- body$output[[1]]$content
      idx <- which(vapply(parts, function(x) identical(x$type, "output_text"), logical(1)))
      if (length(idx)>0) parts[[idx[1]]]$text else ""
    }, error=function(e) "")
  }
  .parse_items_from_string(raw)
}

.call_chat_fc <- function(key, model, user_text, system_msg, tag) {
  tool_schema <- list(
    type="function",
    `function`=list(
      name="return_items",
      description="Return extracted events as strictly typed JSON",
      parameters=list(
        type="object",
        additionalProperties=FALSE,
        properties=list(
          items=list(
            type="array",
            items=list(
              type="object",
              additionalProperties=FALSE,
              properties=list(
                date=list(type="string"), band=list(type="string"), time=list(type="string")
              ),
              required=list("date","band","time")
            )
          )
        ),
        required=list("items")
      )
    )
  )

  req <- request("https://api.openai.com/v1/chat/completions") |>
    req_headers(Authorization=paste("Bearer", key), `Content-Type`="application/json") |>
    req_body_json(list(
      model=model, temperature=0, max_tokens=2000,
      messages=list(list(role="system", content=system_msg),
                    list(role="user",   content=user_text)),
      tools=list(tool_schema),
      tool_choice=list(type="function", `function`=list(name="return_items"))
    ), auto_unbox=TRUE)

  resp <- tryCatch(req_perform(req), error=function(e) NULL)
  .throttle()
  if (is.null(resp)) return(NULL)

  body <- resp_body_json(resp, simplifyVector=TRUE)
  if (!is.null(.debug_dir())) {
    fp <- file.path(.debug_dir(), paste0("openai_text_chat_", tag, ".json"))
    writeLines(jsonlite::toJSON(body, auto_unbox=TRUE, pretty=TRUE), fp, useBytes=TRUE)
  }
  args_json <- tryCatch(body$choices[[1]]$message$tool_calls[[1]]$`function`$arguments, error=function(e) NULL)
  if (!is.null(args_json) && nzchar(args_json)) {
    args <- tryCatch(jsonlite::fromJSON(args_json), error=function(e) NULL)
    if (!is.null(args$items)) return(args$items) else return(NULL)
  }
  raw <- tryCatch(body$choices[[1]]$message$content, error=function(e) "")
  .parse_items_from_string(raw)
}

# ---- One OCR → items ----------------------------------------------------------

.extract_one <- function(key, text, mo_es, yr, text_model, tag) {
  mo_num <- .month_es_to_num(mo_es)

  system_msg <- "Eres un extractor de carteleras musicales. Devuelve solo filas confiables; omite las dudosas."
  user_text <- paste(
    "Texto OCR (es/en):\n---\n", text, "\n---\n",
    if (!is.na(yr) && !is.na(mo_num))
      sprintf("Asume que el mes es '%s' y el año es %d. Incluye solo fechas dentro de ese mes/año.", mo_es, yr)
    else if (!is.na(mo_num))
      sprintf("Asume que el mes es '%s'. Incluye solo fechas dentro de ese mes.", mo_es)
    else
      "Si no hay mes/año claro, intenta deducirlo; si no estás seguro, omite la fila.",
    "\n\nDevuelve JSON estrictamente del tipo {\"items\": [{\"date\":\"YYYY-MM-DD\",\"band\":\"...\",\"time\":\"HH:MM\"|\"\"}]}",
    sep=""
  )

  # Try Responses JSON first, then Chat+tools; two total attempts if empty
  for (attempt in 1:2) {
    items <- .call_responses_json(key, text_model, user_text, system_msg, tag)
    if (!is.null(items) && length(items)) break
    items <- .call_chat_fc(key, text_model, user_text, system_msg, tag)
    if (!is.null(items) && length(items)) break
  }
  if (is.null(items) || !length(items)) return(NULL)

  # Canonicalize and filter programmatically by month/year
  out <- tibble::as_tibble(items) %>%
    transmute(
      event_date = suppressWarnings(lubridate::as_date(.data$date)),
      band_name  = as.character(.data$band %||% ""),
      event_time = as.character(.data$time %||% "")
    ) %>%
    filter(!is.na(event_date) & nzchar(band_name))

  if (!is.na(mo_num)) out <- dplyr::filter(out, lubridate::month(event_date) == mo_num)
  if (!is.na(yr))     out <- dplyr::filter(out, lubridate::year(event_date)  == yr)

  out
}

# ---- Public entry -------------------------------------------------------------

extract_openai_text <- function(df,
                                text_model = Sys.getenv("OPENAI_TEXT_MODEL","gpt-4.1")) {
  # df columns: source_image, ocr_text, month_name_es, year
  if (nrow(df)==0) {
    return(tibble(source_image=character(), event_date=as.Date(character()), band_name=character(), event_time=character()))
  }
  key <- Sys.getenv("OPENAI_API_KEY","")
  if (!nzchar(key)) {
    cli::cli_alert_warning("OPENAI_API_KEY not set. Text-only extraction will be skipped.")
    return(tibble(source_image=character(), event_date=as.Date(character()), band_name=character(), event_time=character()))
  }

  purrr::pmap_dfr(df, function(source_image, ocr_text, month_name_es, year) {
    tag <- tools::file_path_sans_ext(basename(source_image))
    rows <- .extract_one(key, ocr_text, month_name_es, year, text_model, tag)
    if (is.null(rows) || nrow(rows)==0) {
      return(tibble(source_image = source_image,
                    event_date   = as.Date(character()),
                    band_name    = character(),
                    event_time   = character()))
    }
    rows %>% mutate(source_image = !!source_image) %>%
      select(source_image, event_date, band_name, event_time)
  }) %>%
    distinct(source_image, event_date, band_name, event_time, .keep_all = TRUE)
}
