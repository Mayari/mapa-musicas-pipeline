# pipeline/extract_openai.R
# v2.3.0 Minimal direct image extraction using OpenAI (strict month/year)
# - Uses Chat Completions with proper multimodal content:
#   content = [{type:"text",...}, {type:"image_url", image_url:{url:...}}]
# - Adds response_format=json_object, HTTP status checks, and debug dumps.

suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(base64enc)
  library(cli)
  library(tibble)
  library(purrr)
  library(dplyr)
  library(lubridate)
  library(stringr)
})

`%||%` <- function(a, b) if (!is.null(a)) a else b
.debug_dir <- function() getOption("mapa.debug_dir", NULL)

.month_es_to_num <- function(m) {
  if (is.null(m) || is.na(m)) return(NA_integer_)
  m <- tolower(str_trim(m))
  dict <- c("enero"=1,"febrero"=2,"marzo"=3,"abril"=4,"mayo"=5,"junio"=6,
            "julio"=7,"agosto"=8,"septiembre"=9,"setiembre"=9,"octubre"=10,"noviembre"=11,"diciembre"=12)
  dict[[m]] %||% NA_integer_
}

.strip_code_fences <- function(x) {
  x <- gsub("^\\s*```(json)?\\s*", "", x)
  gsub("\\s*```\\s*$", "", x)
}

extract_openai <- function(image_paths, month_es, year, source_image_ids = NULL,
                           model = Sys.getenv("OPENAI_IMAGE_MODEL", unset = "gpt-4o")) {
  if (length(image_paths) == 0) {
    return(tibble(source_image=character(), event_date=as.Date(character()), band_name=character(), event_time=character()))
  }
  key <- Sys.getenv("OPENAI_API_KEY", "")
  if (!nzchar(key)) cli::cli_abort("OPENAI_API_KEY not set.")
  if (is.null(source_image_ids)) source_image_ids <- image_paths

  out_rows <- purrr::map2_dfr(seq_along(image_paths), image_paths, function(i, pth) {
    mo     <- month_es[i] %||% NA_character_
    yr     <- suppressWarnings(as.integer(year[i]))
    src    <- source_image_ids[i]
    mo_num <- .month_es_to_num(mo)

    # Data URL (keeps everything local to request)
    ext  <- tolower(tools::file_ext(pth))
    mime <- if (ext %in% c("jpg","jpeg")) "image/jpeg" else if (ext=="png") "image/png" else "image/*"
    bytes <- readBin(pth, what="raw", n=file.info(pth)$size)
    b64   <- base64enc::base64encode(bytes)
    data_url <- paste0("data:", mime, ";base64,", b64)

    assume_line <- if (!is.na(yr) && !is.na(mo_num)) {
      sprintf("Assume the month is '%s' (Spanish) and the year is %d. Only include dates within that month/year; omit anything outside.", mo, yr)
    } else if (!is.na(mo_num)) {
      sprintf("Assume the month is '%s' (Spanish). Only include dates within that month.", mo)
    } else {
      "Use the visible month/year; if uncertain, omit the row."
    }

    system_msg <- "You extract structured gig listings directly from venue posters. Be conservative and omit doubtful rows."
    user_text  <- paste(
      "Extract only confident events and return STRICT JSON (no commentary).",
      assume_line,
      "Schema:",
      '{ "items": [ { "date": "YYYY-MM-DD", "band": "...", "time": "HH:MM" | "" } ] }',
      "Rules:",
      "- Do not invent bands or times.",
      "- If time is missing, set it to empty string.",
      "- If multiple bands share the same date, return multiple items.",
      "- If you are not sure about a row, omit it.",
      sep = "\n"
    )

    req <- request("https://api.openai.com/v1/chat/completions") |>
      req_headers(Authorization = paste("Bearer", key),
                  `Content-Type` = "application/json") |>
      req_body_json(list(
        model = model,
        temperature = 0,
        max_tokens = 400,
        response_format = list(type = "json_object"),
        messages = list(
          list(role = "system", content = system_msg),
          list(role = "user", content = list(
            list(type = "text", text = user_text),
            list(type = "image_url", image_url = list(url = data_url))
          ))
        )
      ), auto_unbox = TRUE)

    resp <- tryCatch(req_perform(req), error = function(e) e)
    if (inherits(resp, "error")) {
      cli::cli_alert_danger("OpenAI image HTTP/client error for {basename(pth)}: {resp$message}")
      if (!is.null(.debug_dir()))
        writeLines(paste("CLIENT_ERROR:", resp$message),
                   file.path(.debug_dir(), paste0("openai_image_error_", basename(pth), ".txt")))
      return(tibble(source_image = src, event_date = as.Date(character()), band_name = character(), event_time = character()))
    }

    status   <- resp_status(resp)
    raw_body <- tryCatch(resp_body_string(resp), error=function(e) "")
    if (!is.null(.debug_dir()))
      writeLines(raw_body, file.path(.debug_dir(), paste0("openai_image_", basename(pth), ".json")))
    if (status >= 400) {
      cli::cli_alert_danger("OpenAI image HTTP {status} for {basename(pth)}")
      return(tibble(source_image = src, event_date = as.Date(character()), band_name = character(), event_time = character()))
    }

    cont <- tryCatch(jsonlite::fromJSON(raw_body, simplifyVector = TRUE), error=function(e) NULL)
    if (is.null(cont)) {
      cli::cli_alert_warning("OpenAI image returned non-JSON (parse fail) for {basename(pth)}")
      return(tibble(source_image = src, event_date = as.Date(character()), band_name = character(), event_time = character()))
    }

    raw <- tryCatch(cont$choices[[1]]$message$content, error=function(e) "")
    raw <- .strip_code_fences(raw)
    parsed <- tryCatch(jsonlite::fromJSON(raw), error=function(e) NULL)
    if (is.null(parsed) || is.null(parsed$items)) {
      cli::cli_alert_warning("OpenAI image returned non-JSON or empty items for {basename(pth)}")
      if (!is.null(.debug_dir()))
        writeLines(raw, file.path(.debug_dir(), paste0("openai_image_content_", basename(pth), ".txt")))
      return(tibble(source_image = src, event_date = as.Date(character()), band_name = character(), event_time = character()))
    }

    items <- as_tibble(parsed$items)
    if (!nrow(items)) {
      return(tibble(source_image = src, event_date = as.Date(character()), band_name = character(), event_time = character()))
    }

    out <- items %>%
      transmute(
        source_image = src,
        event_date   = suppressWarnings(lubridate::as_date(.data$date)),
        band_name    = .data$band %||% "",
        event_time   = .data$time %||% ""
      ) %>%
      filter(!is.na(event_date) & nzchar(band_name))

    # Strict post-filter by filename month/year
    if (!is.na(mo_num)) out <- out %>% filter(lubridate::month(event_date) == mo_num)
    if (!is.na(yr))     out <- out %>% filter(lubridate::year(event_date)  == yr)

    out
  })

  out_rows
}
