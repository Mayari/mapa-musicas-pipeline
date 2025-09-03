# pipeline/extract_openai_text.R
# v2.1.0 Minimal text-only extractor (strict month/year)
# Input: tibble with columns: source_image, ocr_text, month_name_es (opt), year (opt)
# Output: tibble: source_image, event_date (Date), band_name, event_time

suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(cli)
  library(tibble)
  library(purrr)
  library(dplyr)
  library(lubridate)
  library(stringr)
})

`%||%` <- function(a, b) if (!is.null(a)) a else b

.month_es_to_num <- function(m) {
  if (is.null(m) || is.na(m)) return(NA_integer_)
  m <- tolower(str_trim(m))
  dict <- c("enero"=1,"febrero"=2,"marzo"=3,"abril"=4,"mayo"=5,"junio"=6,
            "julio"=7,"agosto"=8,"septiembre"=9,"setiembre"=9,"octubre"=10,"noviembre"=11,"diciembre"=12)
  dict[[m]] %||% NA_integer_
}

.strip_code_fences <- function(x) {
  x <- gsub("^\\s*```(json)?\\s*", "", x)
  x <- gsub("\\s*```\\s*$", "", x)
  x
}

extract_openai_text <- function(df,
                                model = Sys.getenv("OPENAI_TEXT_MODEL", unset = "gpt-4.1")) {
  if (nrow(df) == 0) {
    return(tibble(source_image = character(),
                  event_date = as.Date(character()),
                  band_name = character(),
                  event_time = character()))
  }

  key <- Sys.getenv("OPENAI_API_KEY", "")
  if (!nzchar(key)) cli::cli_abort("OPENAI_API_KEY not set.")

  # Process row-by-row (keeps prompts small; avoids cross-poster leakage)
  rows <- split(df, seq_len(nrow(df)))

  out <- purrr::map_dfr(rows, function(row) {
    src <- row$source_image[[1]]
    txt <- row$ocr_text[[1]] %||% ""
    mo  <- row$month_name_es[[1]] %||% NA_character_
    yr  <- suppressWarnings(as.integer(row$year[[1]]))

    mo_num <- .month_es_to_num(mo)

    assume_line <- if (!is.na(yr) && !is.na(mo_num)) {
      sprintf("Assume the month is '%s' (Spanish) and the year is %d. Only include dates within that month and year. If any date appears to be outside that month, OMIT it.", mo, yr)
    } else if (!is.na(mo_num)) {
      sprintf("Assume the month is '%s' (Spanish). Only include dates within that month. If outside, OMIT.", mo)
    } else {
      "If the month/year is not clearly visible, include only rows you are confident about."
    }

    system_msg <- "You extract structured gig listings from plain text transcribed from posters. Be conservative: omit rows if unsure."
    user_text <- paste(
      "Extract only confident events as JSON. Return STRICT JSON and nothing else.",
      assume_line,
      "Schema:",
      "{ \"items\": [ { \"date\": \"YYYY-MM-DD\", \"band\": \"...\", \"time\": \"HH:MM\" | \"\" } ] }",
      "Rules:",
      "- If a time is not present, set it to \"\" (empty).",
      "- Do not invent bands or times.",
      "- If you cannot parse a row confidently, omit it.",
      sep = "\n"
    )

    req <- request("https://api.openai.com/v1/chat/completions") |>
      req_headers(
        Authorization = paste("Bearer", key),
        `Content-Type` = "application/json"
      ) |>
      req_body_json(list(
        model = model,
        temperature = 0,
        messages = list(
          list(role = "system", content = system_msg),
          list(role = "user", content = paste(user_text, "\n\nTEXT:\n", txt))
        )
      ), auto_unbox = TRUE)

    resp <- tryCatch(req_perform(req), error = function(e) e)
    if (inherits(resp, "error")) {
      cli::cli_alert_danger("OpenAI text request failed for {basename(src)}: {resp$message}")
      return(tibble(source_image = src, event_date = as.Date(character()),
                    band_name = character(), event_time = character()))
    }
    cont <- resp_body_json(resp, simplifyVector = TRUE)
    raw  <- tryCatch(cont$choices[[1]]$message$content, error = function(e) "")
    raw  <- .strip_code_fences(raw)

    parsed <- tryCatch(jsonlite::fromJSON(raw), error = function(e) NULL)
    if (is.null(parsed) || is.null(parsed$items)) {
      cli::cli_alert_warning("OpenAI text returned non-JSON or empty items for {basename(src)}")
      return(tibble(source_image = src, event_date = as.Date(character()),
                    band_name = character(), event_time = character()))
    }

    items <- as_tibble(parsed$items) %>% mutate(source_image = src, .before = 1)
    if (!nrow(items)) return(tibble(source_image = src, event_date = as.Date(character()),
                                    band_name = character(), event_time = character()))

    # Coerce & post-filter strictly by month/year if provided
    items <- items %>%
      transmute(
        source_image = source_image,
        event_date = suppressWarnings(lubridate::as_date(.data$date)),
        band_name  = .data$band %||% "",
        event_time = .data$time %||% ""
      ) %>%
      filter(!is.na(event_date) & nzchar(band_name))

    if (!is.na(mo_num)) {
      items <- items %>% filter(lubridate::month(event_date) == mo_num)
    }
    if (!is.na(yr)) {
      items <- items %>% filter(lubridate::year(event_date) == yr)
    }

    items
  })

  out
}
