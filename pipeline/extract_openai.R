# pipeline/extract_openai.R
# Minimal direct image extraction using OpenAI (gpt-4o by default).
# Returns tibble: source_image, event_date (Date), band_name (chr), event_time (chr).
# Behavior:
#  - Uses month/year hints from filename (month_es, year) to avoid wrong months/years.
#  - STRICT JSON-only response; omit rows if unsure.

suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(base64enc)
  library(cli)
  library(tibble)
  library(purrr)
  library(dplyr)
  library(lubridate)
})

`%||%` <- function(a, b) if (!is.null(a)) a else b

# Map Spanish month to number
.month_es_to_num <- function(m) {
  m <- tolower(trimws(m))
  dict <- c("enero"=1,"febrero"=2,"marzo"=3,"abril"=4,"mayo"=5,"junio"=6,
            "julio"=7,"agosto"=8,"septiembre"=9,"setiembre"=9,"octubre"=10,"noviembre"=11,"diciembre"=12)
  dict[[m]] %||% NA_integer_
}

.strip_code_fences <- function(x) {
  x <- gsub("^\\s*```(json)?\\s*", "", x)
  x <- gsub("\\s*```\\s*$", "", x)
  x
}

# image_paths: local PNG/JPGs; month_es/year are vectors (same length) of filename hints.
# source_image_ids: optional vector of original source paths (if PDFs were converted first)
extract_openai <- function(image_paths, month_es, year, source_image_ids = NULL,
                           model = Sys.getenv("OPENAI_IMAGE_MODEL", unset = "gpt-4o")) {
  if (length(image_paths) == 0) {
    return(tibble(source_image = character(),
                  event_date = as.Date(character()),
                  band_name = character(),
                  event_time = character()))
  }

  key <- Sys.getenv("OPENAI_API_KEY", "")
  if (!nzchar(key)) cli::cli_abort("OPENAI_API_KEY not set.")

  if (is.null(source_image_ids)) source_image_ids <- image_paths

  out_rows <- purrr::map2_dfr(seq_along(image_paths), image_paths, function(i, pth) {
    mo <- month_es[i]; yr <- suppressWarnings(as.integer(year[i])); src <- source_image_ids[i]

    # Build data URL
    ext <- tolower(tools::file_ext(pth))
    mime <- if (ext %in% c("jpg","jpeg")) "image/jpeg" else if (ext == "png") "image/png" else "image/*"
    bytes <- readBin(pth, what = "raw", n = file.info(pth)$size)
    b64   <- base64enc::base64encode(bytes)
    data_url <- paste0("data:", mime, ";base64,", b64)

    # Prompt with strict schema + assumptions
    assume_line <- if (!is.na(yr) && !is.na(.month_es_to_num(mo))) {
      sprintf("Assume month is '%s' (Spanish) and year is %d unless the poster explicitly shows a different month/year.", mo, yr)
    } else {
      "Use the month/year visible on the poster; if uncertain, omit the row."
    }

    system_msg <- "You extract structured gig listings from venue posters. Be careful and conservative."
    user_text <- paste(
      "Extract only events that you are confident about.",
      assume_line,
      "Return STRICT JSON, NO commentary.",
      "Schema:",
      "{ \"items\": [ { \"date\": \"YYYY-MM-DD\", \"band\": \"...\", \"time\": \"HH:MM\" | \"\" } ] }",
      "Rules:",
      "- Use Spanish month/day names correctly.",
      "- If a date lacks month/year, use the assumed month/year above.",
      "- If you are not sure about a row, OMIT it.",
      "- Do not invent bands or times.",
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
          list(role = "user", content = list(
            list(type = "text", text = user_text),
            list(type = "image_url", image_url = list(url = data_url))
          ))
        )
      ), auto_unbox = TRUE)

    resp <- tryCatch(req_perform(req), error = function(e) e)
    if (inherits(resp, "error")) {
      cli::cli_alert_danger("OpenAI image request failed for {basename(pth)}: {resp$message}")
      return(tibble(source_image = src, event_date = as.Date(character()), band_name = character(), event_time = character()))
    }
    cont <- resp_body_json(resp, simplifyVector = TRUE)
    raw  <- tryCatch(cont$choices[[1]]$message$content, error = function(e) "")
    raw  <- .strip_code_fences(raw)

    parsed <- tryCatch(jsonlite::fromJSON(raw), error = function(e) NULL)
    if (is.null(parsed) || is.null(parsed$items)) {
      cli::cli_alert_warning("OpenAI image returned non-JSON or empty items for {basename(pth)}")
      return(tibble(source_image = src, event_date = as.Date(character()), band_name = character(), event_time = character()))
    }

    items <- parsed$items
    if (length(items) == 0) {
      return(tibble(source_image = src, event_date = as.Date(character()), band_name = character(), event_time = character()))
    }

    tibble(
      source_image = src,
      event_date   = suppressWarnings(lubridate::as_date(items$date)),
      band_name    = items$band %||% "",
      event_time   = items$time %||% ""
    ) %>%
      filter(!is.na(event_date) & nzchar(band_name))
  })

  out_rows
}
