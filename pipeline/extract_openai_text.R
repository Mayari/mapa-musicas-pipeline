# pipeline/extract_openai_text.R
# v3.0.0 Text-only extraction from OCR text
# - Default provider: OpenAI Responses API (JSON output) if OPENAI_API_KEY is set
# - If provider is "regex" or no key is set → regex fallback (no API cost)
# Inputs: df with columns: source_image, ocr_text, month_name_es, year
# Output: tibble(source_image, event_date, band_name, event_time)

suppressPackageStartupMessages({
  library(httr2); library(jsonlite); library(cli); library(dplyr); library(stringr)
  library(lubridate); library(tibble); library(purrr)
})

`%||%` <- function(a, b) if (!is.null(a)) a else b
.debug_dir <- function() getOption("mapa.debug_dir", NULL)

.month_es_to_num <- function(m) {
  m <- tolower(stringr::str_trim(m %||% ""))
  dict <- c("enero"=1,"febrero"=2,"marzo"=3,"abril"=4,"mayo"=5,"junio"=6,
            "julio"=7,"agosto"=8,"septiembre"=9,"setiembre"=9,"octubre"=10,"noviembre"=11,"diciembre"=12)
  dict[[m]] %||% NA_integer_
}

.strip_fences <- function(x) {
  x <- gsub("^\\s*```(json)?\\s*", "", x)
  gsub("\\s*```\\s*$", "", x)
}
.json_salvage <- function(txt) {
  if (!nzchar(txt)) return(NULL)
  x <- .strip_fences(txt)
  p <- tryCatch(jsonlite::fromJSON(x), error = function(e) NULL)
  if (!is.null(p)) return(p)
  # try largest {...} block
  opens  <- gregexpr("\\{", x, perl=TRUE)[[1]]
  closes <- gregexpr("\\}", x, perl=TRUE)[[1]]
  if (length(opens) == 0 || opens[1] == -1 || length(closes) == 0 || closes[1] == -1) return(NULL)
  y <- substr(x, opens[1], closes[length(closes)])
  tryCatch(jsonlite::fromJSON(y), error = function(e) NULL)
}

# ------------------ REGEX fallback ------------------
.regex_extract <- function(row) {
  txt <- row$ocr_text %||% ""
  yr  <- suppressWarnings(as.integer(row$year))
  mo  <- .month_es_to_num(row$month_name_es)

  if (!nzchar(txt) || is.na(yr) || is.na(mo)) return(tibble())
  lines <- unlist(strsplit(txt, "\n"))
  lines <- stringr::str_squish(lines)
  lines <- lines[nzchar(lines)]

  # Patterns
  month_pat <- "(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)"
  # 17 de enero / 17 enero / 17-01 / 17/1 etc.
  p_dayname <- "(lunes|martes|miércoles|miercoles|jueves|viernes|sábado|sabado|domingo)"
  pat1 <- sprintf("\\b(\\d{1,2})\\s*(de\\s+)?%s\\b", month_pat)
  pat2 <- "\\b(\\d{1,2})[./-](\\d{1,2})\\b"
  ptime <- "\\b([01]?\\d|2[0-3])[:.][0-5]\\d\\b"

  make_row <- function(d, band_line) {
    # strip time from band_line
    tm <- stringr::str_extract(band_line, ptime)
    band <- band_line
    band <- stringr::str_replace_all(band, ptime, "")
    band <- stringr::str_replace_all(band, paste0("\\b", p_dayname, "\\b"), "")
    band <- stringr::str_replace_all(band, "^\\W+|\\W+$", "")
    band <- stringr::str_squish(band)
    tibble(event_date = as.Date(d), band_name = band, event_time = tm %||% "")
  }

  rows <- list()

  for (ln in seq_along(lines)) {
    L <- lines[ln]

    # pattern 1: "17 de enero" or "17 enero"
    m1 <- stringr::str_match_all(string = tolower(L), pattern = pat1)[[1]]
    if (nrow(m1)) {
      for (k in seq_len(nrow(m1))) {
        day <- as.integer(m1[k, 2]); mo_line <- m1[k, 3] %||% m1[k, 4]
        mo_num <- .month_es_to_num(mo_line)
        if (is.na(day) || is.na(mo_num) || mo_num != mo) next
        d <- tryCatch(as.Date(sprintf("%04d-%02d-%02d", yr, mo, day)), error=function(e) NA)
        if (is.na(d)) next
        band_line <- stringr::str_replace(L, pat1, "")
        if (!nzchar(stringr::str_squish(band_line)) && ln < length(lines)) band_line <- lines[ln+1]
        rows <- append(rows, list(make_row(d, band_line)))
      }
      next
    }

    # pattern 2: "17/01" or "17-1"
    m2 <- stringr::str_match_all(string = L, pattern = pat2)[[1]]
    if (nrow(m2)) {
      for (k in seq_len(nrow(m2))) {
        day <- as.integer(m2[k, 2]); mo_num <- as.integer(m2[k, 3])
        if (is.na(day) || is.na(mo_num) || mo_num != mo) next
        d <- tryCatch(as.Date(sprintf("%04d-%02d-%02d", yr, mo, day)), error=function(e) NA)
        if (is.na(d)) next
        band_line <- stringr::str_replace(L, pat2, "")
        if (!nzchar(stringr::str_squish(band_line)) && ln < length(lines)) band_line <- lines[ln+1]
        rows <- append(rows, list(make_row(d, band_line)))
      }
    }
  }

  if (!length(rows)) return(tibble())
  bind_rows(rows) %>% filter(nzchar(band_name))
}

# ------------------ OpenAI text-only ------------------
.openai_text_extract <- function(df) {
  key   <- Sys.getenv("OPENAI_API_KEY", "")
  model <- Sys.getenv("OPENAI_TEXT_MODEL", "gpt-4.1")
  if (!nzchar(key)) return(tibble())  # no key → caller should fallback to regex

  throttle <- function() {
    sl <- suppressWarnings(as.numeric(Sys.getenv("OPENAI_THROTTLE_SEC", "0"))); if (!is.na(sl) && sl > 0) Sys.sleep(sl)
  }

  out <- purrr::map_dfr(seq_len(nrow(df)), function(i) {
    src <- df$source_image[i]
    yr  <- suppressWarnings(as.integer(df$year[i]))
    mo  <- .month_es_to_num(df$month_name_es[i])
    txt <- df$ocr_text[i] %||% ""
    if (!nzchar(txt)) return(tibble())

    sys <- "You convert noisy OCR text from music posters into a clean list of events. Be conservative and omit uncertain rows."
    usr <- paste(
      sprintf("Assume month=%s (Spanish) and year=%d. Only include dates within that month/year.", df$month_name_es[i], yr),
      "From the OCR text below, extract a JSON object with this schema:",
      '{ "items": [ { "date": "YYYY-MM-DD", "band": "string", "time": "HH:MM" } ] }',
      "- If time missing, use empty string.",
      "- Do not invent bands. If unsure about a row, omit it.",
      "",
      "OCR TEXT:",
      txt,
      sep = "\n"
    )

    req <- request("https://api.openai.com/v1/responses") |>
      req_headers(Authorization = paste("Bearer", key), `Content-Type` = "application/json") |>
      req_body_json(list(
        model = model, temperature = 0, max_output_tokens = 2000,
        response_format = list(type = "json_object"),
        input = list(
          list(role="system", content=list(list(type="input_text", text=sys))),
          list(role="user",   content=list(list(type="input_text",  text=usr)))
        )
      ), auto_unbox = TRUE)

    resp <- req_perform(req)
    body <- resp_body_string(resp)

    if (!is.null(.debug_dir())) writeLines(body, file.path(.debug_dir(), paste0("openai_text_", basename(src), ".json")))

    parsed <- tryCatch(jsonlite::fromJSON(body), error=function(e) NULL)
    raw <- ""
    if (!is.null(parsed)) {
      raw <- tryCatch(parsed$output_text, error=function(e) NULL)
      if (is.null(raw)) {
        raw <- tryCatch({
          parts <- parsed$output[[1]]$content
          idx <- which(vapply(parts, function(x) identical(x$type, "output_text"), logical(1)))
          if (length(idx) > 0) parts[[idx[1]]]$text else ""
        }, error=function(e) "")
      }
    }
    items <- NULL
    if (nzchar(raw)) {
      salv <- .json_salvage(raw)
      if (!is.null(salv)) items <- salv$items %||% salv
    }

    if (is.null(items) || length(items) == 0) {
      throttle(); return(tibble())
    }

    as_tibble(items) %>%
      transmute(
        source_image = src,
        event_date   = suppressWarnings(lubridate::as_date(.data$date)),
        band_name    = .data$band %||% "",
        event_time   = .data$time %||% ""
      ) %>%
      filter(!is.na(event_date) & nzchar(band_name)) %>%
      { if (!is.na(mo)) dplyr::filter(., lubridate::month(event_date)==mo) else . } %>%
      { if (!is.na(yr)) dplyr::filter(., lubridate::year(event_date)==yr) else . } %>%
      distinct()
  })

  out
}

# ------------------ public entry ------------------
extract_openai_text <- function(df) {
  if (!all(c("source_image","ocr_text","month_name_es","year") %in% names(df))) {
    cli::cli_abort("extract_openai_text: input must have columns source_image, ocr_text, month_name_es, year")
  }

  provider <- tolower(Sys.getenv("LLM_TEXT_PROVIDER", "openai"))
  key_set  <- nzchar(Sys.getenv("OPENAI_API_KEY", ""))

  if (provider == "regex" || !key_set) {
    cli::cli_alert_info("Text-only extractor: using REGEX mode")
    out <- purrr::map_dfr(seq_len(nrow(df)), ~ .regex_extract(df[.x, , drop=FALSE]))
    if (!nrow(out)) return(tibble(source_image=character(), event_date=as.Date(character()), band_name=character(), event_time=character()))
    return(out %>% mutate(source_image = df$source_image[1])[c("source_image","event_date","band_name","event_time")])
  }

  cli::cli_alert_info("Text-only extractor: using OpenAI Responses API (json)")
  out <- .openai_text_extract(df)
  if (!nrow(out)) {
    cli::cli_alert_warning("OpenAI text-only returned 0 rows; falling back to REGEX for this batch.")
    out <- purrr::map_dfr(seq_len(nrow(df)), ~ .regex_extract(df[.x, , drop=FALSE]))
    if (!nrow(out)) return(tibble(source_image=character(), event_date=as.Date(character()), band_name=character(), event_time=character()))
    out <- out %>% mutate(source_image = df$source_image[1])[c("source_image","event_date","band_name","event_time")]
  }
  out
}
