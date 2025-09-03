# pipeline/parse_text_rules.R
# Rule-based extraction from OCR text → {date, band, time}
suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(lubridate); library(tibble); library(purrr); library(readr)
})

`%||%` <- function(a,b) if (!length(a) || is.null(a)) b else a

.month_num <- function(mes) {
  mes <- tolower(trimws(mes))
  map <- c("enero"=1,"febrero"=2,"marzo"=3,"abril"=4,"mayo"=5,"junio"=6,
           "julio"=7,"agosto"=8,"septiembre"=9,"setiembre"=9,"octubre"=10,"noviembre"=11,"diciembre"=12)
  as.integer(map[[mes]] %||% NA)
}

.norm_time <- function(line) {
  s <- tolower(line)
  # HH:MM or HH.MM or HHhMM
  m <- str_match(s, "\\b(\\d{1,2})[:h\\.]?(\\d{2})\\b")
  if (!any(is.na(m))) {
    h <- as.integer(m[2]); mm <- as.integer(m[3]); if (h<=23 && mm<=59) return(sprintf("%02d:%02d", h, mm))
  }
  # 9 pm / 9pm / 21h
  m2 <- str_match(s, "\\b(\\d{1,2})\\s*(am|pm)\\b")
  if (!any(is.na(m2))) {
    h <- as.integer(m2[2]); ap <- m2[3]
    if (tolower(ap)=="pm" && h<12) h <- h+12
    if (tolower(ap)=="am" && h==12) h <- 0
    if (h<=23) return(sprintf("%02d:00", h))
  }
  m3 <- str_match(s, "\\b(\\d{1,2})h\\b")
  if (!any(is.na(m3))) {
    h <- as.integer(m3[2]); if (h<=23) return(sprintf("%02d:00", h))
  }
  ""
}

.is_weekday <- function(x) {
  grepl("^(lunes|martes|miercoles|miércoles|jueves|viernes|sabado|sábado|domingo)\\b", tolower(x))
}

.clean_band <- function(line) {
  s <- line
  s <- str_replace_all(s, "(?i)\\b(lunes|martes|miercoles|miércoles|jueves|viernes|sabado|sábado|domingo)\\b", "")
  s <- str_replace_all(s, "(?i)\\b(de|del|la|el|los|las|con)\\b", " ")
  s <- str_replace_all(s, "(?i)\\bcover\\b.*$", " ")
  s <- str_replace_all(s, "\\s+", " ")
  str_trim(s)
}

# Parse one poster text
parse_one_text <- function(ocr_text, year, month_es) {
  mo <- .month_num(month_es)
  if (is.na(year) || is.na(mo) || !nzchar(ocr_text)) {
    return(tibble(event_date=as.Date(character()), band_name=character(), event_time=character()))
  }

  # split lines, keep order
  lines <- strsplit(ocr_text, "\\n")[[1]] %||% character(0)
  lines <- trimws(lines)
  lines <- lines[nzchar(lines)]

  # date regexes (focus on the "day" number; assume month/year from filename)
  # e.g., "Jue 11", "11 Enero", "11/01", "11", "Jueves 11 – Banda"
  re_day    <- "(?<!\\d)(\\d{1,2})(?!\\d)"                 # standalone 1..31
  re_ddmm   <- "\\b(\\d{1,2})[\\-/\\.](\\d{1,2})\\b"       # 11/01
  re_month  <- "(?i)(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)"

  out <- list()

  for (i in seq_along(lines)) {
    ln <- lines[i]

    # try dd/mm hints → only take if mm == target month
    m_ddmm <- str_match_all(ln, re_ddmm)[[1]]
    day_candidates <- integer(0)
    if (nrow(m_ddmm)) {
      for (r in seq_len(nrow(m_ddmm))) {
        dd <- as.integer(m_ddmm[r,2]); mm <- as.integer(m_ddmm[r,3])
        if (!is.na(dd) && !is.na(mm) && mm == mo && dd >= 1 && dd <= 31) {
          day_candidates <- c(day_candidates, dd)
        }
      }
    }

    # generic day numbers (when a month word is present on the line OR we accept month-by-filename)
    has_month_word <- grepl(re_month, ln, perl=TRUE)
    if (length(day_candidates)==0) {
      m_day <- str_match_all(ln, re_day)[[1]]
      if (nrow(m_day)) {
        for (r in seq_len(nrow(m_day))) {
          dd <- as.integer(m_day[r,2])
          if (!is.na(dd) && dd>=1 && dd<=31) {
            # allow this day number; month fixed by filename
            day_candidates <- c(day_candidates, dd)
          }
        }
      }
    }

    day_candidates <- unique(day_candidates)
    if (!length(day_candidates)) next

    # derive time and band on the same line first
    time_guess <- .norm_time(ln)
    # remove date tokens, month tokens, weekdays, times from the line to get a band guess
    band_ln <- ln
    # strip dd/mm
    if (nrow(m_ddmm)) {
      for (r in seq_len(nrow(m_ddmm))) band_ln <- sub(m_ddmm[r,1], " ", band_ln, fixed=TRUE)
    }
    # strip standalone day numbers
    band_ln <- gsub(paste0("(?<!\\d)", paste(day_candidates, collapse="|"), "(?!\\d)"), " ", band_ln, perl=TRUE)
    # strip month words + weekdays + times
    band_ln <- str_replace_all(band_ln, re_month, " ")
    band_ln <- str_replace_all(band_ln, "(?i)\\b(lunes|martes|miercoles|miércoles|jueves|viernes|sabado|sábado|domingo)\\b", " ")
    band_ln <- str_replace_all(band_ln, "\\b\\d{1,2}[:h\\.]\\d{2}\\b", " ")
    band_ln <- str_replace_all(band_ln, "\\b\\d{1,2}\\s*(am|pm)\\b", " ")
    band_ln <- str_replace_all(band_ln, "\\s+[-–—]\\s+", " ")
    band_ln <- .clean_band(band_ln)

    # if band empty, peek next line as band
    if (!nzchar(band_ln) && i < length(lines)) {
      nxt <- lines[i+1]
      if (!grepl(re_ddmm, nxt) && !grepl(re_day, nxt) && !.is_weekday(nxt)) {
        band_ln <- .clean_band(nxt)
      }
    }

    if (!nzchar(band_ln)) next

    for (dd in day_candidates) {
      # construct date in targeted month/year
      dt <- suppressWarnings(as.Date(sprintf("%04d-%02d-%02d", as.integer(year), as.integer(mo), as.integer(dd))))
      if (!is.na(dt)) {
        out[[length(out)+1]] <- tibble(event_date=dt, band_name=band_ln, event_time=time_guess)
      }
    }
  }

  if (!length(out)) {
    return(tibble(event_date=as.Date(character()), band_name=character(), event_time=character()))
  }

  bind_rows(out) %>%
    filter(!is.na(event_date) & nzchar(band_name)) %>%
    distinct(event_date, band_name, event_time, .keep_all = TRUE)
}

# df columns expected: source_image, ocr_text, year, month_name_es
parse_ocr_df <- function(df) {
  if (!nrow(df)) return(tibble(source_image=character(), event_date=as.Date(character()), band_name=character(), event_time=character()))

  purrr::pmap_dfr(df, function(source_image, ocr_text, year, month_name_es) {
    res <- parse_one_text(ocr_text, year, month_name_es)
    if (!nrow(res)) {
      tibble(source_image=source_image, event_date=as.Date(character()), band_name=character(), event_time=character())
    } else {
      res %>% mutate(source_image = !!source_image) %>% select(source_image, event_date, band_name, event_time)
    }
  })
}
