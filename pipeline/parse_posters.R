suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(stringr)
})

message("[parse_posters.R] v2025-09-01 per-venue rules + allowed + regex-fixes")

# ---- rules cache (populated by venue_rules.R if sourced) ----
if (!exists("VENUE_RULES")) VENUE_RULES <- list()

weekday_index <- c(
  "lunes"=1, "martes"=2, "miércoles"=3, "miercoles"=3,
  "jueves"=4, "viernes"=5, "sábado"=6, "sabado"=6, "domingo"=7
)

norm_venue_key <- function(x){
  x <- tolower(as.character(x))
  x <- gsub("_", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

dates_for <- function(year, month, wd){
  first <- as.Date(sprintf("%04d-%02d-01", year, month))
  last  <- first + months(1) - days(1)
  all   <- seq(first, last, by = "1 day")
  all[lubridate::wday(all, week_start = 1) == wd]
}

snap_day_to_weekday <- function(day, year, month, wd_idx){
  cand <- day + c(0, -1, +1, -2, +2)
  cand <- cand[cand >= 1 & cand <= 31]
  for (d in cand){
    dt <- as.Date(sprintf("%04d-%02d-%02d", year, month, d))
    if (lubridate::wday(dt, week_start = 1) == wd_idx) return(d)
  }
  NA_integer_
}

parse_day_list <- function(s){
  s <- tolower(s)
  s <- gsub("[^0-9y, ]", "", s)
  s <- gsub("\\s*y\\s*", ",", s)
  toks <- unlist(strsplit(s, "[, ]+"))
  as.integer(unique(toks[nchar(toks) > 0]))
}

title_case_es <- function(s){
  if (is.na(s) || !nzchar(s)) return(NA_character_)
  s <- tolower(s)
  s <- gsub("\\bmiercoles\\b", "miércoles", s)
  s <- gsub("\\bsabado\\b", "sábado", s)
  tools::toTitleCase(s)
}

block_after_heading <- function(lines_lc, start_idx, max_lines = 14, stop_pat = NULL){
  if (is.null(stop_pat)) {
    stop_pat <- "(^\\s*(lunes|martes|mi[eé]rcoles|miercoles|jueves|viernes|s[áa]bado|sabado|domingo)\\b)|residen|big\\s*band|programaci[oó]n|cartelera|covers?|cover|entrada|reserv"
  }
  end_idx <- length(lines_lc)
  nxt <- which(grepl(stop_pat, lines_lc))
  nxt <- nxt[nxt > start_idx]
  if (length(nxt)) end_idx <- min(end_idx, min(nxt) - 1L)
  end_idx <- min(end_idx, start_idx + max_lines)
  c(start_idx, end_idx)
}

# ---------- venue-aware engine ----------
parse_with_rules <- function(row, rules, raw_text){
  if (is.null(rules) || is.null(rules$residencies)) return(tibble())
  out <- list()

  raw_lines <- unlist(strsplit(raw_text, "\\r?\\n"))
  lines <- stringr::str_squish(raw_lines)
  lines_lc <- tolower(lines)
  low <- tolower(raw_text)

  for (r in rules$residencies){
    head_rx   <- r$heading_regex %||% ""
    ev_title  <- r$event_title %||% ""
    wd_name   <- r$weekday %||% NA
    wd_idx    <- if (!is.na(wd_name)) weekday_index[tolower(wd_name)] else NA
    win_lines <- r$line_window %||% 16
    stop_pat  <- r$stop_pattern %||% NULL
    band_rx   <- r$band_line_regex %||% "([\\p{L}\\d &'\\.\\-]+?)\\s*[:\\-]\\s*([0-9]{1,2}(?:\\s*(?:,|y)\\s*[0-9]{1,2})+)"
    default_band <- r$default_band %||% NULL
    drop_band_rx <- r$drop_band_regex %||% NULL
    snap_days    <- isTRUE(r$snap_days)
    allowed_bands <- r$allowed_bands %||% character(0)
    allowed_norm  <- tolower(trimws(allowed_bands))

    # corrections: exact or regex
    corr_exact <- tibble(from = character(), to = character())
    corr_rx    <- tibble(from_rx = character(), to = character())
    if (!is.null(r$corrections)) {
      if (!is.null(r$corrections$from))   corr_exact <- tibble(from = as.character(r$corrections$from),   to = as.character(r$corrections$to))
      if (!is.null(r$corrections$from_rx)) corr_rx   <- tibble(from_rx = as.character(r$corrections$from_rx), to = as.character(r$corrections$to))
    }

    # find the heading area
    idx <- which(grepl(head_rx, lines_lc))
    trigger <- length(idx) > 0
    if (!trigger && !is.null(default_band) && nzchar(head_rx)) {
      trigger <- grepl(head_rx, low)
      if (trigger) {
        first_word <- strsplit(head_rx, "\\W")[[1]][1]
        idx <- which(grepl(first_word, lines_lc))[1]; if (is.na(idx)) idx <- 1L
      }
    }
    if (!trigger) next

    rng <- block_after_heading(lines_lc, idx[1], max_lines = win_lines, stop_pat = stop_pat)
    block <- paste(lines[rng[1]:rng[2]], collapse = "\n")

    # pattern "Band: 10 y 24"
    mm <- stringr::str_match_all(tolower(block), band_rx)
    if (length(mm) && length(mm[[1]]) > 0){
      M <- mm[[1]]
      bands <- stringr::str_squish(M[,2])
      dayss <- M[,3]
      for (i in seq_along(bands)){
        dn0 <- parse_day_list(dayss[i])
        dn  <- dn0
        if (snap_days && !is.na(wd_idx)) {
          dn <- unique(na.omit(sapply(dn0, snap_day_to_weekday, year=row$year, month=row$month, wd_idx=wd_idx)))
        }
        if (!length(dn)) next

        # clean/correct band
        b_clean <- stringr::str_squish(stringr::str_to_title(bands[i]))
        # regex corrections
        if (nrow(corr_rx)){
          for (j in seq_len(nrow(corr_rx))){
            if (grepl(corr_rx$from_rx[j], b_clean, perl = TRUE)) b_clean <- corr_rx$to[j]
          }
        }
        # exact corrections
        if (nrow(corr_exact)){
          hit <- tolower(b_clean) == tolower(corr_exact$from)
          if (any(hit)) b_clean <- corr_exact$to[which(hit)[1]]
        }
        # drop unwanted bands
        if (!is.null(drop_band_rx) && grepl(drop_band_rx, tolower(b_clean))) next
        # whitelist if provided
        if (length(allowed_norm) && !(tolower(b_clean) %in% allowed_norm)) next

        dates <- as.Date(sprintf("%04d-%02d-%02d", row$year, row$month, dn))
        out[[length(out)+1]] <- tibble(
          venue        = row$venue,
          venue_id     = NA_character_,
          event_date   = dates,
          band_name    = b_clean,
          event_title  = if (nzchar(ev_title)) title_case_es(ev_title) else NA_character_,
          event_time   = NA_character_,
          source_image = row$image_path
        )
      }
      next
    }

    # default weekly rows (e.g., Jam)
    if (!is.null(default_band) && !is.na(wd_idx)) {
      dts <- dates_for(row$year, row$month, wd_idx)
      if (length(dts)){
        out[[length(out)+1]] <- tibble(
          venue        = row$venue,
          venue_id     = NA_character_,
          event_date   = dts,
          band_name    = default_band,
          event_title  = if (nzchar(ev_title)) title_case_es(ev_title) else NA_character_,
          event_time   = NA_character_,
          source_image = row$image_path
        )
      }
    }
  }

  if (!length(out)) return(tibble())
  dplyr::bind_rows(out)
}

# Generic fallback (kept minimal: Jam)
generic_parse <- function(row){
  txt <- row$ocr_text
  if (is.na(txt) || !nzchar(txt)) return(tibble())
  low <- tolower(txt)
  out <- list()

  jam_present <- grepl("\\bjam\\b", low) || grepl("jam\\s*session", low)
  jam_martes  <- grepl("(cada|todos?\\s+los)\\s+martes", low) ||
                 grepl("martes\\s+de\\s+jam", low) ||
                 grepl("martes[^\\n]{0,20}jam", low) ||
                 grepl("jam[^\\n]{0,20}martes", low)
  if (jam_present && jam_martes) {
    tues <- dates_for(row$year, row$month, 2)
    if (length(tues)){
      out[[length(out)+1]] <- tibble(
        venue        = row$venue,
        venue_id     = NA_character_,
        event_date   = tues,
        band_name    = "Jam de la casa",
        event_title  = "Martes De Jam",
        event_time   = NA_character_,
        source_image = row$image_path
      )
    }
  }

  if (!length(out)) return(tibble())
  dplyr::bind_rows(out)
}

# ocr_raw must have: image_path, ocr_text, venue, year, month
parse_poster_events <- function(ocr_raw){
  purrr::map_dfr(seq_len(nrow(ocr_raw)), function(i){
    row <- ocr_raw[i,]
    rules <- VENUE_RULES[[ norm_venue_key(row$venue) ]]
    out <- parse_with_rules(row, rules, row$ocr_text)
    if (!nrow(out)) out <- generic_parse(row)
    out
  })
}
