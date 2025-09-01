suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(stringr)
})

message("[parse_posters.R] v2025-09-01 per-venue rules + fallbacks")

# ---- load rules once
if (exists("load_venue_rules")) {
  VENUE_RULES <- tryCatch(load_venue_rules("data/venue_rules"), error=function(e) list())
} else {
  VENUE_RULES <- list()
}

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

apply_corrections <- function(x, corr){
  if (is.null(corr) || !length(corr)) return(x)
  for (i in seq_len(nrow(corr))){
    frm <- corr$from[i]; to <- corr$to[i]
    x[tolower(x) == tolower(frm)] <- to
  }
  x
}

# ---------- venue-aware engine ----------
parse_with_rules <- function(row, rules, raw_text){
  if (is.null(rules) || is.null(rules$residencies)) return(tibble())
  out <- list()

  raw_lines <- unlist(strsplit(raw_text, "\\r?\\n"))
  lines <- stringr::str_squish(raw_lines)
  lines_lc <- tolower(lines)
  low <- tolower(raw_text)

  for (k in seq_along(rules$residencies)){
    r <- rules$residencies[[k]]
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
    corrections  <- NULL
    if (!is.null(r$corrections)) {
      corrections <- tibble(from = as.character(r$corrections$from), to = as.character(r$corrections$to))
    }

    # Find the heading in lines; if not found but we have default_band and its regex matches in full text, allow it
    idx <- which(grepl(head_rx, lines_lc))
    trigger <- length(idx) > 0
    if (!trigger && !is.null(default_band) && nzchar(head_rx)) {
      trigger <- grepl(head_rx, low)
      if (trigger) {
        # Pick a reasonable starting line (first line matching heading’s first word)
        first_word <- strsplit(head_rx, "\\W")[[1]][1]
        if (!is.na(first_word) && nzchar(first_word)) {
          idx <- which(grepl(first_word, lines_lc))[1]
          if (is.na(idx)) idx <- 1L
        } else idx <- 1L
      }
    }
    if (!trigger) next

    rng <- block_after_heading(lines_lc, idx[1], max_lines = win_lines, stop_pat = stop_pat)
    block <- paste(lines[rng[1]:rng[2]], collapse = "\n")

    # If band list format is present, expand dates for each band
    mm <- stringr::str_match_all(tolower(block), band_rx)
    if (length(mm) && length(mm[[1]]) > 0){
      M <- mm[[1]]
      bands <- stringr::str_squish(M[,2])
      dayss <- M[,3]
      for (i in seq_along(bands)){
        dn0 <- parse_day_list(dayss[i])
        dn <- dn0
        if (snap_days && !is.na(wd_idx)) {
          dn <- unique(na.omit(sapply(dn0, snap_day_to_weekday, year=row$year, month=row$month, wd_idx=wd_idx)))
        }
        if (!length(dn)) next

        b_clean <- stringr::str_squish(stringr::str_to_title(bands[i]))
        if (!is.null(drop_band_rx) && grepl(drop_band_rx, tolower(b_clean))) next
        if (!is.null(corrections)) b_clean <- apply_corrections(b_clean, corrections)

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

    # Otherwise, if there is a default band and a target weekday, generate weekly rows
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

# --------- generic fallback (kept minimal)
generic_parse <- function(row){
  txt <- row$ocr_text
  if (is.na(txt) || !nzchar(txt)) return(tibble())
  low <- tolower(txt)
  out <- list()

  # Generic Jam (fallback)
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
