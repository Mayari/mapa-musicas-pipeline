suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(stringr)
})

# Headings we don't want in band_name
GENERIC_HEADING_PAT <- "(residenc|mi[eé]rcoles|miercoles|martes|jueves|viernes|s[áa]bado|domingo|noche|jam|open mic|valentine|festival|fiesta|d[íi]a|domingo familiar|salsa|jazz day|karaoke)"

# --- Vectorized time normalizer: accepts a character vector and returns HH:MM or NA for each element
normalize_time_vec <- function(tt) {
  if (is.null(tt)) return(character(0))
  tt <- as.character(tt)
  tt <- tolower(tt)

  # Match: 8, 8:30, 8 pm, 20h, 20:30h, 20 hrs, etc.
  m <- stringr::str_match(tt, "(\\d{1,2})(?::(\\d{2}))?\\s*(am|pm|h|hrs?)?")
  # m has columns: full, hour, minute, suffix
  hh <- suppressWarnings(as.integer(m[, 2]))
  mm <- suppressWarnings(as.integer(ifelse(is.na(m[, 3]), 0L, m[, 3])))
  suf <- m[, 4]

  # Convert AM/PM to 24h
  pm <- !is.na(suf) & suf == "pm" & !is.na(hh)
  am <- !is.na(suf) & suf == "am" & !is.na(hh)
  hh[pm & hh < 12] <- hh[pm & hh < 12] + 12
  hh[am & hh == 12] <- 0

  ok <- !is.na(hh)
  out <- rep(NA_character_, length(tt))
  out[ok] <- sprintf("%02d:%02d", hh[ok] %% 24, mm[ok] %% 60)
  out
}

validate_events <- function(df){
  if (!nrow(df)) return(df)

  df %>%
    mutate(
      # move generic headings to event_title if band looks generic
      is_generic_band = str_detect(tolower(band_name), GENERIC_HEADING_PAT),
      event_title = coalesce(event_title, ifelse(is_generic_band, band_name, NA_character_)),
      band_name   = ifelse(is_generic_band, NA_character_, band_name),
      # vectorized normalization (no ifelse on scalars)
      event_time  = normalize_time_vec(event_time)
    ) %>%
    filter(!is.na(band_name)) %>%
    arrange(source) %>%
    distinct(venue, event_date, band_name, event_time, .keep_all = TRUE)
}
