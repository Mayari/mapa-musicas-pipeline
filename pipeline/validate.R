suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
})

# generic headings we don't want as band names
GENERIC_HEADING_PAT <- "(residenc|mi[eé]rcoles|miercoles|martes|jueves|viernes|s[áa]bado|domingo|noche|jam|open mic|valentine|festival|fiesta|d[íi]a|domingo familiar|salsa|jazz day|karaoke)"

normalize_time <- function(t){
  if (is.null(t) || !length(t)) return(NA_character_)
  t <- tolower(t)
  m <- stringr::str_match(t, "(\\d{1,2})(?::(\\d{2}))?\\s*(am|pm|h|hrs?)?")
  if (all(is.na(m))) return(NA_character_)
  hh <- as.integer(m[,2]); mm <- ifelse(is.na(m[,3]), 0L, as.integer(m[,3])); suf <- m[,4]
  if (!is.na(suf) && suf %in% c("pm") && hh < 12) hh <- hh + 12
  if (!is.na(suf) && suf %in% c("am") && hh == 12) hh <- 0
  sprintf("%02d:%02d", hh %% 24, mm %% 60)
}

validate_events <- function(df){
  if (!nrow(df)) return(df)

  out <- df %>%
    mutate(
      # move generic headings to event_title if band looks generic
      is_generic_band = stringr::str_detect(tolower(band_name), GENERIC_HEADING_PAT),
      event_title = dplyr::coalesce(event_title, ifelse(is_generic_band, band_name, NA_character_)),
      band_name   = ifelse(is_generic_band, NA_character_, band_name),
      event_time  = ifelse(!is.na(event_time), normalize_time(event_time), event_time)
    ) %>%
    filter(!is.na(band_name)) %>%                              # drop rows where we couldn’t fix a heading
    arrange(source) %>%
    distinct(venue, event_date, band_name, event_time, .keep_all = TRUE)

  out
}
