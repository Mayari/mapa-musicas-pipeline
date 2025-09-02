suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(stringr)
})

message("[validate.R] v2025-09-02 minimal validator (no events/cycles)")

norm_venue_key <- function(x){
  x <- tolower(as.character(x))
  x <- gsub("_", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

normalize_time_vec <- function(tt) {
  if (is.null(tt)) return(character(0))
  tt <- as.character(tt); if (!length(tt)) return(tt)
  tt <- tolower(tt)
  m <- stringr::str_match(tt, "(\\d{1,2})(?::(\\d{2}))?\\s*(am|pm|h|hrs?)?")
  hh <- suppressWarnings(as.integer(m[, 2]))
  mm <- suppressWarnings(as.integer(ifelse(is.na(m[, 3]), 0L, m[, 3])))
  suf <- m[, 4]
  pm <- !is.na(suf) & suf == "pm" & !is.na(hh)
  am <- !is.na(suf) & suf == "am" & !is.na(hh)
  hh[pm & hh < 12] <- hh[pm & hh < 12] + 12
  hh[am & hh == 12] <- 0
  ok <- !is.na(hh)
  out <- rep(NA_character_, length(tt))
  out[ok] <- sprintf("%02d:%02d", hh[ok] %% 24, mm[ok] %% 60)
  out
}

apply_name_patches <- function(df){
  pth <- file.path("data","name_patches.csv")
  if (!file.exists(pth)) return(df)
  patches <- tryCatch(readr::read_csv(pth, show_col_types = FALSE), error=function(e) tibble())
  if (!nrow(patches)) return(df)

  norm <- function(s) tolower(gsub("\\s+"," ", trimws(as.character(s))))
  df$band_name <- as.character(df$band_name)
  for (i in seq_len(nrow(patches))){
    to <- as.character(patches$to[i])
    venue <- if ("venue" %in% names(patches)) as.character(patches$venue[i]) else NA_character_
    year  <- if ("year"  %in% names(patches)) suppressWarnings(as.integer(patches$year[i]))  else NA_integer_
    month <- if ("month" %in% names(patches)) suppressWarnings(as.integer(patches$month[i])) else NA_integer_

    cond <- rep(TRUE, nrow(df))
    if (!is.na(venue)) cond <- cond & (tolower(df$venue) == tolower(venue))
    if (!is.na(year))  cond <- cond & (df$year  == year)
    if (!is.na(month)) cond <- cond & (df$month == month)

    if ("from_rx" %in% names(patches) && is.character(patches$from_rx[i]) && nzchar(patches$from_rx[i])) {
      cond <- cond & grepl(patches$from_rx[i], df$band_name, perl = TRUE)
    } else if ("from" %in% names(patches)) {
      cond <- cond & (norm(df$band_name) == norm(patches$from[i]))
    } else next

    df$band_name[cond] <- to
  }
  df
}

merge_manual_events <- function(df){
  pth <- file.path("data","manual_events.csv")
  if (!file.exists(pth)) return(df)
  man <- tryCatch(readr::read_csv(pth, show_col_types = FALSE), error=function(e) tibble())
  if (!nrow(man)) return(df)

  if (!("event_date" %in% names(man))) {
    if ("date" %in% names(man)) {
      man$event_date <- as.Date(man$date)
    } else if (all(c("year","month","day") %in% names(man))) {
      man$event_date <- as.Date(sprintf("%04d-%02d-%02d",
                                        as.integer(man$year),
                                        as.integer(man$month),
                                        as.integer(man$day)))
    }
  }

  man <- man %>%
    transmute(
      venue        = as.character(venue),
      venue_id     = NA_character_,
      event_date   = as.Date(event_date),
      band_name    = stringr::str_squish(as.character(band_name)),
      event_time   = if ("event_time" %in% names(.)) as.character(event_time) else NA_character_
    ) %>%
    mutate(event_time = normalize_time_vec(event_time)) %>%
    filter(!is.na(event_date), nzchar(venue), nzchar(band_name))

  if (!nrow(man)) return(df)

  df %>%
    anti_join(man %>% select(venue, event_date), by = c("venue","event_date")) %>%
    bind_rows(man)
}

weekday_es <- function(d){
  dn <- wday(d, week_start = 1)
  c("lunes","martes","miércoles","jueves","viernes","sábado","domingo")[dn]
}

validate_events <- function(df, venues_df = NULL){
  if (!nrow(df)) return(df)

  df <- df %>%
    mutate(
      band_name   = stringr::str_squish(as.character(band_name)),
      event_time  = normalize_time_vec(event_time),
      year        = as.integer(lubridate::year(event_date)),
      month       = as.integer(lubridate::month(event_date)),
      weekday     = weekday_es(event_date)
    ) %>%
    filter(!is.na(event_date), nzchar(band_name)) %>%
    distinct(venue, event_date, band_name, event_time, .keep_all = TRUE)

  # Optional join to venues.csv for muni/state/coords if provided
  if (!is.null(venues_df) && nrow(venues_df)) {
    vsel <- venues_df %>%
      transmute(
        venue,
        municipality = coalesce(municipality, NA_character_),
        state        = coalesce(state, NA_character_),
        latitude     = suppressWarnings(as.numeric(coalesce(latitude, lat, NA))),
        longitude    = suppressWarnings(as.numeric(coalesce(longitude, lon, NA)))
      )
    df <- df %>% left_join(vsel, by = "venue")
  }

  # Name cleanups via CSV patches
  df <- apply_name_patches(df)

  # Final order & columns (note: NO event_title)
  df %>%
    select(venue, event_date, weekday, band_name, event_time,
           municipality, state, latitude, longitude, venue_id) %>%
    arrange(venue, event_date, band_name)
}
