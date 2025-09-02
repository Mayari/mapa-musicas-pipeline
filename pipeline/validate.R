suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(stringr)
  library(readr)
})

message("[validate.R] v2025-09-02 minimal + per-venue manual overrides")

# ---------- Helpers ------------------------------------------------------------

norm_venue_key <- function(x){
  x <- tolower(as.character(x))
  x <- gsub("_", " ", x)
  x <- gsub("-", " ", x)
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

weekday_es <- function(d){
  dn <- wday(d, week_start = 1)
  c("lunes","martes","miércoles","jueves","viernes","sábado","domingo")[dn]
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

# ---------- Per-venue manual CSV reader ---------------------------------------

# Read ALL manual CSVs:
# - Global fallback: data/manual_events.csv (optional)
# - Tree: data/manual_events/<STATE>/<VENUE>.csv (recommended)
# CSV columns accepted:
#   event_date OR date OR (year,month,day)
#   band_name OR band
#   event_time OR time OR hora
#   venue (optional; inferred from file path if missing)
read_manual_events_all <- function(base_dir = "data/manual_events", venues_df = NULL){
  out <- list()

  # 0) optional global CSV for convenience
  glob <- file.path("data","manual_events.csv")
  if (file.exists(glob)) {
    df <- suppressWarnings(readr::read_csv(glob, show_col_types = FALSE))
    if (nrow(df)) out <- append(out, list(list(df = df, venue_from = NA_character_)))
  }

  # 1) Per-venue files
  if (dir.exists(base_dir)) {
    files <- list.files(base_dir, pattern = "\\.csv$", recursive = TRUE, full.names = TRUE)
    for (fp in files){
      # venue guess from file name (last path segment, without extension)
      vn <- tools::file_path_sans_ext(basename(fp))
      vn <- gsub("[-_]+", " ", vn)
      vn <- trimws(vn)
      df <- suppressWarnings(readr::read_csv(fp, show_col_types = FALSE))
      if (!is.null(df) && nrow(df)){
        out <- append(out, list(list(df = df, venue_from = vn)))
      }
    }
  }

  if (!length(out)) return(tibble())

  # Canonical venue names (if venues_df provided)
  vmap <- NULL
  if (!is.null(venues_df) && nrow(venues_df)){
    vmap <- venues_df %>% mutate(vkey = norm_venue_key(venue)) %>% select(vkey, venue)
  }

  # Parse & normalize each small table
  parsed <- lapply(out, function(x){
    df <- x$df
    venue_from_path <- x$venue_from

    # unify column names to lowercase
    names(df) <- tolower(names(df))

    # event_date from event_date | date | y/m/d
    if (!("event_date" %in% names(df))) {
      if ("date" %in% names(df)) {
        df$event_date <- suppressWarnings(as.Date(df$date))
      } else if (all(c("year","month","day") %in% names(df))) {
        df$event_date <- as.Date(sprintf("%04d-%02d-%02d",
                                         as.integer(df$year),
                                         as.integer(df$month),
                                         as.integer(df$day)))
      }
    }
    # band_name from band_name | band
    if (!("band_name" %in% names(df)) && "band" %in% names(df)) {
      df$band_name <- df$band
    }
    # event_time from event_time | time | hora
    if (!("event_time" %in% names(df))) {
      if ("time" %in% names(df)) df$event_time <- df$time
      else if ("hora" %in% names(df)) df$event_time <- df$hora
      else df$event_time <- NA_character_
    }

    # venue
    if (!("venue" %in% names(df)) || all(is.na(df$venue)) || !any(nzchar(df$venue))) {
      df$venue <- venue_from_path %||% ""
    }

    df <- df %>%
      transmute(
        venue        = as.character(venue),
        venue_id     = NA_character_,
        event_date   = suppressWarnings(as.Date(event_date)),
        band_name    = stringr::str_squish(as.character(band_name)),
        event_time   = as.character(event_time)
      ) %>%
      mutate(event_time = normalize_time_vec(event_time)) %>%
      filter(!is.na(event_date), nzchar(venue), nzchar(band_name))

    # canonicalize venue name via venues.csv if possible
    if (!is.null(vmap)) {
      df <- df %>%
        mutate(vkey = norm_venue_key(venue)) %>%
        left_join(vmap, by = "vkey") %>%
        mutate(venue = coalesce(venue.y, venue.x)) %>%
        select(-vkey, -venue.x, -venue.y)
    }

    df
  })

  bind_rows(parsed)
}

# ---------- Main entry ---------------------------------------------------------

# df: auto-extracted rows (from OCR+LLM), columns: venue, event_date, band_name, event_time, ...
# venues_df: data/venues.csv (optional) for muni/state/coords
validate_events <- function(df, venues_df = NULL){
  # Read per-venue manual overrides first
  manual <- read_manual_events_all("data/manual_events", venues_df = venues_df)

  # Start from auto-extracted (could be empty)
  if (!nrow(df)) {
    base <- tibble(venue = character(), event_date = as.Date(character()),
                   band_name = character(), event_time = character(),
                   venue_id = character())
  } else {
    base <- df %>%
      mutate(
        band_name  = stringr::str_squish(as.character(band_name)),
        event_time = normalize_time_vec(event_time)
      ) %>%
      select(venue, event_date, band_name, event_time, venue_id)
  }

  # Manual rows override any auto rows for same venue+date
  all_rows <- base %>%
    anti_join(manual %>% select(venue, event_date), by = c("venue","event_date")) %>%
    bind_rows(manual)

  if (!nrow(all_rows)) return(all_rows)

  # Join venue metadata if provided
  if (!is.null(venues_df) && nrow(venues_df)) {
    vsel <- venues_df %>%
      transmute(
        venue,
        municipality = coalesce(municipality, NA_character_),
        state        = coalesce(state, NA_character_),
        latitude     = suppressWarnings(as.numeric(coalesce(latitude, lat, NA))),
        longitude    = suppressWarnings(as.numeric(coalesce(longitude, lon, NA)))
      )
    all_rows <- all_rows %>% left_join(vsel, by = "venue")
  }

  all_rows %>%
    mutate(
      year    = as.integer(lubridate::year(event_date)),
      month   = as.integer(lubridate::month(event_date)),
      weekday = {
        dn <- wday(event_date, week_start = 1)
        c("lunes","martes","miércoles","jueves","viernes","sábado","domingo")[dn]
      }
    ) %>%
    distinct(venue, event_date, band_name, event_time, .keep_all = TRUE) %>%
    select(venue, event_date, weekday, band_name, event_time,
           municipality, state, latitude, longitude, venue_id) %>%
    arrange(venue, event_date, band_name)
}
