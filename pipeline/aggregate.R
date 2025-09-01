suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
})

# Normalize a venues-like table to {venue_key, municipality, state}
canon_venues <- function(df){
  # Handle NULL or truly empty inputs
  if (is.null(df)) df <- tibble()
  # If there are zero rows but known columns, nrow(df)==0 still holds
  if (nrow(df) == 0 && !length(names(df))) {
    return(tibble(venue_key=character(), municipality=character(), state=character()))
  }

  nm <- names(df)
  # normalize name column to 'venue'
  if (!"venue" %in% nm) {
    alt <- intersect(c("venue_name","name"), nm)
    if (length(alt)) df <- dplyr::rename(df, venue = dplyr::all_of(alt[1])) else df$venue <- NA_character_
  }
  if (!"municipality" %in% names(df)) df$municipality <- NA_character_
  if (!"state" %in% names(df)) df$state <- NA_character_

  df %>%
    mutate(
      venue_key = tolower(venue) %>% gsub("_"," ",.) %>% trimws()
    ) %>%
    select(venue_key, municipality, state) %>%
    distinct()
}

aggregate_all <- function(events_path, venues_path, out_dir){
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

  ev <- tryCatch(readr::read_csv(events_path, show_col_types = FALSE), error = function(e) tibble())
  if (!nrow(ev)) {
    readr::write_csv(tibble(state=character(), year=integer(), month=integer(), events=integer()),
                     file.path(out_dir, "states_month.csv"))
    readr::write_csv(tibble(state=character(), municipality=character(), year=integer(), month=integer(), events=integer()),
                     file.path(out_dir, "municipalities_month.csv"))
    readr::write_csv(tibble(state=character(), municipality=character(), venue=character(), year=integer(), month=integer(), events=integer()),
                     file.path(out_dir, "venues_month.csv"))
    message("[aggregate] No events; writing empty aggregation files.")
    return(invisible(NULL))
  }

  # Ensure event_date and year/month
  if (!"event_date" %in% names(ev)) ev$event_date <- as.Date(ev$event_date)
  ev <- ev %>%
    mutate(
      year  = if ("year"  %in% names(ev)) year  else lubridate::year(event_date),
      month = if ("month" %in% names(ev)) month else lubridate::month(event_date)
    )

  # Ensure venue/state/municipality cols exist
  if (!"venue" %in% names(ev)) {
    if ("venue_name" %in% names(ev)) ev <- ev %>% mutate(venue = .data$venue_name) else ev$venue <- NA_character_
  }
  if (!"state" %in% names(ev)) ev$state <- NA_character_
  if (!"municipality" %in% names(ev)) ev$municipality <- NA_character_

  # Read explicit venues.csv and auto-fill table (from run_monthly)
  vdf   <- tryCatch(readr::read_csv(venues_path, show_col_types = FALSE), error = function(e) tibble())
  vauto <- tryCatch(readr::read_csv(file.path(dirname(venues_path), "venues_autofill.csv"), show_col_types = FALSE),
                    error = function(e) tibble())

  vcanon_explicit <- canon_venues(vdf)
  vcanon_auto     <- canon_venues(vauto)

  # Build unified canon with priority: explicit (1) overrides auto (2)
  vcanon <- bind_rows(
      vcanon_auto     %>% mutate(.priority = 2L),
      vcanon_explicit %>% mutate(.priority = 1L)
    ) %>%
    group_by(venue_key) %>%
    arrange(.priority, .by_group = TRUE) %>%
    summarise(
      municipality = dplyr::coalesce(first(na.omit(municipality)), NA_character_),
      state        = dplyr::coalesce(first(na.omit(state)),        NA_character_),
      .groups = "drop"
    )

  # Enrich events with municipality/state (do not overwrite non-missing values in ev)
  ev <- ev %>%
    mutate(venue_key = tolower(venue) %>% gsub("_"," ",.) %>% trimws()) %>%
    left_join(vcanon, by = "venue_key", suffix = c("", ".canon")) %>%
    mutate(
      municipality = dplyr::coalesce(municipality, .data$municipality.canon),
      state        = dplyr::coalesce(state,        .data$state.canon)
    ) %>%
    select(-venue_key, -ends_with(".canon"))

  # Aggregations (NA groups allowed; fill later as you enrich venues.csv)
  states_month <- ev %>% count(state, year, month, name = "events") %>% arrange(state, year, month)
  municipalities_month <- ev %>% count(state, municipality, year, month, name = "events") %>% arrange(state, municipality, year, month)
  venues_month <- ev %>% count(state, municipality, venue, year, month, name = "events") %>% arrange(state, municipality, venue, year, month)

  readr::write_csv(states_month,         file.path(out_dir, "states_month.csv"))
  readr::write_csv(municipalities_month, file.path(out_dir, "municipalities_month.csv"))
  readr::write_csv(venues_month,         file.path(out_dir, "venues_month.csv"))
  message("[aggregate] Wrote aggregates: states_month.csv, municipalities_month.csv, venues_month.csv")
}
