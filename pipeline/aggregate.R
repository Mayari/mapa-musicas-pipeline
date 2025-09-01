suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
})

canon_venues <- function(df){
  if (!nrow(df)) return(tibble(venue_key=character(), municipality=character(), state=character()))
  nm <- names(df)
  # normalize name column to 'venue'
  if (!"venue" %in% nm) {
    alt <- intersect(c("venue_name","name"), nm)
    if (length(alt)) df <- dplyr::rename(df, venue = dplyr::all_of(alt[1])) else df$venue <- NA_character_
  }
  if (!"municipality" %in% names(df)) df$municipality <- NA_character_
  if (!"state" %in% names(df)) df$state <- NA_character_

  df %>%
    mutate(venue_key = tolower(venue) |> gsub("_"," ", x = _) |> trimws()) %>%
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

  # Ensure required columns exist
  if (!"event_date" %in% names(ev)) ev$event_date <- as.Date(ev$event_date)
  ev <- ev %>%
    mutate(
      year  = if ("year"  %in% names(ev)) year  else lubridate::year(event_date),
      month = if ("month" %in% names(ev)) month else lubridate::month(event_date)
    )

  if (!"venue" %in% names(ev)) {
    if ("venue_name" %in% names(ev)) ev <- ev %>% mutate(venue = .data$venue_name) else ev$venue <- NA_character_
  }
  if (!"state" %in% names(ev)) ev$state <- NA_character_
  if (!"municipality" %in% names(ev)) ev$municipality <- NA_character_

  # Read explicit venues.csv and optional auto-fill table
  vdf   <- tryCatch(readr::read_csv(venues_path, show_col_types = FALSE), error = function(e) tibble())
  vauto <- tryCatch(readr::read_csv(file.path(dirname(venues_path), "venues_autofill.csv"), show_col_types = FALSE),
                    error = function(e) tibble())

  vcanon_explicit <- canon_venues(vdf)
  vcanon_auto     <- canon_venues(vauto)  # may only have state

  # Merge: prefer explicit values; use auto as fallback or to add missing venues
  if (nrow(vcanon_auto)) {
    vcanon <- full_join(vcanon_explicit, vcanon_auto, by = "venue_key", suffix = c("", "_auto")) %>%
      mutate(
        municipality = dplyr::coalesce(municipality, municipality_auto),
        state        = dplyr::coalesce(state, state_auto)
      ) %>%
      select(venue_key, municipality, state) %>%
      distinct()
  } else {
    vcanon <- vcanon_explicit
  }

  # Enrich events with municipality/state from merged canon (without overwriting known values)
  ev <- ev %>%
    mutate(venue_key = tolower(venue) %>% gsub("_"," ",.) %>% trimws()) %>%
    left_join(vcanon, by = "venue_key", suffix = c("", ".v")) %>%
    mutate(
      municipality = dplyr::coalesce(municipality, .data$municipality.v),
      state        = dplyr::coalesce(state,        .data$state.v)
    ) %>%
    select(-venue_key, -ends_with(".v"))

  # Aggregations
  states_month <- ev %>%
    count(state, year, month, name = "events") %>%
    arrange(state, year, month)

  municipalities_month <- ev %>%
    count(state, municipality, year, month, name = "events") %>%
    arrange(state, municipality, year, month)

  venues_month <- ev %>%
    count(state, municipality, venue, year, month, name = "events") %>%
    arrange(state, municipality, venue, year, month)

  readr::write_csv(states_month,         file.path(out_dir, "states_month.csv"))
  readr::write_csv(municipalities_month, file.path(out_dir, "municipalities_month.csv"))
  readr::write_csv(venues_month,         file.path(out_dir, "venues_month.csv"))
  message("[aggregate] Wrote aggregates: states_month.csv, municipalities_month.csv, venues_month.csv")
}
