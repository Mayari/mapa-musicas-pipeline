suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
})

# Normalize a venues table to {venue_key, municipality, state}
canon_venues <- function(df){
  if (!nrow(df)) return(tibble(venue_key=character(), municipality=character(), state=character()))
  # choose a name column
  nm <- names(df)
  if (!"venue" %in% nm) {
    alt <- intersect(c("venue_name","name"), nm)
    if (length(alt)) df <- dplyr::rename(df, venue = dplyr::all_of(alt[1])) else df$venue <- NA_character_
  }
  if (!"municipality" %in% names(df)) df$municipality <- NA_character_
  if (!"state" %in% names(df)) df$state <- NA_character_

  df %>%
    mutate(
      venue_key = tolower(venue) |> gsub("_", " ", x = _) |> trimws()
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

  # Enrich from venues.csv if present
  vdf <- tryCatch(readr::read_csv(venues_path, show_col_types = FALSE), error = function(e) tibble())
  vcanon <- canon_venues(vdf)

  if (nrow(vcanon)){
    ev <- ev %>%
      mutate(venue_key = tolower(venue) %>% gsub("_"," ",.) %>% trimws()) %>%
      left_join(vcanon, by = "venue_key", suffix = c("", ".v")) %>%
      mutate(
        municipality = dplyr::coalesce(municipality, .data$municipality.v),
        state        = dplyr::coalesce(state, .data$state.v)
      ) %>%
      select(-venue_key, -ends_with(".v"))
  }

  # Aggregations (will include NA groups if some fields missing)
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
