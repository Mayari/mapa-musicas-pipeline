suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(stringr)
})

norm_name <- function(x){
  x %>%
    tolower() %>%
    str_replace_all("_", " ") %>%
    str_squish()
}

get_state_from_path <- function(p){
  parts <- strsplit(p, "/")[[1]]
  i <- which(parts == "carteleras")
  if (length(i) && length(parts) >= i + 1) return(parts[i + 1])
  NA_character_
}

safe_write_csv <- function(df, path){
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  readr::write_csv(df, path)
}

aggregate_all <- function(events_path, venues_path, out_dir){
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

  ev <- suppressWarnings(readr::read_csv(events_path, show_col_types = FALSE))

  # If no events yet, write empty aggregation files and exit
  if (!nrow(ev)) {
    message("[aggregate] No events; writing empty aggregation files.")
    safe_write_csv(tibble(venue = character(), year = integer(), month = integer(), events = integer()),
                   file.path(out_dir, "agg_venue_month.csv"))
    safe_write_csv(tibble(municipality = character(), year = integer(), month = integer(), events = integer()),
                   file.path(out_dir, "agg_municipality_month.csv"))
    safe_write_csv(tibble(state = character(), year = integer(), month = integer(), events = integer()),
                   file.path(out_dir, "agg_state_month.csv"))
    return(invisible())
  }

  # derive year/month
  ev <- ev %>% mutate(year = year(event_date), month = month(event_date))

  # ensure state exists (derive from image path if missing)
  if (!"state" %in% names(ev) || all(is.na(ev$state))) {
    ev <- ev %>% mutate(state = purrr::map_chr(source_image, get_state_from_path))
  }

  # load venues and make sure there's a 'venue' column
  venues <- suppressWarnings(readr::read_csv(venues_path, show_col_types = FALSE))
  if (!"venue" %in% names(venues)) {
    if ("venue_name" %in% names(venues)) venues <- venues %>% rename(venue = venue_name)
    else if ("name" %in% names(venues)) venues <- venues %>% rename(venue = name)
    else {
      message("[aggregate] Venues file has no 'venue'/'venue_name'/'name' column; proceeding without join.")
      venues <- tibble(venue = character(), municipality = character(), state = character(), lat = double(), lon = double())
    }
  }

  # normalized keys for robust joining
  ev <- ev %>% mutate(venue_key = norm_name(venue))
  venues <- venues %>% mutate(venue_key = norm_name(venue))

  # join municipality/state from venues (do not crash if missing)
  evj <- ev %>%
    left_join(venues %>% select(venue_key, municipality, state), by = "venue_key", keep = FALSE)

  # prefer event-derived state, fallback to venues state
  if ("state.x" %in% names(evj) && "state.y" %in% names(evj)) {
    evj <- evj %>% mutate(state = dplyr::coalesce(state.x, state.y)) %>% select(-state.x, -state.y)
  }

  # aggregates
  agg_venue <- ev %>% count(venue, year, month, name = "events")
  agg_muni  <- evj %>% count(municipality, year, month, name = "events")
  agg_state <- evj %>% count(state, year, month, name = "events")

  # write out
  safe_write_csv(agg_venue, file.path(out_dir, "agg_venue_month.csv"))
  safe_write_csv(agg_muni,  file.path(out_dir, "agg_municipality_month.csv"))
  safe_write_csv(agg_state, file.path(out_dir, "agg_state_month.csv"))
}
