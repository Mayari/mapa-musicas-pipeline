suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
})

# Aggregate monthly counts at venue / municipality / state
aggregate_all <- function(events_path, venues_path, out_dir){
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

  ev <- suppressWarnings(readr::read_csv(events_path, show_col_types = FALSE))
  if (!nrow(ev)) return(invisible())

  venues <- suppressWarnings(readr::read_csv(venues_path, show_col_types = FALSE))

  # Join on human-readable venue name (as written by the extractors)
  ev <- ev |>
    left_join(venues, by = "venue") |>
    mutate(year = lubridate::year(event_date),
           month = lubridate::month(event_date))

  agg_venue <- ev |> count(venue, year, month, name = "events")
  agg_muni  <- ev |> count(municipality, year, month, name = "events")
  agg_state <- ev |> count(state, year, month, name = "events")

  readr::write_csv(agg_venue, file.path(out_dir, "agg_venue_month.csv"))
  readr::write_csv(agg_muni,  file.path(out_dir, "agg_municipality_month.csv"))
  readr::write_csv(agg_state, file.path(out_dir, "agg_state_month.csv"))
}
