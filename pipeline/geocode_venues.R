suppressPackageStartupMessages({
  library(tidyverse)
  library(tidygeocoder)
})

# Geocode only rows missing latitude OR longitude.
# Uses OpenStreetMap (Nominatim). Free, polite, low volume.
geocode_missing_venues <- function(venues_path) {
  df <- tryCatch(readr::read_csv(venues_path, show_col_types = FALSE), error = function(e) tibble())
  if (!nrow(df)) { message("[geocode] venues.csv not found/empty"); return(invisible(FALSE)) }

  # Normalize column names we need
  name_col <- intersect(c("venue","venue_name","name"), names(df))
  if (length(name_col) && name_col[1] != "venue") df <- dplyr::rename(df, venue = dplyr::all_of(name_col[1]))
  if (!"municipality" %in% names(df)) df$municipality <- NA_character_
  if (!"state" %in% names(df))        df$state        <- NA_character_
  if (!"latitude" %in% names(df))     df$latitude     <- NA_real_
  if (!"longitude" %in% names(df))    df$longitude    <- NA_real_

  miss <- df %>%
    filter(is.na(latitude) | is.na(longitude)) %>%
    mutate(query = paste(venue, municipality, state, "México", sep = ", "))

  if (!nrow(miss)) { message("[geocode] nothing to geocode"); return(invisible(FALSE)) }

  # Be polite: cap per run (default 5). Override with secret/ENV GEOCODE_MAX if you want.
  N <- suppressWarnings(as.integer(Sys.getenv("GEOCODE_MAX", unset = "5"))); if (is.na(N) || N < 0) N <- 0L
  if (N == 0) { message("[geocode] skipped (GEOCODE_MAX=0)"); return(invisible(FALSE)) }

  todo <- miss %>% slice_head(n = N)

  message("[geocode] querying ", nrow(todo), " venue(s): ", paste(todo$venue, collapse = " | "))

  # Nominatim prefers a user agent; tidygeocoder sets one, we just stay gentle (limit=1)
  g <- tidygeocoder::geo(
    todo,
    address = query,
    method  = "osm",
    lat     = latitude,
    long    = longitude,
    limit   = 1,
    mode    = "single",
    custom_query = list(countrycodes = "mx", accept_language = "es")
  )

  # Merge results back, but only fill blanks
  out <- df %>%
    left_join(g %>% select(venue, latitude, longitude), by = "venue", suffix = c("", ".new")) %>%
    mutate(
      latitude  = dplyr::coalesce(latitude,  .data$latitude.new),
      longitude = dplyr::coalesce(longitude, .data$longitude.new)
    ) %>%
    select(-ends_with(".new"))

  readr::write_csv(out, venues_path)
  message("[geocode] wrote updated venues.csv; coords filled: ",
          sum(!is.na(out$latitude) & !is.na(out$longitude)), " / ", nrow(out))
  invisible(TRUE)
}
