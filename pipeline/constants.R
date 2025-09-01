# -------------------------------------------------------------------
# FILE: pipeline/constants.R
# WHAT: Shared helpers so naming stays consistent across scripts.
# -------------------------------------------------------------------

# We accept any of these headers in venues.csv and normalize to `venue`
ACCEPTED_VENUE_COLS <- c("venue", "venue_name", "name")

# Rename whichever of the accepted columns exists → `venue`
canonicalize_venues <- function(df) {
  col <- intersect(ACCEPTED_VENUE_COLS, names(df))[1]
  if (length(col) == 0)
    stop("Venues CSV must include one of: ", paste(ACCEPTED_VENUE_COLS, collapse=", "))
  dplyr::rename(df, venue = dplyr::all_of(col))
}

# Normalize text for matching (case, underscores, accents)
norm_venue_key <- function(x) {
  x |>
    tolower() |>
    gsub("_", " ", x = _) |>
    trimws() |>
    iconv(from = "", to = "ASCII//TRANSLIT")  # remove accents (á -> a, etc.)
}
