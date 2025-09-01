# Canonical column policy
ACCEPTED_VENUE_COLS <- c("venue", "venue_name", "name")

# Normalize venue column name to `venue`
canonicalize_venues <- function(df) {
  col <- intersect(ACCEPTED_VENUE_COLS, names(df))[1]
  if (length(col) == 0) {
    stop("Venues CSV must include one of: ", paste(ACCEPTED_VENUE_COLS, collapse=", "))
  }
  dplyr::rename(df, venue = dplyr::all_of(col))
}

# Normalize text for matching (case/underscores/accents)
norm_venue_key <- function(x) {
  x |>
    tolower() |>
    gsub("_", " ", x = _) |>
    trimws() |>
    iconv(from = "", to = "ASCII//TRANSLIT")  # drop accents
}
