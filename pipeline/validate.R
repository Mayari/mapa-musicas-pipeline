suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
})

source("pipeline/constants.R")

# Keep the most recent copy of each (venue, date, band) row.
validate_events <- function(df){
  if (!nrow(df)) return(df)

  clean <- df %>%
    mutate(
      venue_key = norm_venue_key(venue),
      band_key  = tolower(band_name) %>% iconv(from = "", to = "ASCII//TRANSLIT")
    ) %>%
    arrange(source) %>%               # stable but arbitrary; tweak if you add confidences
    distinct(venue_key, event_date, band_key, .keep_all = TRUE) %>%
    select(-venue_key, -band_key)

  clean
}
