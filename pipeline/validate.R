suppressPackageStartupMessages({ library(tidyverse) })

validate_events <- function(df){
  if (!nrow(df)) return(df)
  df |>
    mutate(
      band_name = stringr::str_squish(band_name),
      confidence = dplyr::case_when(
        is.na(event_date) ~ 0.2,
        nchar(band_name) < 3 ~ 0.3,
        TRUE ~ if_else(source == "openai", 0.9, 0.6)
      )
    ) |>
    distinct(venue, event_date, band_name, .keep_all = TRUE)
}
