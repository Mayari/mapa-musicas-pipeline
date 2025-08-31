suppressPackageStartupMessages({
  library(googleLanguageR)
  library(tidyverse)
})

extract_gvision_text <- function(image_path, venue_id, venue_name, year, month){
  if (!nzchar(Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS"))) return(tibble())
  gl_auth(Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
  res <- gl_vision(image_path, feature = "TEXT_DETECTION")
  text <- tryCatch(res$text$description[1], error = function(e) NA_character_)
  tibble(
    venue_id = venue_id,
    venue = venue_name,
    year = year,
    month = month,
    source_image = image_path,
    ocr_text = text
  )
}
