suppressPackageStartupMessages({
  library(tesseract)   # R bindings for Tesseract OCR
  library(tidyverse)
})

message("[extract_tesseract.R] v2025-08-31")

# Use Spanish language data if available; fallback to 'eng'
.tess_lang <- Sys.getenv("TESSERACT_LANG", unset = "spa")

extract_tesseract_text <- function(image_path, venue_id, venue_name, year, month){
  if (!file.exists(image_path)) return(tibble())
  eng <- tryCatch(tesseract(language = .tess_lang), error=function(e) tesseract("eng"))
  txt <- tryCatch(ocr(image_path, engine = eng), error=function(e) "")
  tibble(
    ocr_text     = txt,
    venue_id     = venue_id,
    venue        = venue_name,
    year         = year,
    month        = month,
    source_image = image_path
  )
}
