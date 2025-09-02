suppressPackageStartupMessages({
  library(tesseract)
  library(magick)
  library(tidyverse)
})

message("[extract_tesseract.R] v2025-09-02 robust (spa+eng, preprocess, multi-psm)")

OCR_LANGS     <- Sys.getenv("OCR_LANGS");     if (!nzchar(OCR_LANGS))     OCR_LANGS <- "spa+eng"
OCR_DPI       <- suppressWarnings(as.integer(Sys.getenv("OCR_DPI")));       if (is.na(OCR_DPI))       OCR_DPI <- 350
OCR_MIN_WIDTH <- suppressWarnings(as.integer(Sys.getenv("OCR_MIN_WIDTH"))); if (is.na(OCR_MIN_WIDTH)) OCR_MIN_WIDTH <- 1400
OCR_PSMS      <- Sys.getenv("OCR_PSMS");      if (!nzchar(OCR_PSMS))      OCR_PSMS <- "6,4,3"
OCR_PSMS      <- as.integer(strsplit(OCR_PSMS, ",")[[1]])

# ---- Preprocess image for better OCR -----------------------------------------
.prep_image <- function(path) {
  # Read with density for PDFs and raster; auto-orient
  img <- tryCatch(image_read(path, density = OCR_DPI), error = function(e) image_read(path))
  img <- image_auto_orient(img)

  # If multi-page (PDF), keep all frames
  info <- image_info(img)

  # Convert to grayscale, upscale if narrow, clean up
  upscale_one <- function(frame) {
    fr_info <- image_info(frame)
    if (fr_info$width[1] < OCR_MIN_WIDTH) {
      frame <- image_resize(frame, paste0(OCR_MIN_WIDTH, "x"))
    }
    frame <- image_convert(frame, colorspace = "gray")
    frame <- image_enhance(frame)
    frame <- image_normalize(frame)
    # light deskew; ignore errors if not supported in build
    frame <- tryCatch(image_deskew(frame, threshold = 40), error = function(e) frame)
    frame
  }

  if (length(img) > 1) {
    image_join(lapply(seq_along(img), function(i) upscale_one(img[i])) )
  } else {
    upscale_one(img)
  }
}

# ---- OCR with multiple PSM attempts; bail once we have enough text -----------
.tess_once <- function(image, psm) {
  eng <- try(tesseract(language = OCR_LANGS,
                       options  = list(tessedit_pageseg_mode = as.integer(psm))),
             silent = TRUE)
  if (inherits(eng, "try-error")) return("")

  if (length(image) > 1) {
    paste(vapply(seq_len(length(image)),
                 function(i) ocr(image[i], engine = eng),
                 FUN.VALUE = character(1L)), collapse = "\n\n")
  } else {
    ocr(image, engine = eng)
  }
}

# ---- Public API ---------------------------------------------------------------
tesseract_to_text <- function(path) {
  img <- tryCatch(.prep_image(path), error = function(e) NULL)
  if (is.null(img)) return("")

  for (psm in OCR_PSMS) {
    txt <- tryCatch(.tess_once(img, psm), error = function(e) "")
    # Return as soon as we have a reasonably non-empty result
    if (nchar(gsub("\\s+", "", txt)) > 60) return(txt)
  }
  # Fallback: whatever we got last
  txt %||% ""
}

# Backward-compat aliases the pipeline probes for
tesseract_ocr_text <- tesseract_to_text
ocr_to_text        <- tesseract_to_text
ocr_image_text     <- tesseract_to_text

`%||%` <- function(a, b) if (!is.null(a) && !is.na(a) && nzchar(a)) a else b
