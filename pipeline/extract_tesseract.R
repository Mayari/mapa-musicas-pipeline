# pipeline/extract_tesseract.R
# v2025-09-02a robust (spa+eng, preprocess, multi-psm) + defaults + debug dumps

suppressPackageStartupMessages({
  library(magick)
  library(tesseract)
  library(stringr)
  library(cli)
  library(glue)
  library(purrr)
  library(tibble)
  library(dplyr)
})

# ---- Read OCR settings from env with SAFE DEFAULTS ----
langs_env     <- Sys.getenv("OCR_LANGS",     unset = "spa+eng")
dpi_env       <- Sys.getenv("OCR_DPI",       unset = "350")
min_width_env <- Sys.getenv("OCR_MIN_WIDTH", unset = "1400")
psms_env      <- Sys.getenv("OCR_PSMS",      unset = "6,4,3")

langs     <- if (nzchar(langs_env)) langs_env else "spa+eng"
dpi       <- suppressWarnings(as.integer(if (nzchar(dpi_env)) dpi_env else "350"))
min_width <- suppressWarnings(as.integer(if (nzchar(min_width_env)) min_width_env else "1400"))
psms      <- suppressWarnings(as.integer(strsplit(if (nzchar(psms_env)) psms_env else "6,4,3", ",")[[1]]))

cli::cli_h1("[extract_tesseract.R] v2025-09-02a robust (spa+eng, preprocess, multi-psm)")
cli::cli_alert_info("Effective OCR settings: langs={langs} dpi={dpi} min_width={min_width} psms={paste(psms, collapse=',')}")

if (!nzchar(Sys.getenv("OCR_LANGS")))     cli::cli_alert_warning("OCR_LANGS empty; defaulting to {langs}")
if (!nzchar(Sys.getenv("OCR_DPI")))       cli::cli_alert_warning("OCR_DPI empty; defaulting to {dpi}")
if (!nzchar(Sys.getenv("OCR_MIN_WIDTH"))) cli::cli_alert_warning("OCR_MIN_WIDTH empty; defaulting to {min_width}")
if (!nzchar(Sys.getenv("OCR_PSMS")))      cli::cli_alert_warning("OCR_PSMS empty; defaulting to {paste(psms, collapse=',')}")

# Optional debug directory is passed via option set in run_monthly.R
.debug_dir <- getOption("mapa.debug_dir", default = NULL)

# ---- Helpers ----

# Preprocess image for better OCR
.preprocess_image <- function(path, min_width, dpi) {
  # If density helps with vector/PDF or small rasters, pass here; otherwise image_read works fine.
  img <- tryCatch(
    image_read(path, density = paste0(dpi, "x", dpi)),
    error = function(e) image_read(path)
  )

  # Convert to grayscale, orient based on EXIF, slight contrast, deskew
  img <- img |>
    image_quantize(colorspace = "gray") |>
    image_orient() |>                 # <-- correct magick function
    image_contrast(sharpen = 1L) |>
    image_deskew(threshold = "40%")

  # Ensure minimum width (scale up if needed)
  info <- image_info(img)
  if (info$width < min_width) {
    scale_percent <- ceiling((min_width / info$width) * 100)
    img <- image_resize(img, geometry = glue::glue("{scale_percent}%"))
  }

  # Light unsharp mask (helps small fonts)
  img <- image_unsharp_mask(img, radius = 1, sigma = 0.5, amount = 0.8, threshold = 0.02)

  img
}

# Run Tesseract with multiple PSMs, pick the best by text length
.ocr_multi_psm <- function(img, langs, psms) {
  engine <- tesseract(language = langs)
  best <- list(text = "", psm = NA_integer_)
  for (p in psms) {
    txt <- tryCatch({
      ocr(image = img, engine = engine, options = list(psm = as.integer(p)))
    }, error = function(e) "")
    if (nzchar(txt) && nchar(txt) > nchar(best$text)) {
      best$text <- txt
      best$psm  <- p
    }
  }
  best
}

# ---- Public function ----
extract_tesseract <- function(image_paths) {
  if (length(image_paths) == 0) {
    cli::cli_alert_warning("extract_tesseract: No image paths given.")
    return(tibble(source_image = character(), ocr_text = character(), psm_used = integer()))
  }

  results <- map_df(image_paths, function(pth) {
    cli::cli_alert("OCR: {basename(pth)}")
    img <- tryCatch(.preprocess_image(pth, min_width = min_width, dpi = dpi),
                    error = function(e) { cli::cli_alert_danger("Preprocess failed: {e$message}"); return(NULL) })
    if (is.null(img)) {
      return(tibble(source_image = pth, ocr_text = "", psm_used = NA_integer_))
    }

    best <- .ocr_multi_psm(img, langs = langs, psms = psms)
    txt  <- if (!is.null(best$text)) best$text else ""

    # Debug dump
    if (!is.null(.debug_dir)) {
      safe_name <- gsub("[^A-Za-z0-9._-]", "_", basename(pth))
      out_txt   <- file.path(.debug_dir, paste0(safe_name, ".txt"))
      cat(txt, file = out_txt, sep = "")
    }

    # Warn on very short OCR (likely failure)
    if (nchar(txt) < 80) {
      cli::cli_alert_warning("Very short OCR text (<80 chars) for {basename(pth)}. Consider raising OCR_DPI/OCR_MIN_WIDTH or checking language packs.")
    }

    tibble(source_image = pth, ocr_text = txt, psm_used = as.integer(best$psm))
  })

  results
}
