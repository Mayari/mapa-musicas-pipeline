# pipeline/run_monthly.R
# v2.2.0 Vision (with preprocess) + strict month/year + per-image fallback to OpenAI + manual overrides + debug

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(stringr)
  library(lubridate)
  library(tidyr)
  library(cli)
  library(glue)
  library(purrr)
  library(tibble)
})

cli::cli_h1(">> run_monthly v2.2.0 (Vision+preprocess + strict month/year + per-image OpenAI fallback)")

# ---- Args ----
args <- commandArgs(trailingOnly = TRUE)
get_arg <- function(flag, default = NULL) {
  i <- which(args == flag)
  if (length(i) == 0) return(default)
  if (i == length(args)) return(default)
  args[i + 1]
}

images_dir  <- get_arg("--images_dir", "../carteleras")
venues_path <- get_arg("--venues_path", "data/venues.csv")
out_dir     <- get_arg("--out_dir", "data")
agg_dir     <- get_arg("--agg_dir", "data/aggregations")
debug_dir   <- get_arg("--debug_dir", NULL)

dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(agg_dir, showWarnings = FALSE, recursive = TRUE)
if (!is.null(debug_dir)) dir.create(debug_dir, showWarnings = FALSE, recursive = TRUE)

# Debug snapshot
if (!is.null(debug_dir)) {
  options(mapa.debug_dir = debug_dir)
  envdump <- c(
    sprintf("OCR_PROVIDER=%s", Sys.getenv("OCR_PROVIDER")),
    sprintf("GCP_VISION_API_KEY=%s", ifelse(nzchar(Sys.getenv("GCP_VISION_API_KEY")),"set","")),
    sprintf("GOOGLE_APPLICATION_CREDENTIALS=%s", Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS")),
    sprintf("OPENAI_TEXT_MODEL=%s", Sys.getenv("OPENAI_TEXT_MODEL")),
    sprintf("OPENAI_IMAGE_MODEL=%s", Sys.getenv("OPENAI_IMAGE_MODEL"))
  )
  writeLines(envdump, file.path(debug_dir, "env.txt"))
}

# ---- Discover poster files ----
is_poster <- function(x) grepl("\\.(jpg|jpeg|png|pdf)$", x, ignore.case = TRUE)
poster_paths <- list.files(images_dir, pattern = NULL, recursive = TRUE, full.names = TRUE)
poster_paths <- poster_paths[is_poster(poster_paths)]

cli::cli_alert_info("Discovered {length(poster_paths)} poster file(s) under: {images_dir}")
if (length(poster_paths) > 0) cli::cli_alert_info("Example: {paste(head(poster_paths, 3), collapse=' | ')}")

# ---- Parse filename metadata (Venue_YYYYMesEspañol_n.ext) ----
parse_filename <- function(path) {
  b <- basename(path)
  m <- stringr::str_match(b, "^(.+?)_(\\d{4})([A-Za-zñÑ]+)_(\\d+)\\.(jpg|jpeg|png|pdf)$")
  if (is.na(m[1,1])) {
    return(tibble(source_image = path, venue_guess = NA_character_,
                  year = NA_integer_, month_name_es = NA_character_, file_num = NA_integer_))
  }
  tibble(
    source_image = path,
    venue_guess = m[1,2],
    year = as.integer(m[1,3]),
    month_name_es = m[1,4],
    file_num = as.integer(m[1,5])
  )
}
meta_df <- purrr::map_df(poster_paths, parse_filename)
usable_meta <- meta_df %>% filter(!is.na(venue_guess), !is.na(year))
cli::cli_alert_info("Parsed metadata rows (usable): {nrow(usable_meta)}")

# ---- PDF → PNG first page ----
pdf_to_png_once <- function(pdf_path, out_dir) {
  base <- tools::file_path_sans_ext(basename(pdf_path))
  out  <- file.path(out_dir, paste0(base, "_page1"))
  cmd  <- sprintf("pdftoppm -png -f 1 -l 1 -singlefile %s %s", shQuote(pdf_path), shQuote(out))
  system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE)
  png_path <- paste0(out, ".png")
  if (file.exists(png_path)) png_path else pdf_path
}
eff_dir <- if (is.null(debug_dir)) tempdir() else file.path(debug_dir, "pdf_firstpage")
dir.create(eff_dir, showWarnings = FALSE, recursive = TRUE)

eff_df <- usable_meta %>%
  mutate(
    ext = tolower(tools::file_ext(source_image)),
    effective_path = ifelse(ext == "pdf", pdf_to_png_once(source_image, eff_dir), source_image)
  ) %>%
  select(source_image, effective_path, year, month_name_es)

# ---- Light preprocessing (ImageMagick CLI) for better OCR ----
# auto-orient → grayscale → deskew 40% → resize width 1800px → unsharp → strip
preproc_image_cli <- function(in_path, out_dir, target_width = 1800L) {
  ext <- tolower(tools::file_ext(in_path))
  if (!ext %in% c("jpg","jpeg","png")) return(in_path)
  out <- file.path(out_dir,_
