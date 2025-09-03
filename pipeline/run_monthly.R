# pipeline/run_monthly.R
# v2.4.0 Vision (with preprocess) + strict month/year + per-image OpenAI fallback + debug

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

`%||%` <- function(a, b) if (!is.null(a)) a else b

cli::cli_h1(">> run_monthly v2.4.0 (Vision+preprocess + strict month/year + per-image OpenAI fallback + debug)")

# ---- Args ----
args <- commandArgs(trailingOnly = TRUE)
get_arg <- function(flag, default = NULL) {
  i <- which(args == flag)
  if (length(i) == 0 || i == length(args)) return(default)
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

# ---- Debug snapshot ----
if (!is.null(debug_dir)) {
  options(mapa.debug_dir = debug_dir)
  envdump <- c(
    sprintf("OCR_PROVIDER=%s", Sys.getenv("OCR_PROVIDER")),
    sprintf("GCP_VISION_API_KEY=%s", ifelse(nzchar(Sys.getenv("GCP_VISION_API_KEY")), "set", "missing")),
    sprintf("GOOGLE_APPLICATION_CREDENTIALS=%s", ifelse(nzchar(Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS")), "set", "missing")),
    sprintf("OPENAI_TEXT_MODEL=%s", Sys.getenv("OPENAI_TEXT_MODEL")),
    sprintf("OPENAI_IMAGE_MODEL=%s", Sys.getenv("OPENAI_IMAGE_MODEL")),
    sprintf("OPENAI_API_KEY_SET=%s", ifelse(nzchar(Sys.getenv("OPENAI_API_KEY")), "yes", "no")),
    sprintf("OPENAI_THROTTLE_SEC=%s", Sys.getenv("OPENAI_THROTTLE_SEC", "0"))
  )
  writeLines(envdump, file.path(debug_dir, "env.txt"))
}

# ---- Discover poster files ----
is_poster <- function(x) grepl("\\.(jpg|jpeg|png|pdf)$", x, ignore.case = TRUE)
poster_paths <- list.files(images_dir, pattern = NULL, recursive = TRUE, full.names = TRUE)
poster_paths <- poster_paths[is_poster(poster_paths)]

cli::cli_alert_info(glue("Discovered {length(poster_paths)} poster file(s) under: {images_dir}"))
if (length(poster_paths) > 0) {
  cli::cli_alert_info(glue("Example: {paste(head(poster_paths, 3), collapse = ' | ')}"))
}

# ---- Parse filename metadata (Venue_YYYYMesEspañol_n.ext) ----
parse_filename <- function(path) {
  b <- basename(path)
  m <- stringr::str_match(b, "^(.+?)_(\\d{4})([A-Za-zñÑ]+)_(\\d+)\\.(jpg|jpeg|png|pdf)$")
  if (is.na(m[1, 1])) {
    return(tibble(
      source_image   = path,
      venue_guess    = NA_character_,
      year           = NA_integer_,
      month_name_es  = NA_character_,
      file_num       = NA_integer_
    ))
  }
  tibble(
    source_image   = path,
    venue_guess    = m[1, 2],
    year           = as.integer(m[1, 3]),
    month_name_es  = m[1, 4],
    file_num       = as.integer(m[1, 5])
  )
}

meta_df     <- purrr::map_df(poster_paths, parse_filename)
usable_meta <- meta_df %>% filter(!is.na(venue_guess), !is.na(year))
cli::cli_alert_info(glue("Parsed metadata rows (usable): {nrow(usable_meta)}"))

# ---- PDF → PNG first page ----
pdf_to_png_once <- function(pdf_path, out_dir_) {
  base <- tools::file_path_sans_ext(basename(pdf_path))
  out  <- file.path(out_dir_, paste0(base, "_page1"))
  cmd  <- sprintf("pdftoppm -png -f 1 -l 1 -singlefile %s %s", shQuote(pdf_path), shQuote(out))
  system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE)
  png_path <- paste0(out, ".png")
  if (file.exists(png_path)) png_path else pdf_path
}

eff_dir <- if (is.null(debug_dir)) tempdir() else file.path(debug_dir, "pdf_firstpage")
dir.create(eff_dir, showWarnings = FALSE, recursive = TRUE)

eff_df <- usable_meta %>%
  mutate(
    ext            = tolower(tools::file_ext(source_image)),
    effective_path = ifelse(ext == "pdf", pdf_to_png_once(source_image, eff_dir), source_image)
  ) %>%
  select(source_image, effective_path, year, month_name_es)

# ---- Preprocess (ImageMagick CLI) ----
# auto-orient → grayscale → deskew 40% → resize width 1600px → unsharp → strip  (slightly smaller for API payloads)
preproc_image_cli <- function(in_path, out_dir_, target_width = 1600L) {
  ext <- tolower(tools::file_ext(in_path))
  if (!ext %in% c("jpg", "jpeg", "png")) return(in_path)
  out <- file.path(out_dir_, basename(in_path))
  cmd <- sprintf(
    "convert %s -auto-orient -colorspace Gray -filter Lanczos -resize %dx -deskew 40%% -unsharp 0x1+0.75+0.02 -strip %s",
    shQuote(in_path), as.integer(target_width), shQuote(out)
  )
  system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE)
  if (file.exists(out)) out else in_path
}

pre_dir <- if (is.null(debug_dir)) tempdir() else file.path(debug_dir, "preproc")
dir.create(pre_dir, showWarnings = FALSE, recursive = TRUE)

eff_df <- eff_df %>%
  mutate(preproc_path = purrr::map_chr(effective_path, ~ preproc_image_cli(.x, pre_dir, target_width = 1600L)))

# For debug: record file sizes
if (!is.null(debug_dir)) {
  sizes <- eff_df %>%
    mutate(size_effective = file.info(effective_path)$size,
           size_preproc   = file.info(preproc_path)$size)
  readr::write_csv(sizes, file.path(debug_dir, "image_sizes.csv"))
}

# ---- Provider selection ----
provider_env <- tolower(Sys.getenv("OCR_PROVIDER", unset = "auto"))
has_vision   <- nzchar(Sys.getenv("GCP_VISION_API_KEY")) || nzchar(Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
provider <- dplyr::case_when(
  provider_env %in% c("vision", "openai", "auto") ~ provider_env,
  TRUE ~ "auto"
)
if (provider == "auto") provider <- if (has_vision) "vision" else "openai"
cli::cli_alert_info(glue("OCR provider selection: {provider} (vision_cred={has_vision})"))

# ---- Helper: OpenAI image path (now uses preprocessed images) ----
run_openai_image_pipeline <- function(df) {
  source("pipeline/extract_openai.R")
  cli::cli_alert_info("[extract_openai.R] strict-month image -> date / band / time (preprocessed)")
  extract_openai(
    image_paths      = df$preproc_path,   # <-- use preprocessed paths
    month_es         = df$month_name_es,
    year             = df$year,
    source_image_ids = df$source_image
  )
}

# ---- MAIN EXECUTION ----
min_chars <- 60L

if (nrow(eff_df) == 0) {

  all_extracted <- tibble(
    source_image = character(),
    event_date   = as.Date(character()),
    band_name    = character(),
    event_time   = character()
  )

} else if (provider == "openai") {

  all_extracted <- run_openai_image_pipeline(eff_df)

} else {

  # Vision-first with per-image fallback
  cand <- c("pipeline/extract_gvision.R", "pipeline/extract_google_vision.R")
  for (f in cand) if (file.exists(f)) try(source(f), silent = TRUE)
  if (!exists("extract_gvision") && !exists("extract_google_vision")) {
    stop("No Vision helper found (expected pipeline/extract_gvision.R or extract_google_vision.R).")
  }
  gv <- if (exists("extract_gvision")) extract_gvision else extract_google_vision

  # 1) Vision OCR on preprocessed images
  ocr_raw <- gv(eff_df$preproc_path, language_hints = c("es", "en"), feature = "DOCUMENT_TEXT_DETECTION") %>%
    rename(preproc_path = source_image)

  ocr_df <- eff_df %>%
    left_join(ocr_raw, by = "preproc_path") %>%
    mutate(nchar_ocr = nchar(ocr_text %||% ""))

  # Debug: small previews
  if (!is.null(debug_dir) && nrow(ocr_df) > 0) {
    head2 <- head(ocr_df, 2)
    for (i in seq_len(nrow(head2))) {
      txt <- substr(head2$ocr_text[i] %||% "", 1, 300)
      writeLines(txt, file.path(debug_dir, paste0("ocr_preview_", i, ".txt")))
    }
  }

  # 2) LLM text-only pass where OCR is long enough
  source("pipeline/extract_openai_text.R")
  cli::cli_alert_info("[extract_openai_text.R] strict-month (band+date+time only)")

  llm_in <- ocr_df %>%
    filter(nchar_ocr >= min_chars) %>%
    transmute(source_image, ocr_text, month_name_es, year)

  from_text <- if (nrow(llm_in) == 0) {
    cli::cli_alert_warning(glue("Vision OCR produced < {min_chars} chars for all images. Falling back to OpenAI image for all."))
    tibble(source_image = character(), event_date = as.Date(character()), band_name = character(), event_time = character())
  } else {
    extract_openai_text(llm_in)
  }

  # 3) For short OCR images, fallback to OpenAI image (preprocessed)
  short_df <- ocr_df %>% filter(nchar_ocr < min_chars)
  from_image <- if (nrow(short_df) > 0) {
    cli::cli_alert_info(glue("OpenAI image fallback for {nrow(short_df)} poster(s) with short OCR."))
    idx <- match(short_df$source_image, eff_df$source_image)
    run_openai_image_pipeline(eff_df[idx, ])
  } else {
    tibble(source_image = character(), event_date = as.Date(character()), band_name = character(), event_time = character())
  }

  # 4) Combine and dedupe
  all_extracted <- bind_rows(from_text, from_image) %>%
    distinct(source_image, event_date, band_name, event_time, .keep_all = TRUE)
}

# ---- Per-image counts ----
all_extracted <- eff_df %>%
  select(source_image) %>% distinct() %>%
  left_join(all_extracted, by = "source_image")

per_image_counts <- all_extracted %>%
  group_by(source_image) %>%
  summarise(events = sum(!is.na(event_date) & nzchar(coalesce(band_name, ""))), .groups = "drop")

cli::cli_alert_info("Per-image extracted events:")
print(per_image_counts, n = nrow(per_image_counts))

# ---- Validation / merges / outputs ----
source("pipeline/validate.R")
cli::cli_alert_info("[validate.R] minimal + per-venue manual overrides + safe-venue-cols")

validated <- validate(
  extracted   = all_extracted,
  meta        = usable_meta,
  venues_path = venues_path,
  images_dir  = images_dir
)

perf_path <- file.path(out_dir, "performances_monthly.csv")
write_csv(validated$performances, perf_path)
cli::cli_alert_success(glue("Wrote: {perf_path}"))

dir.create(agg_dir, showWarnings = FALSE, recursive = TRUE)
write_csv(validated$agg_municipality, file.path(agg_dir, "events_by_municipality.csv"))
write_csv(validated$agg_state,       file.path(agg_dir, "events_by_state.csv"))
cli::cli_alert_success(glue("Wrote aggregations to: {agg_dir}"))

cli::cli_alert_success("Done.")
