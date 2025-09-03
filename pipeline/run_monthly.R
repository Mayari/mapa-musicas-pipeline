# pipeline/run_monthly.R
# v2.1.0 Vision/OpenAI only + strict month/year + manual overrides + debug

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

cli::cli_h1(">> run_monthly v2.1.0 (Vision/OpenAI only + strict month/year + overrides + debug)")

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

# Wire up debug option
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

# ---- Convert first page of PDFs to PNG ----
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

# ---- Provider selection: vision | openai | auto ----
provider_env <- tolower(Sys.getenv("OCR_PROVIDER", unset = "auto"))
has_vision   <- nzchar(Sys.getenv("GCP_VISION_API_KEY")) || nzchar(Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

provider <- dplyr::case_when(
  provider_env %in% c("vision","openai","auto") ~ provider_env,
  TRUE ~ "auto"
)
if (provider == "auto") provider <- if (has_vision) "vision" else "openai"
cli::cli_alert_info("OCR provider selection: {provider} (vision_cred={has_vision})")

# ---- OCR/extraction functions ----

# Vision: OCR text -> strict minimal OpenAI text extractor (with month/year hints)
run_vision_pipeline <- function(df) {
  cand_files <- c("pipeline/extract_gvision.R", "pipeline/extract_google_vision.R")
  for (f in cand_files) if (file.exists(f)) try(source(f), silent = TRUE)
  stopifnot(exists("extract_gvision") || exists("extract_google_vision"))
  gv <- if (exists("extract_gvision")) extract_gvision else extract_google_vision

  ocr_df <- gv(df$effective_path, language_hints = c("es","en"), feature = "DOCUMENT_TEXT_DETECTION") %>%
    rename(effective_path = source_image)
  ocr_df <- df %>% left_join(ocr_df, by = "effective_path") %>%
    select(source_image, ocr_text, month_name_es, year)

  source("pipeline/extract_openai_text.R")
  cli::cli_alert_info("[extract_openai_text.R] v2025-09-02 strict-month (band+date+time only)")

  min_chars <- 60L
  llm_in <- ocr_df %>%
    mutate(use_llm = nchar(ocr_text) >= min_chars) %>%
    filter(use_llm) %>% select(source_image, ocr_text, month_name_es, year)

  if (nrow(llm_in) == 0) {
    cli::cli_alert_warning("Vision OCR produced < {min_chars} chars for all images; skipping text-only pass.")
    return(tibble(source_image = character(),
                  event_date = as.Date(character()),
                  band_name = character(),
                  event_time = character()))
  }

  extract_openai_text(llm_in)  # returns strict-month filtered rows
}

# OpenAI image: direct minimal extraction (strict month/year)
run_openai_image_pipeline <- function(df) {
  source("pipeline/extract_openai.R")
  cli::cli_alert_info("[extract_openai.R] strict-month image → {date,band,time}")
  extract_openai(
    image_paths = df$effective_path,
    month_es = df$month_name_es,
    year = df$year,
    source_image_ids = df$source_image
  )
}

# ---- Execute ----
if (nrow(eff_df) == 0) {
  all_extracted <- tibble(source_image = character(),
                          event_date = as.Date(character()),
                          band_name = character(),
                          event_time = character())
} else if (provider == "vision") {
  all_extracted <- run_vision_pipeline(eff_df)
} else if (provider == "openai") {
  all_extracted <- run_openai_image_pipeline(eff_df)
} else {
  all_extracted <- if (has_vision) run_vision_pipeline(eff_df) else run_openai_image_pipeline(eff_df)
}

# Ensure row for each poster (for per-image counts)
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
cli::cli_alert_info("[validate.R] v2025-09-02 minimal + per-venue manual overrides + safe-venue-cols")

validated <- validate(
  extracted   = all_extracted,
  meta        = usable_meta,
  venues_path = venues_path,
  images_dir  = images_dir
)

perf_path <- file.path(out_dir, "performances_monthly.csv")
write_csv(validated$performances, perf_path)
cli::cli_alert_success("Wrote: {perf_path}")

dir.create(agg_dir, showWarnings = FALSE, recursive = TRUE)
agg_muni_path  <- file.path(agg_dir, "events_by_municipality.csv")
agg_state_path <- file.path(agg_dir, "events_by_state.csv")
write_csv(validated$agg_municipality, agg_muni_path)
write_csv(validated$agg_state,       agg_state_path)
cli::cli_alert_success("Wrote aggregations to: {agg_dir}")

cli::cli_alert_success("Done.")
