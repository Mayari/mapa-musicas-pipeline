# pipeline/run_monthly.R
# v1.4.3 minimal + per-venue overrides + source_image-safe + debug_dir support

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(stringr)
  library(lubridate)
  library(tidyr)
  library(cli)
  library(glue)
  library(purrr)
})

cli::cli_h1(">> run_monthly v1.4.3 (minimal + per-venue overrides + source_image-safe + debug)")

# ---- Args ----
# --images_dir <dir>
# --venues_path <file>
# --out_dir <dir>
# --agg_dir <dir>
# --debug_dir <dir>    (optional; when present, raw OCR text & env dump are saved)

args <- commandArgs(trailingOnly = TRUE)
get_arg <- function(flag, default = NULL) {
  i <- which(args == flag)
  if (length(i) == 0) return(default)
  if (i == length(args)) return(default)
  args[i + 1]
}

images_dir <- get_arg("--images_dir", "../carteleras")
venues_path <- get_arg("--venues_path", "data/venues.csv")
out_dir <- get_arg("--out_dir", "data")
agg_dir <- get_arg("--agg_dir", "data/aggregations")
debug_dir <- get_arg("--debug_dir", NULL)

# Ensure directories
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(agg_dir, showWarnings = FALSE, recursive = TRUE)
if (!is.null(debug_dir)) dir.create(debug_dir, showWarnings = FALSE, recursive = TRUE)

# Wire up debug option for downstream scripts
if (!is.null(debug_dir)) {
  options(mapa.debug_dir = debug_dir)
  # Save env snapshot for OCR knobs
  envdump <- c(
    sprintf("OCR_LANGS=%s", Sys.getenv("OCR_LANGS")),
    sprintf("OCR_DPI=%s", Sys.getenv("OCR_DPI")),
    sprintf("OCR_MIN_WIDTH=%s", Sys.getenv("OCR_MIN_WIDTH")),
    sprintf("OCR_PSMS=%s", Sys.getenv("OCR_PSMS")),
    sprintf("OPENAI_TEXT_MODEL=%s", Sys.getenv("OPENAI_TEXT_MODEL")),
    sprintf("EXTRACT_IMAGE_PASS=%s", Sys.getenv("EXTRACT_IMAGE_PASS"))
  )
  writeLines(envdump, file.path(debug_dir, "env.txt"))
}

# ---- Discover poster files ----
is_poster <- function(x) {
  grepl("\\.(jpg|jpeg|png|pdf)$", x, ignore.case = TRUE)
}
poster_paths <- list.files(images_dir, pattern = NULL, recursive = TRUE, full.names = TRUE)
poster_paths <- poster_paths[is_poster(poster_paths)]

cli::cli_alert_info("Discovered {length(poster_paths)} poster file(s) under: {images_dir}")
if (length(poster_paths) > 0) {
  eg <- head(poster_paths, 3)
  cli::cli_alert_info("Example: {paste(eg, collapse=' | ')}")
}

# ---- Parse filename metadata (Venue_YYYYMesEspañol_n.ext) ----
# Returns tibble: source_image, venue_guess, year, month_name_es, file_num
parse_filename <- function(path) {
  b <- basename(path)
  # Example: Jazzatlan_Cholula_2024Enero_1.jpeg  OR  Mendrugo_2024Marzo_2.jpg
  # Accept both with/without municipality in middle chunk
  m <- str_match(b, "^(.+?)_(\\d{4})([A-Za-zñÑ]+)_(\\d+)\\.(jpg|jpeg|png|pdf)$")
  if (is.na(m[1,1])) {
    # Try with venue_muni form: Venue_Municipio_YYYYMes_#
    m2 <- str_match(b, "^(.+?)_(\\d{4})([A-Za-zñÑ]+)_(\\d+)\\.(jpg|jpeg|png|pdf)$")
    # (regex is identical here; kept for readability if you later add a variant)
    m <- m2
  }
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

meta_df <- map_df(poster_paths, parse_filename)

usable_meta <- meta_df %>% filter(!is.na(venue_guess), !is.na(year))
cli::cli_alert_info("Parsed metadata rows (usable): {nrow(usable_meta)}")

# ---- OCR stage ----
source("pipeline/extract_tesseract.R")
ocr_df <- extract_tesseract(usable_meta$source_image)

# ---- Minimal text extractor (OpenAI text-only on OCR text) ----
# This file is assumed to exist in your repo. We don't change its logic here,
# but we *do* print its version banner and rely on it to be minimal & omit-if-unsure.
source("pipeline/extract_openai_text.R")
cli::cli_alert_info("[extract_openai_text.R] v2025-09-02 minimal (band+date+time only)")

# OPTIONAL GUARD: if OCR text is too short, skip LLM call for that row
min_chars <- as.integer(Sys.getenv("MIN_OCR_CHARS_FOR_LLM", unset = "120"))
if (!is.null(debug_dir)) writeLines(sprintf("MIN_OCR_CHARS_FOR_LLM=%s", min_chars), file.path(debug_dir, "env_llm.txt"))

ocr_df <- ocr_df %>%
  mutate(use_llm = nchar(ocr_text) >= min_chars)

# Call your minimal extractor only on rows with enough text
llm_in  <- ocr_df %>% filter(use_llm) %>% select(source_image, ocr_text)
if (nrow(llm_in) == 0) {
  cli::cli_alert_warning("All OCR texts are below threshold ({min_chars} chars). LLM text-only pass will be skipped.")
  extracted_events <- tibble(source_image = character(), event_date = as.Date(character()), band_name = character(), event_time = character())
} else {
  extracted_events <- extract_openai_text(llm_in)  # must return: source_image, date/band/time cols
  # Ensure expected names (be lenient to schema keys)
  possible_date_cols <- c("event_date", "date")
  possible_band_cols <- c("band_name", "band")
  possible_time_cols <- c("event_time", "time", "hora")

  if (!"event_date" %in% names(extracted_events)) {
    hit <- intersect(names(extracted_events), possible_date_cols)
    if (length(hit) > 0) extracted_events <- extracted_events %>% rename(event_date = !!hit[1])
  }
  if (!"band_name" %in% names(extracted_events)) {
    hit <- intersect(names(extracted_events), possible_band_cols)
    if (length(hit) > 0) extracted_events <- extracted_events %>% rename(band_name = !!hit[1])
  }
  if (!"event_time" %in% names(extracted_events)) {
    hit <- intersect(names(extracted_events), possible_time_cols)
    if (length(hit) > 0) extracted_events <- extracted_events %>% rename(event_time = !!hit[1])
  }
}

# Join back non-LLM rows as blanks (so we can count per-image)
all_extracted <- ocr_df %>%
  select(source_image) %>%
  distinct() %>%
  left_join(extracted_events, by = "source_image")

# ---- Log per-image counts ----
per_image_counts <- all_extracted %>%
  group_by(source_image) %>%
  summarise(events = sum(!is.na(event_date) & nzchar(coalesce(band_name, ""))), .groups = "drop")

cli::cli_alert_info("Per-image extracted events:")
print(per_image_counts, n = nrow(per_image_counts))

# ---- Validation / manual overrides / joins ----
source("pipeline/validate.R")
cli::cli_alert_info("[validate.R] v2025-09-02 minimal + per-venue manual overrides + safe-venue-cols")

# validate() is expected to accept:
#  - extracted (with source_image, event_date, band_name, event_time)
#  - meta_df (for venue parsing)
#  - venues_path (for joins)
#  - images_dir (for relative paths if needed)
validated <- validate(
  extracted = all_extracted,
  meta = usable_meta,
  venues_path = venues_path,
  images_dir = images_dir
)

# ---- Write outputs ----
perf_path <- file.path(out_dir, "performances_monthly.csv")
write_csv(validated$performances, perf_path)
cli::cli_alert_success("Wrote: {perf_path}")

# Aggregations
dir.create(agg_dir, showWarnings = FALSE, recursive = TRUE)
agg_muni_path <- file.path(agg_dir, "events_by_municipality.csv")
agg_state_path <- file.path(agg_dir, "events_by_state.csv")
write_csv(validated$agg_municipality, agg_muni_path)
write_csv(validated$agg_state,       agg_state_path)
cli::cli_alert_success("Wrote aggregations to: {agg_dir}")

cli::cli_alert_success("Done.")
