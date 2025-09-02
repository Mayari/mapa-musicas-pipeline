# pipeline/run_monthly.R
# v1.5.0 Vision-first (auto-detect) + minimal OpenAI text pass + manual overrides + debug

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

cli::cli_h1(">> run_monthly v1.5.0 (Vision-first + minimal + overrides + debug)")

# ---- Args ----
# --images_dir <dir>
# --venues_path <file>
# --out_dir <dir>
# --agg_dir <dir>
# --debug_dir <dir>    (optional)

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

# Wire up debug option for downstream scripts
if (!is.null(debug_dir)) {
  options(mapa.debug_dir = debug_dir)
  # Save env snapshot for OCR/LLM knobs
  envdump <- c(
    sprintf("OCR_PROVIDER=%s", Sys.getenv("OCR_PROVIDER")),
    sprintf("GCP_VISION_API_KEY=%s", ifelse(nzchar(Sys.getenv("GCP_VISION_API_KEY")),"set","")),
    sprintf("GOOGLE_APPLICATION_CREDENTIALS=%s", Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS")),
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

# ---- Provider selection ----
provider_env <- tolower(Sys.getenv("OCR_PROVIDER", unset = ""))
has_vision   <- nzchar(Sys.getenv("GCP_VISION_API_KEY")) || nzchar(Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

provider <- dplyr::case_when(
  provider_env %in% c("vision","tesseract","auto") ~ provider_env,
  has_vision ~ "vision",
  TRUE ~ "tesseract"
)

cli::cli_alert_info("OCR provider selection: {provider} (env OCR_PROVIDER='{provider_env}', vision_cred={has_vision})")

# ---- OCR functions (load as needed) ----
min_chars <- as.integer(Sys.getenv("MIN_OCR_CHARS_FOR_LLM", unset = "120"))
if (!is.null(debug_dir)) writeLines(sprintf("MIN_OCR_CHARS_FOR_LLM=%s", min_chars), file.path(debug_dir, "env_llm.txt"))

run_tesseract <- function(paths) {
  source("pipeline/extract_tesseract.R")
  extract_tesseract(paths)
}

run_vision <- function(paths) {
  source("pipeline/extract_google_vision.R")
  extract_google_vision(paths, language_hints = c("es","en"), feature = "DOCUMENT_TEXT_DETECTION")
}

# ---- Execute OCR based on provider ----
ocr_df <- tibble(source_image = character(), ocr_text = character(), psm_used = NA_integer_)

if (provider == "vision") {
  cli::cli_h2("OCR via Google Vision (primary)")
  ocr_df <- run_vision(usable_meta$source_image)
  # harmonize columns
  if (!"psm_used" %in% names(ocr_df)) ocr_df$psm_used <- NA_integer_

} else if (provider == "tesseract") {
  cli::cli_h2("OCR via Tesseract (primary)")
  ocr_df <- run_tesseract(usable_meta$source_image)

} else { # auto
  cli::cli_h2("OCR auto: Tesseract -> Vision for short texts")
  tes <- run_tesseract(usable_meta$source_image)
  need_boost <- tes %>% mutate(n = nchar(ocr_text)) %>% filter(n < min_chars)
  if (nrow(need_boost) > 0 && has_vision) {
    vis <- run_vision(need_boost$source_image)
    # prefer whichever yields longer text
    tes <- tes %>%
      left_join(vis %>% select(source_image, ocr_text_vis = ocr_text), by = "source_image") %>%
      mutate(ocr_text = ifelse(nchar(coalesce(ocr_text_vis, "")) > nchar(coalesce(ocr_text, "")),
                               ocr_text_vis, ocr_text)) %>%
      select(-ocr_text_vis)
  }
  ocr_df <- tes
}

# ---- Minimal OpenAI text-only extractor on OCR text ----
source("pipeline/extract_openai_text.R")
cli::cli_alert_info("[extract_openai_text.R] v2025-09-02 minimal (band+date+time only)")

ocr_df <- ocr_df %>% mutate(use_llm = nchar(ocr_text) >= min_chars)
llm_in  <- ocr_df %>% filter(use_llm) %>% select(source_image, ocr_text)

if (nrow(llm_in) == 0) {
  cli::cli_alert_warning("All OCR texts are below threshold ({min_chars} chars). LLM text-only pass will be skipped.")
  extracted_events <- tibble(source_image = character(),
                             event_date = as.Date(character()),
                             band_name = character(),
                             event_time = character())
} else {
  extracted_events <- extract_openai_text(llm_in)

  # Normalize expected column names
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

# Keep one row per source_image even if empty, for per-image counts
all_extracted <- ocr_df %>%
  select(source_image) %>%
  distinct() %>%
  left_join(extracted_events, by = "source_image")

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
