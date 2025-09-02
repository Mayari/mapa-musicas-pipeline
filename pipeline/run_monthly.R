# pipeline/run_monthly.R
# v1.5.1 Vision-first (filename-tolerant) + minimal OpenAI text pass + manual overrides + debug

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

cli::cli_h1(">> run_monthly v1.5.1 (Vision-first + minimal + overrides + debug)")

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
