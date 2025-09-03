# pipeline/run_monthly.R
# v2.3.1 Vision (with preprocess) + strict month/year + per-image OpenAI fallback + manual overrides + deep debug

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

cli::cli_h1(">> run_monthly v2.3.1 (Vision+preprocess + strict month/year + per-image OpenAI fallback + debug)")

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

# Debug snapshot
if (!is.null(debug_dir)) {
  options(mapa.debug_dir = debug_dir)
  envdump <- c(
    sprintf("OCR_PROVIDER=%s", Sys.getenv("OCR_PROVIDER")),
    sprintf("GCP_VISION_API_KEY=%s", ifelse(nzchar(Sys.getenv("GCP_VISION_API_KEY")),"set","")),
    sprintf("GOOGLE_APPLICATION_CREDENTIALS=%s", Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS")),
    sprintf("OPENAI_TEXT_MODEL=%s", Sys.getenv("OPENAI_TEXT_MODEL")),
    sprintf("OPENAI_IMAGE_MODEL=%s", Sys.gete
