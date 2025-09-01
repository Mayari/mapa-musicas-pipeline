#!/usr/bin/env Rscript
# Filename-based ingest:
# - Posters live under carteleras/<STATE>/...
# - Filenames: <Venue_Name>_<YYYY><MesEnEspañol>_<n>.(jpg|jpeg|png)
# - Also supports: <YYYY><MesEnEspañol>_<Venue_Name>_<n>.(jpg|jpeg|png)

suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(jsonlite)
  library(stringr)
})

# ---- version banner ----
VERSION <- "run_monthly filename-ingest v1.1 (weekday+time)"
message(">> ", VERSION)

# ---- arg parsing ----
args <- commandArgs(trailingOnly = TRUE)
get_arg <- function(flag, default = NULL){
  i <- which(args == flag)
  if (length(i) == 0 || i == length(args)) return(default)
  args[i + 1]
}

images_dir    <- get_arg("--images_dir",  "carteleras")
venues_path   <- get_arg("--venues_path", "data/venues.csv")
out_dir       <- get_arg("--out_dir",     "data")
agg_dir       <- get_arg("--agg_dir",     file.path(out_dir, "aggregations"))
manifest_path <- get_arg("--manifest_path", "posters_manifest.csv") # optional CSV override

# ---- load helpers ----
safe_source <- function(p){ if (file.exists(p)) source(p) else message("Missing ", p, " (skipping)") }
safe_source("pipeline/extract_openai.R")
safe_source("pipeline/extract_gvision.R")
safe_source("pipeline/parse_posters.R")
safe_source("pipeline/validate.R")
safe_source("pipeline/aggregate.R")

# ---- helpers (month map, normalization, parsing) ----
month_map <- c(
  "enero"=1, "ene"=1,
  "febrero"=2, "feb"=2,
  "marzo"=3, "mar"=3,
  "abril"=4, "abr"=4,
  "mayo"=5, "may"=5,
  "junio"=6, "jun"=6,
  "julio"=7, "jul"=7,
  "agosto"=8, "ago"=8,
  "septiembre"=9, "sep"=9, "setiembre"=9, "set"=9,
  "octubre"=10, "oct"=10,
  "noviembre"=11, "nov"=11,
  "diciembre"=12, "dic"=12
)

norm_name <- function(x){
  x |>
    tolower() |>
    gsub("_", " ", x = _) |>
    gsub("\\s+", " ", x = _) |>
    trimws()
}

get_state_from_path <- function(path){
  parts <- strsplit(path, "/")[[1]]
  i <- which(parts == "carteleras")
  if (length(i) && length(parts) >= i + 1) return(parts[i + 1])
  NA_character_
}

# Parse from filename:
# Pattern A: <venue>_<YYYY><mes>_<n>
# Pattern B: <YYYY><mes>_<venue>_<n>
parse_from_filename <- function(path){
  fn <- basename(path)
  stem <- tools::file_path_sans_ext(fn)
  stem_lc <- tolower(stem)

  # A) <venue>_<YYYY><mes>_<n>
  m <- str_match(stem_lc, "^(.*)_((20)\\d{2})([a-záéíóúñ]{3,10})_(\\d+)$")
  if (!all(is.na(m))) {
    venue_raw <- m[,2]
    yr  <- as.integer(m[,3])
    mo_tok <- gsub("[^a-z]", "", m[,5])
    mo  <- suppressWarnings(as.integer(month_map[mo_tok]))
    pg  <- as.integer(m[,6])
    return(tibble(
      image_path = path,
      state      = get_state_from_path(path),
      venue_name = gsub("_", " ", venue_raw),
      venue_norm = norm_name(venue_raw),
      year       = yr,
      month      = mo,
      page       = pg
    ))
  }

  # B) <YYYY><mes>_<venue>_<n>
  m <- str_match(stem_lc, "^((20)\\d{2})([a-záéíóúñ]{3,10})_(.*)_(\\d+)$")
  if (!all(is.na(m))) {
    yr  <- as.integer(m[,2])
    mo_tok <- gsub("[^a-z]", "", m[,4])
    mo  <- suppressWarnings(as.integer(month_map[mo_tok]))
    venue_raw <- m[,5]
    pg  <- as.integer(m[,6])
    return(tibble(
      image_path = path,
      state      = get_state_from_path(path),
      venue_name = gsub("_", " ", venue_raw),
      venue_norm = norm_name(venue_raw),
      year       = yr,
      month      = mo,
      page       = pg
    ))
  }

  tibble(
    image_path = path,
    state      = get_state_from_path(path),
    venue_name = NA_character_, venue_norm = NA_character_,
    year = NA_integer_, month = NA_integer_, page = NA_integer_
  )
}

# ---- discover posters ----
imgs <- list.files(images_dir, pattern = "\\.(png|jpg|jpeg)$", recursive = TRUE, full.names = TRUE)
message("Discovered ", length(imgs), " poster file(s) under: ", images_dir)
if (length(imgs)) message("Example: ", paste(utils::head(imgs, 3), collapse = " | "))
if (!length(imgs)) { message("No images found under ", images_dir); quit(save = "no", status = 0) }

# ---- build metadata (or take manifest) ----
if (file.exists(manifest_path)){
  message("Using manifest at ", manifest_path)
  meta <- readr::read_csv(manifest_path, show_col_types = FALSE) |>
    mutate(
      image_path = image_path,
      venue_norm = norm_name(venue_name),
      state      = ifelse(is.na(state), get_state_from_path(image_path), state),
      page       = suppressWarnings(as.integer(page))
    )
} else {
  meta <- purrr::map_dfr(imgs, parse_from_filename)
}

bad <- meta |> filter(is.na(venue_name) | is.na(year) | is.na(month))
if (nrow(bad)) message("Skipping ", nrow(bad), " files with unrecognized names. Example: ", bad$image_path[1])
meta <- meta |> filter(!is.na(venue_name), !is.na(year), !is.na(month))
meta <- meta |> rename(venue = venue_name)
message("Parsed metadata rows (usable): ", nrow(meta))
if (!nrow(meta)) { message("No usable filenames parsed. Exiting."); quit(save="no", status=0) }

# ---- run extractors ----
use_openai <- nzchar(Sys.getenv("OPENAI_API_KEY"))
use_gcv    <- nzchar(Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

openai_events <- tibble()
vision_raw    <- tibble()

if (use_openai && exists("extract_openai_events")) {
  message("Running OpenAI extraction…")
  openai_events <- purrr::map_dfr(seq_len(nrow(meta)), function(i){
    row <- meta[i,]
    tryCatch(
      extract_openai_events(
        image_path = row$image_path,
        venue_name = row$venue,
        year       = row$year,
        month      = row$month
      ),
      error = function(e){
        message("OpenAI failed for ", row$image_path, ": ", e$message)
        tibble()
      }
    )
  })
} else {
  message("Skipping OpenAI extraction (no key or extractor missing).")
}

if (use_gcv && exists("extract_gvision_text")) {
  message("Running Google Vision OCR…")
  vision_raw <- purrr::map_dfr(seq_len(nrow(meta)), function(i){
    row <- meta[i,]
    tryCatch(
      extract_gvision_text(
        image_path = row$image_path,
        venue_id   = NA_character_,
        venue_name = row$venue,
        year       = row$year,
        month      = row$month
      ),
      error = function(e){
        message("Vision failed for ", row$image_path, ": ", e$message)
        tibble()
      }
    )
  })
} else {
  message("Skipping Vision OCR (no creds or extractor missing).")
}

# Parse Vision OCR into events (if any)
vision_events <- if (nrow(vision_raw) && exists("parse_poster_events")) parse_poster_events(vision_raw) else tibble()

# Ensure both sources have event_time column
if (!"event_time" %in% names(openai_events)) openai_events <- openai_events %>% mutate(event_time = NA_character_)
if (!"event_time" %in% names(vision_events)) vision_events <- vision_events %>% mutate(event_time = NA_character_)

# Combine & dedupe
openai_events <- openai_events |> mutate(source = "openai")
vision_events <- vision_events |> mutate(source = "gvision")
all_events <- bind_rows(openai_events, vision_events) |> distinct()

# Validate & clean (adds confidence, dedupes by venue/date/band) if available
clean <- if (exists("validate_events")) validate_events(all_events) else all_events

# ---- add weekday (Spanish) ----
# Map lubridate::wday (Mon=1) -> c("lunes", ..., "domingo")
if (nrow(clean)) {
  clean <- clean %>%
    mutate(
      weekday = c("lunes","martes","miércoles","jueves","viernes","sábado","domingo")[lubridate::wday(event_date, week_start = 1)]
    )
}

# ---- write outputs ----
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(agg_dir, showWarnings = FALSE, recursive = TRUE)

if (!nrow(clean)) {
  message("No events extracted; writing empty CSV with headers.")
  clean <- tibble(
    venue = character(),
    venue_id = character(),
    event_date = as.Date(character()),
    band_name = character(),
    event_time = character(),       # <- new column
    source = character(),
    source_image = character(),
    weekday = character()           # <- new column
  )
}
readr::write_csv(clean, file.path(out_dir, "performances_monthly.csv"))

if (exists("aggregate_all")) {
  aggregate_all(
    events_path = file.path(out_dir, "performances_monthly.csv"),
    venues_path = venues_path,
    out_dir     = agg_dir
  )
} else {
  message("aggregate_all() not found; skipping aggregation.")
}

message("Done.")
