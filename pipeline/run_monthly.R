#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(stringr)
  library(readr)
  library(glue)
})

cat(">> run_monthly vBASE-image (vision only; no OCR; no residency rules)\n")

safe_source <- function(path){
  if (file.exists(path)) {
    tryCatch({ source(path, chdir = TRUE) ; TRUE },
             error = function(e){ message(sprintf("!! could not source %s: %s", path, e$message)); FALSE })
  } else { FALSE }
}

# Load helpers we need
ok1 <- safe_source("pipeline/validate.R")
ok2 <- safe_source("pipeline/extract_openai.R")
if (!ok2) stop("pipeline/extract_openai.R is required for image extraction.")

args <- commandArgs(trailingOnly = TRUE)
get_arg <- function(flag, default=NULL){
  hit <- which(args == flag)
  if (length(hit) && hit < length(args)) args[hit+1] else default
}

images_dir <- get_arg("--images_dir", "carteleras")
venues_path <- get_arg("--venues_path", "data/venues.csv")
out_dir   <- get_arg("--out_dir",   "data")
agg_dir   <- get_arg("--agg_dir",   file.path(out_dir, "aggregations"))

dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(agg_dir, showWarnings = FALSE, recursive = TRUE)

month_map <- c(
  "enero"=1, "febrero"=2, "marzo"=3, "abril"=4, "mayo"=5, "junio"=6,
  "julio"=7, "agosto"=8, "septiembre"=9, "setiembre"=9, "octubre"=10,
  "noviembre"=11, "diciembre"=12
)

is_poster <- function(p) grepl("\\.(jpg|jpeg|png)$", tolower(p))  # PDFs skipped in image-only mode
all_files <- list.files(images_dir, recursive = TRUE, full.names = TRUE)
poster_files <- all_files[file.info(all_files)$isdir %in% c(FALSE) & vapply(all_files, is_poster, TRUE)]

if (!length(poster_files)) {
  message("No poster files found under: ", images_dir)
  empty <- tibble(venue=character(), event_date=as.Date(character()), weekday=character(),
                  band_name=character(), event_time=character(),
                  municipality=character(), state=character(),
                  latitude=double(), longitude=double(),
                  event_title=character(), venue_id=character())
  write_csv(empty, file.path(out_dir, "performances_monthly.csv"))
  write_csv(tibble(), file.path(agg_dir, "events_by_municipality.csv"))
  write_csv(tibble(), file.path(agg_dir, "events_by_state.csv"))
  quit(status = 0)
}

example_str <- paste(head(poster_files, 3), collapse = " | ")
cat("Discovered", length(poster_files), "poster file(s) under:", images_dir, "\n")
cat("Example:", example_str, "\n")

parse_one <- function(p){
  rel <- sub(paste0("^", normalizePath(images_dir), .Platform$file.sep), "", normalizePath(p))
  parts <- strsplit(rel, .Platform$file.sep, fixed = TRUE)[[1]]
  state <- if (length(parts) > 1) parts[1] else NA_character_

  file <- tools::file_path_sans_ext(basename(p))
  toks <- unlist(strsplit(file, "_"))
  if (length(toks) < 3) return(NULL)

  ym <- toks[length(toks)-1]
  num <- suppressWarnings(as.integer(toks[length(toks)]))
  venue_tokens <- toks[1:(length(toks)-2)]
  venue <- gsub("_", " ", paste(venue_tokens, collapse = "_"))
  venue <- stringr::str_squish(venue)

  m <- stringr::str_match(ym, "^(\\d{4})([A-Za-zÁÉÍÓÚáéíóúñÑ]+)$")
  year <- suppressWarnings(as.integer(m[,2]))
  mes  <- tolower(iconv(m[,3], to="ASCII//TRANSLIT"))
  month <- month_map[mes] %||% NA_integer_

  tibble(
    source_image = p,
    state = state,
    venue = venue,
    year = year,
    month = as.integer(month),
    file_num = num %||% NA_integer_
  )
}

meta <- purrr::map_dfr(poster_files, function(p) { tryCatch(parse_one(p), error=function(e) NULL) })
meta <- meta %>% filter(!is.na(year), !is.na(month), nzchar(venue))
cat("Parsed metadata rows (usable):", nrow(meta), "\n")
if (!nrow(meta)) stop("No parsable filenames. Check naming convention.")

venues_df <- tryCatch(readr::read_csv(venues_path, show_col_types = FALSE), error = function(e) tibble())
for (nm in c("venue","municipality","state","latitude","longitude","lat","lon")) if (!nm %in% names(venues_df)) venues_df[[nm]] <- NA

message("Running OpenAI image extraction…")
events_list <- purrr::pmap(meta, function(source_image, state, venue, year, month, file_num){
  tryCatch(extract_openai_events(source_image, venue, year, month), error=function(e) tibble())
})
events_raw <- bind_rows(events_list)

# Per-image counts (safe)
clean <- events_raw
if (!"source_image" %in% names(clean)) clean$source_image <- NA_character_
per_img <- clean %>% filter(!is.na(source_image)) %>% count(source_image, name = "events")
cat("Per-image extracted events:\n")
print(per_img)

# Validate + join
if (!exists("validate_events", mode="function")) stop("validate.R not loaded or validate_events() not found.")
final <- validate_events(events_raw, venues_df = venues_df)

outfile <- file.path(out_dir, "performances_monthly.csv")
readr::write_csv(final, outfile)
cat("Wrote:", outfile, "\n")

# Aggregations (guard against empty)
if (!nrow(final)) {
  readr::write_csv(tibble(), file.path(agg_dir, "events_by_municipality.csv"))
  readr::write_csv(tibble(), file.path(agg_dir, "events_by_state.csv"))
  cat("No rows; wrote empty aggregations.\n")
  cat("Done.\n"); quit(status = 0)
}

agg_in <- final %>% mutate(year = year(event_date), month = month(event_date))
agg_muni <- agg_in %>% count(municipality, year, month, name = "events") %>% arrange(municipality, year, month)
agg_state <- agg_in %>% count(state, year, month, name = "events") %>% arrange(state, year, month)
readr::write_csv(agg_muni, file.path(agg_dir, "events_by_municipality.csv"))
readr::write_csv(agg_state, file.path(agg_dir, "events_by_state.csv"))
cat("Wrote aggregations to:", agg_dir, "\n")

cat("Done.\n")
