#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(stringr)
  library(readr)
  library(glue)
})

cat(">> run_monthly v1.4.2 (minimal + per-venue overrides + source_image-safe)\n")

# --- Safe sourcing -------------------------------------------------------------
safe_source <- function(path){
  if (file.exists(path)) {
    tryCatch({ source(path, chdir = TRUE) ; TRUE },
             error = function(e){ message(sprintf("!! could not source %s: %s", path, e$message)); FALSE })
  } else { FALSE }
}

# Pipeline helpers (best-effort; okay if some are missing)
safe_source("pipeline/extract_openai.R")
safe_source("pipeline/extract_openai_text.R")
safe_source("pipeline/extract_tesseract.R")
safe_source("pipeline/validate.R")

# --- Args ---------------------------------------------------------------------
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

# --- List posters & parse filename metadata -----------------------------------
# Expect paths like: <images_dir>/<STATE>/<Venue>_<YYYY><MesEspañol>_<n>.<ext>
month_map <- c(
  "enero"=1, "febrero"=2, "marzo"=3, "abril"=4, "mayo"=5, "junio"=6,
  "julio"=7, "agosto"=8, "septiembre"=9, "setiembre"=9, "octubre"=10,
  "noviembre"=11, "diciembre"=12
)

is_poster <- function(p) grepl("\\.(jpg|jpeg|png|pdf)$", tolower(p))
all_files <- list.files(images_dir, recursive = TRUE, full.names = TRUE)
poster_files <- all_files[file.info(all_files)$isdir %in% c(FALSE) & vapply(all_files, is_poster, TRUE)]

if (!length(poster_files)) {
  message("No poster files found under: ", images_dir)
  # write empty outputs and exit cleanly
  write_csv(tibble(venue=character(), event_date=as.Date(character()), weekday=character(),
                   band_name=character(), event_time=character(),
                   municipality=character(), state=character(),
                   latitude=double(), longitude=double(), venue_id=character()),
            file.path(out_dir, "performances_monthly.csv"))
  write_csv(tibble(), file.path(agg_dir, "events_by_municipality.csv"))
  write_csv(tibble(), file.path(agg_dir, "events_by_state.csv"))
  quit(status = 0)
}

example_str <- paste(head(poster_files, 3), collapse = " | ")
cat("Discovered", length(poster_files), "poster file(s) under:", images_dir, "\n")
cat("Example:", example_str, "\n")

parse_one <- function(p){
  # state from first folder inside images_dir if present
  rel <- sub(paste0("^", normalizePath(images_dir), .Platform$file.sep), "", normalizePath(p))
  parts <- strsplit(rel, .Platform$file.sep, fixed = TRUE)[[1]]
  state <- if (length(parts) > 1) parts[1] else NA_character_

  file <- tools::file_path_sans_ext(basename(p))
  # Split on underscores, expecting VenueName_YYYYMes_# (venue itself may contain underscores)
  toks <- unlist(strsplit(file, "_"))
  if (length(toks) < 3) return(NULL)

  # venue may be multiple tokens; the last two are YYYYMes and num
  ym <- toks[length(toks)-1]
  num <- suppressWarnings(as.integer(toks[length(toks)]))
  venue_tokens <- toks[1:(length(toks)-2)]
  venue <- gsub("_", " ", paste(venue_tokens, collapse = "_"))
  venue <- stringr::str_squish(venue)

  # parse year + Spanish month in "YYYYMes"
  m <- stringr::str_match(ym, "^(\\d{4})([A-Za-zÁÉÍÓÚáéíóúñÑ]+)$")
  year <- suppressWarnings(as.integer(m[,2]))
  mes  <- tolower(stringi::stri_trans_general(m[,3], "Latin-ASCII"))
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

# --- Load venues metadata (optional) ------------------------------------------
venues_df <- tryCatch(readr::read_csv(venues_path, show_col_types = FALSE), error = function(e) tibble())
# ensure expected columns exist (avoid joins failing later)
for (nm in c("venue","municipality","state","latitude","longitude","lat","lon")) {
  if (!nm %in% names(venues_df)) venues_df[[nm]] <- NA
}

# --- OCR function shim ---------------------------------------------------------
get_ocr_text <- function(img){
  # Try functions that may be defined in extract_tesseract.R
  for (fn in c("tesseract_to_text","tesseract_ocr_text","ocr_to_text","ocr_image_text")){
    if (exists(fn, mode = "function")) {
      return(tryCatch(get(fn)(img), error=function(e) ""))
    }
  }
  # Fallback: empty (we still might have manual overrides)
  ""
}

# --- Extract per image (text-only + optional manual added later in validate) ---
message("Running Tesseract OCR…")
ocr_texts <- meta %>%
  mutate(ocr = purrr::map_chr(source_image, get_ocr_text))

message("Running OpenAI text-only pass on OCR…")
extract_text <- function(ocr, v, y, m, src){
  ev <- tibble()
  if (exists("extract_openai_events_from_text", mode="function")) {
    ev <- tryCatch(extract_openai_events_from_text(ocr, v, y, m), error=function(e) tibble())
  } else if (exists("extract_openai_text_events", mode="function")) {
    ev <- tryCatch(extract_openai_text_events(ocr, v, y, m), error=function(e) tibble())
  }
  if (nrow(ev)) ev <- ev %>% mutate(source_image = src)
  ev
}

events_list <- purrr::pmap(meta, function(source_image, state, venue, year, month, file_num){
  o <- ocr_texts$ocr[ocr_texts$source_image == source_image][1]
  extract_text(o, venue, year, month, source_image)
})

events_raw <- bind_rows(events_list)

# Log per-image counts safely (handle missing column)
clean <- events_raw
if (!"source_image" %in% names(clean)) clean$source_image <- NA_character_
per_img <- clean %>% filter(!is.na(source_image)) %>% count(source_image, name = "events")
cat("Per-image extracted events:\n")
print(per_img)

# --- Validate & join venue metadata (also reads per-venue manual overrides) ----
if (!exists("validate_events", mode="function")) {
  stop("validate.R not loaded or validate_events() not found.")
}
final <- validate_events(events_raw, venues_df = venues_df)

# --- Write outputs -------------------------------------------------------------
outfile <- file.path(out_dir, "performances_monthly.csv")
readr::write_csv(final, outfile)
cat("Wrote:", outfile, "\n")

# --- Simple aggregations (safe if muni/state missing) --------------------------
safe_col <- function(df, nm, type="chr"){
  if (!nm %in% names(df)) {
    df[[nm]] <- if (type == "num") NA_real_ else NA_character_
  }
  df
}
agg_in <- final %>%
  safe_col("municipality") %>%
  safe_col("state") %>%
  mutate(year = year(event_date), month = month(event_date))

agg_muni <- agg_in %>%
  count(municipality, year, month, name = "events") %>%
  arrange(municipality, year, month)

agg_state <- agg_in %>%
  count(state, year, month, name = "events") %>%
  arrange(state, year, month)

readr::write_csv(agg_muni, file.path(agg_dir, "events_by_municipality.csv"))
readr::write_csv(agg_state, file.path(agg_dir, "events_by_state.csv"))
cat("Wrote aggregations to:", agg_dir, "\n")

cat("Done.\n")
