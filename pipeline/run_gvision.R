#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(tidyverse); library(lubridate); library(stringr); library(readr); library(glue)
})

cat(">> run_gvision v1 (GCV OCR -> text parser -> *_gvision.csv)\n")

safe_source <- function(path){
  if (file.exists(path)) {
    tryCatch({ source(path, chdir = TRUE); TRUE },
             error = function(e){ message(sprintf("!! could not source %s: %s", path, e$message)); FALSE })
  } else FALSE
}

# Load only what we need for GCV path
have_validate <- safe_source("pipeline/validate.R")
have_gcv      <- safe_source("pipeline/extract_gvision_text.R")
have_text     <- safe_source("pipeline/extract_openai_text.R")
if (!have_validate) stop("pipeline/validate.R missing")
if (!have_gcv)      stop("pipeline/extract_gvision_text.R missing")
if (!have_text)     stop("pipeline/extract_openai_text.R missing")

args <- commandArgs(trailingOnly = TRUE)
get_arg <- function(flag, default=NULL){ hit <- which(args == flag); if (length(hit) && hit < length(args)) args[hit+1] else default }

images_dir <- get_arg("--images_dir", "carteleras")
venues_path <- get_arg("--venues_path", "data/venues.csv")
out_dir   <- get_arg("--out_dir",   "data")
agg_dir_g <- file.path(out_dir, "aggregations_gvision")

dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(agg_dir_g, showWarnings = FALSE, recursive = TRUE)

month_map <- c("enero"=1,"febrero"=2,"marzo"=3,"abril"=4,"mayo"=5,"junio"=6,"julio"=7,"agosto"=8,"septiembre"=9,"setiembre"=9,"octubre"=10,"noviembre"=11,"diciembre"=12)

is_img <- function(p) grepl("\\.(jpg|jpeg|png)$", tolower(p))
all_files <- list.files(images_dir, recursive = TRUE, full.names = TRUE)
poster_files <- all_files[file.info(all_files)$isdir %in% c(FALSE) & vapply(all_files, is_img, TRUE)]

if (!length(poster_files)) {
  message("No poster files under: ", images_dir)
  readr::write_csv(tibble(), file.path(out_dir, "performances_monthly_gvision.csv"))
  readr::write_csv(tibble(), file.path(agg_dir_g, "events_by_municipality.csv"))
  readr::write_csv(tibble(), file.path(agg_dir_g, "events_by_state.csv"))
  quit(status = 0)
}

parse_one <- function(p){
  rel <- sub(paste0("^", normalizePath(images_dir), .Platform$file.sep), "", normalizePath(p))
  parts <- strsplit(rel, .Platform$file.sep, fixed = TRUE)[[1]]
  state <- if (length(parts) > 1) parts[1] else NA_character_
  file <- tools::file_path_sans_ext(basename(p))
  toks <- unlist(strsplit(file, "_")); if (length(toks) < 3) return(NULL)
  ym <- toks[length(toks)-1]; num <- suppressWarnings(as.integer(toks[length(toks)]))
  venue_tokens <- toks[1:(length(toks)-2)]
  venue <- gsub("_"," ", paste(venue_tokens, collapse = "_")) |> stringr::str_squish()
  m <- stringr::str_match(ym, "^(\\d{4})([A-Za-zÁÉÍÓÚáéíóúñÑ]+)$")
  year <- suppressWarnings(as.integer(m[,2])); mes <- tolower(iconv(m[,3], to="ASCII//TRANSLIT"))
  month <- month_map[mes] %||% NA_integer_
  tibble(source_image=p, state=state, venue=venue, year=year, month=as.integer(month), file_num=num %||% NA_integer_)
}

meta <- purrr::map_dfr(poster_files, function(p) tryCatch(parse_one(p), error=function(e) NULL))
meta <- meta %>% filter(!is.na(year), !is.na(month), nzchar(venue))
cat("GCV path - parsed metadata rows:", nrow(meta), "\n")
if (!nrow(meta)) stop("No parsable filenames (check naming convention).")

venues_df <- tryCatch(readr::read_csv(venues_path, show_col_types = FALSE), error=function(e) tibble())
for (nm in c("venue","municipality","state","latitude","longitude","lat","lon")) if (!nm %in% names(venues_df)) venues_df[[nm]] <- NA

message("Running Google Vision OCR + text parser…")
gcv_texts <- meta %>% mutate(ocr = purrr::map_chr(source_image, ~tryCatch(gcv_ocr_text(.x), error=function(e) "")))

events_list <- purrr::pmap(meta, function(source_image, state, venue, year, month, file_num){
  o <- gcv_texts$ocr[gcv_texts$source_image == source_image][1]
  tryCatch(extract_openai_events_from_text(o, venue, year, month), error=function(e) tibble())
})
events_raw <- bind_rows(events_list) %>% mutate(source_image = source_image %||% NA_character_)

per_img <- events_raw %>% filter(!is.na(source_image)) %>% count(source_image, name="events")
cat("Per-image extracted events (GCV OCR path):\n"); print(per_img)

final <- validate_events(events_raw, venues_df = venues_df)

outfile <- file.path(out_dir, "performances_monthly_gvision.csv")
readr::write_csv(final, outfile)
cat("Wrote:", outfile, "\n")

if (nrow(final)) {
  agg_in <- final %>% mutate(year = year(event_date), month = month(event_date))
  readr::write_csv(agg_in %>% count(municipality, year, month, name="events") %>% arrange(municipality, year, month),
                   file.path(agg_dir_g, "events_by_municipality.csv"))
  readr::write_csv(agg_in %>% count(state, year, month, name="events") %>% arrange(state, year, month),
                   file.path(agg_dir_g, "events_by_state.csv"))
} else {
  readr::write_csv(tibble(), file.path(agg_dir_g, "events_by_municipality.csv"))
  readr::write_csv(tibble(), file.path(agg_dir_g, "events_by_state.csv"))
}

cat("Done.\n")
