#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(tidyverse); library(lubridate); library(stringr); library(readr); library(glue); library(digest)
})

cat(">> run_gvision v1+cache (GCV OCR -> text parser -> *_gvision.csv; cache successful posters)\n")

safe_source <- function(path){
  if (file.exists(path)) {
    tryCatch({ source(path, chdir = TRUE); TRUE },
             error = function(e){ message(sprintf("!! could not source %s: %s", path, e$message)); FALSE })
  } else FALSE
}

have_validate <- safe_source("pipeline/validate.R")
have_gcv      <- safe_source("pipeline/extract_gvision_text.R")
have_text     <- safe_source("pipeline/extract_openai_text.R")
stopifnot(have_validate, have_gcv, have_text)

args <- commandArgs(trailingOnly = TRUE)
get_arg <- function(flag, default=NULL){ hit <- which(args == flag); if (length(hit) && hit < length(args)) args[hit+1] else default }

images_dir <- get_arg("--images_dir", "carteleras")
venues_path <- get_arg("--venues_path", "data/venues.csv")
out_dir   <- get_arg("--out_dir",   "data")
agg_dir_g <- file.path(out_dir, "aggregations_gvision")
cache_path <- file.path(out_dir, "processed_images.csv")  # unified cache
force_reprocess <- identical(tolower(Sys.getenv("FORCE_REPROCESS_GCV", "0")), "1")

dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(agg_dir_g, showWarnings = FALSE, recursive = TRUE)

month_map <- c("enero"=1,"febrero"=2,"marzo"=3,"abril"=4,"mayo"=5,"junio"=6,"julio"=7,"agosto"=8,"septiembre"=9,"setiembre"=9,"octubre"=10,"noviembre"=11,"diciembre"=12)
is_img <- function(p) grepl("\\.(jpg|jpeg|png)$", tolower(p))
file_sha256 <- function(p) tryCatch(digest::digest(file = p, algo = "sha256"), error=function(e) NA_character_)

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
meta <- meta %>% mutate(sha256 = vapply(source_image, file_sha256, character(1)))
cat("GCV path - parsed metadata rows:", nrow(meta), "\n")
if (!nrow(meta)) stop("No parsable filenames (check naming convention).")

venues_df <- tryCatch(readr::read_csv(venues_path, show_col_types = FALSE), error=function(e) tibble())
for (nm in c("venue","municipality","state","latitude","longitude","lat","lon")) if (!nm %in% names(venues_df)) venues_df[[nm]] <- NA

# Load cache
cache <- if (file.exists(cache_path)) readr::read_csv(cache_path, show_col_types = FALSE) else tibble()
if (!nrow(cache)) cache <- tibble(extractor=character(), sha256=character(), source_image=character(),
                                  venue=character(), year=integer(), month=integer(),
                                  events=integer(), processed_at=as.POSIXct(character()))
gcv_done <- cache %>% filter(extractor == "gcv_text", !is.na(sha256), events > 0) %>% pull(sha256)
todo <- if (force_reprocess) meta else meta %>% filter(!(sha256 %in% gcv_done))
cat(sprintf("Cache: %d previously successful GCV images; will process %d new image(s).\n",
            length(gcv_done), nrow(todo)))

message("Running Google Vision OCR + text parser…")
if (!nrow(todo)) {
  events_raw <- tibble()
} else {
  # OCR + parse only for TODO
  ocr_tbl <- todo %>% mutate(ocr = purrr::map_chr(source_image, ~tryCatch(gcv_ocr_text(.x), error=function(e) "")))
  events_list <- purrr::pmap(todo, function(source_image, state, venue, year, month, file_num, sha256){
    o <- ocr_tbl$ocr[ocr_tbl$source_image == source_image][1]
    tryCatch(extract_openai_events_from_text(o, venue, year, month), error=function(e) tibble())
  })
  events_raw <- bind_rows(events_list)
  if (nrow(events_raw)) events_raw$source_image <- events_raw$source_image %||% todo$source_image
}

per_img <- events_raw %>%
  mutate(source_image = if (!"source_image" %in% names(.)) NA_character_ else source_image) %>%
  filter(!is.na(source_image)) %>% count(source_image, name="events")
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

# Update cache (successful only)
if (nrow(per_img)) {
  per_img2 <- per_img %>%
    left_join(meta %>% select(source_image, venue, year, month, sha256), by = "source_image") %>%
    transmute(extractor = "gcv_text", sha256, source_image, venue, year, month,
              events = events, processed_at = Sys.time()) %>%
    filter(!is.na(sha256), events > 0) %>%
    distinct(extractor, sha256, .keep_all = TRUE)

  cache_keep <- cache %>% anti_join(per_img2 %>% select(extractor, sha256), by = c("extractor","sha256"))
  cache_new  <- bind_rows(cache_keep, per_img2)
  readr::write_csv(cache_new, cache_path)
  cat("Updated cache:", cache_path, "\n")
} else {
  cat("No successful images to add to cache (GCV).\n")
}

cat("Done.\n")
