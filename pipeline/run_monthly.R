#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(jsonlite)
  library(stringr)
})

VERSION <- "run_monthly v1.4 (auto-state-from-path + coords pass-through + residencias/time)"
message(">> ", VERSION)

# --------- args ----------
args <- commandArgs(trailingOnly = TRUE)
get_arg <- function(flag, default = NULL){ i <- which(args == flag); if (length(i) == 0 || i == length(args)) return(default); args[i + 1] }
images_dir    <- get_arg("--images_dir",  "carteleras")
venues_path   <- get_arg("--venues_path", "data/venues.csv")
out_dir       <- get_arg("--out_dir",     "data")
agg_dir       <- get_arg("--agg_dir",     file.path(out_dir, "aggregations"))
manifest_path <- get_arg("--manifest_path", "posters_manifest.csv")

# --------- module loading (kept) ----------
safe_source <- function(p){ if (file.exists(p)) source(p) else message("Missing ", p, " (skipping)") }
safe_source("pipeline/extract_openai.R")
safe_source("pipeline/extract_openai_text.R")
safe_source("pipeline/extract_gvision.R")       # optional (future)
safe_source("pipeline/extract_tesseract.R")
safe_source("pipeline/parse_posters.R")
safe_source("pipeline/validate.R")
safe_source("pipeline/aggregate.R")
safe_source("pipeline/geocode_venues.R")

                           

# --------- helpers ----------
month_map <- c(
  "enero"=1,"ene"=1,"febrero"=2,"feb"=2,"marzo"=3,"mar"=3,"abril"=4,"abr"=4,"mayo"=5,"may"=5,
  "junio"=6,"jun"=6,"julio"=7,"jul"=7,"agosto"=8,"ago"=8,"septiembre"=9,"sep"=9,"setiembre"=9,"set"=9,
  "octubre"=10,"oct"=10,"noviembre"=11,"nov"=11,"diciembre"=12,"dic"=12
)
norm_name <- function(x){ x |> tolower() |> gsub("_"," ",x=_) |> gsub("\\s+"," ",x=_) |> trimws() }
get_state_from_path <- function(path){
  parts <- strsplit(path,"/")[[1]]
  i <- which(parts=="carteleras")
  if (length(i) && length(parts)>=i+1) return(parts[i+1])
  NA_character_
}

parse_from_filename <- function(path){
  fn <- basename(path)
  stem <- tools::file_path_sans_ext(fn)
  stem_lc <- tolower(stem)

  # Venue_YYYYMes_N.jpg
  m <- str_match(stem_lc, "^(.*)_((20)\\d{2})([a-záéíóúñ]{3,10})_(\\d+)$")
  if (!all(is.na(m))) {
    venue_raw <- m[,2]
    yr   <- as.integer(m[,3])
    mo_t <- gsub("[^a-záéíóúñ]","", m[,5])
    mo   <- suppressWarnings(as.integer(month_map[mo_t]))
    pg   <- as.integer(m[,6])
    return(tibble(
      image_path=path,
      state=get_state_from_path(path),
      venue_name=gsub("_"," ",venue_raw),
      venue_norm=norm_name(venue_raw),
      year=yr, month=mo, page=pg
    ))
  }
  # YYYYMes_Venue_N.jpg
  m <- str_match(stem_lc, "^((20)\\d{2})([a-záéíóúñ]{3,10})_(.*)_(\\d+)$")
  if (!all(is.na(m))) {
    yr   <- as.integer(m[,2])
    mo_t <- gsub("[^a-záéíóúñ]","", m[,4])
    mo   <- suppressWarnings(as.integer(month_map[mo_t]))
    venue_raw <- m[,5]
    pg   <- as.integer(m[,6])
    return(tibble(
      image_path=path,
      state=get_state_from_path(path),
      venue_name=gsub("_"," ",venue_raw),
      venue_norm=norm_name(venue_raw),
      year=yr, month=mo, page=pg
    ))
  }
  tibble(image_path=path, state=get_state_from_path(path),
         venue_name=NA_character_, venue_norm=NA_character_,
         year=NA_integer_, month=NA_integer_, page=NA_integer_)
}

# --------- discover inputs ----------
imgs <- list.files(images_dir, pattern="\\.(png|jpg|jpeg)$", recursive=TRUE, full.names=TRUE)
message("Discovered ", length(imgs), " poster file(s) under: ", images_dir)
if (length(imgs)) message("Example: ", paste(utils::head(imgs,3), collapse=" | "))
if (!length(imgs)) { message("No images found under ", images_dir); quit(save="no", status=0) }

if (file.exists(manifest_path)){
  message("Using manifest at ", manifest_path)
  meta <- readr::read_csv(manifest_path, show_col_types=FALSE) |>
    mutate(
      image_path=image_path,
      venue_norm=norm_name(venue_name),
      state=ifelse(is.na(state), get_state_from_path(image_path), state),
      page=suppressWarnings(as.integer(page))
    )
} else {
  meta <- purrr::map_dfr(imgs, parse_from_filename)
}

bad <- meta |> filter(is.na(venue_name) | is.na(year) | is.na(month))
if (nrow(bad)) message("Skipping ", nrow(bad), " files with unrecognized names. Example: ", bad$image_path[1])
meta <- meta |> filter(!is.na(venue_name), !is.na(year), !is.na(month)) |> rename(venue = venue_name)
message("Parsed metadata rows (usable): ", nrow(meta))
if (!nrow(meta)) { message("No usable filenames parsed. Exiting."); quit(save="no", status=0) }

# --- WRITE venues_autofill.csv (infer state from folder path)
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
ven_auto <- meta %>%
  mutate(venue_key = tolower(venue) %>% gsub("_"," ",.) %>% trimws()) %>%
  filter(!is.na(state) & nzchar(state)) %>%
  select(venue, venue_key, state) %>%
  distinct()
readr::write_csv(ven_auto, file.path(out_dir, "venues_autofill.csv"))
                           
# Auto-geocode missing coordinates (bounded by GEOCODE_MAX)
if (exists("geocode_missing_venues")) {
  try(geocode_missing_venues(venues_path), silent = TRUE)
}

# --------- run extractors ----------
use_openai <- nzchar(Sys.getenv("OPENAI_API_KEY"))
use_gcv    <- nzchar(Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

openai_events <- tibble(); ocr_raw <- tibble(); text_events <- tibble()

if (use_openai && exists("extract_openai_events")) {
  message("Running OpenAI image extraction…")
  openai_events <- purrr::map_dfr(seq_len(nrow(meta)), function(i){
    row <- meta[i,]
    tryCatch(extract_openai_events(row$image_path, row$venue, row$year, row$month),
             error=function(e){ message("OpenAI failed for ", row$image_path, ": ", e$message); tibble() })
  })
} else message("Skipping OpenAI image extraction (no key or extractor missing).")

if (exists("extract_tesseract_text")) {
  message("Running Tesseract OCR…")
  ocr_raw <- purrr::map_dfr(seq_len(nrow(meta)), function(i){
    row <- meta[i,]
    tryCatch(extract_tesseract_text(row$image_path, NA_character_, row$venue, row$year, row$month),
             error=function(e){ message("Tesseract failed for ", row$image_path, ": ", e$message); tibble() })
  })
}

vision_events <- if (nrow(ocr_raw) && exists("parse_poster_events")) parse_poster_events(ocr_raw) else tibble()

if (use_openai && exists("extract_openai_events_from_text") && nrow(ocr_raw)){
  message("Running OpenAI text-only pass on OCR…")
  text_events <- purrr::map_dfr(seq_len(nrow(ocr_raw)), function(i){
    rr <- ocr_raw[i,]
    tryCatch(extract_openai_events_from_text(rr$ocr_text, rr$venue, rr$year, rr$month),
             error=function(e){ tibble() })
  })
}

for (nm in c("event_time","event_title")) {
  if (!nm %in% names(openai_events)) openai_events[[nm]] <- NA_character_
  if (!nm %in% names(vision_events)) vision_events[[nm]] <- NA_character_
  if (!nm %in% names(text_events))   text_events[[nm]]   <- NA_character_
}

all_events <- bind_rows(
  openai_events %>% mutate(source="openai"),
  vision_events %>% mutate(source="tesseract"),
  text_events   %>% mutate(source="openai-text")
) %>% distinct()

clean <- if (exists("validate_events")) validate_events(all_events) else all_events

# --- per-image counts for debugging
if (nrow(clean)) {
  per_img <- clean %>% count(source_image, name="events")
  message("Per-image extracted events:")
  utils::capture.output(per_img) |> paste(collapse="\n") |> message()
}

# --- weekday in Spanish
if (nrow(clean)) {
  clean <- clean %>% mutate(
    weekday = c("lunes","martes","miércoles","jueves","viernes","sábado","domingo")[lubridate::wday(event_date, week_start=1)]
  )
}

# --- join venue metadata (municipality/state + optional coordinates)
venues <- tryCatch(readr::read_csv(venues_path, show_col_types = FALSE), error=function(e) tibble())
if (nrow(venues)) {
  # normalize name column to 'venue'
  name_col <- intersect(c("venue","venue_name","name"), names(venues))
  if (length(name_col) && name_col[1] != "venue") venues <- dplyr::rename(venues, venue = dplyr::all_of(name_col[1]))
  venues <- venues %>% mutate(venue_key = tolower(venue) %>% gsub("_"," ",.) %>% trimws())
  keep <- intersect(c("venue_key","municipality","state","lat","lon","latitude","longitude","address"), names(venues))
  venues_j <- venues %>% select(all_of(keep))
} else {
  venues_j <- tibble(venue_key=character())
}

if (!nrow(clean)) {
  message("No events extracted; writing empty CSV with headers.")
  clean <- tibble(
    venue=character(), venue_id=character(), event_date=as.Date(character()),
    band_name=character(), event_title=character(), event_time=character(),
    source=character(), source_image=character(), weekday=character(),
    municipality=character(), state=character(),
    latitude=double(), longitude=double()
  )
} else {
  clean <- clean %>%
    mutate(venue_key = tolower(venue) %>% gsub("_"," ",.) %>% trimws()) %>%
    left_join(venues_j, by="venue_key") %>%
    mutate(
      # Unify coordinate field names if present
      latitude  = suppressWarnings(as.numeric(dplyr::coalesce(.data$latitude, .data$lat))),
      longitude = suppressWarnings(as.numeric(dplyr::coalesce(.data$longitude, .data$lon))),
      # Fallback state from folder path if still missing
      state     = dplyr::coalesce(.data$state, sapply(source_image, get_state_from_path))
    ) %>%
    select(-venue_key, -any_of(c("lat","lon"))) %>%
    relocate(weekday, .after = event_date) %>%
    relocate(municipality, state, .after = event_time) %>%
    relocate(latitude, longitude, .after = state)
}

# --- write main CSV
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(agg_dir, showWarnings = FALSE, recursive = TRUE)
readr::write_csv(clean, file.path(out_dir, "performances_monthly.csv"))

# --- aggregations
if (exists("aggregate_all")) {
  aggregate_all(
    events_path = file.path(out_dir, "performances_monthly.csv"),
    venues_path = venues_path,
    out_dir     = agg_dir
  )
} else message("aggregate_all() not found; skipping aggregation.")

message("Done.")
