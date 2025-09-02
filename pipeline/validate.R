# pipeline/validate.R
# v2025-09-02b minimal validator + per-venue manual overrides + safer venues join
# - Fixes: recognizes more venues.csv schemas (venue_name, nombre, municipio/estado, lat/lon variants)

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(stringr)
  library(tidyr)
  library(lubridate)
  library(purrr)
  library(cli)
})

`%||%` <- function(a, b) if (!is.null(a)) a else b

# ---- helpers ----

.norm_venue <- function(x) {
  if (is.null(x)) return(NA_character_)
  x %>%
    str_replace_all("[_]+", " ") %>%
    str_replace_all("\\s+", " ") %>%
    str_trim()
}

.get_state_from_path <- function(path, images_dir) {
  im <- normalizePath(images_dir, mustWork = FALSE)
  p  <- normalizePath(path, mustWork = FALSE)
  rel <- gsub(paste0("^", gsub("([\\^\\$\\.\\|\\(\\)\\[\\]\\*\\+\\?\\/])", "\\\\\\1", im), "[/\\\\]?"), "", p)
  strsplit(rel, "[/\\\\]")[[1]][1] %||% NA_character_
}

.weekday_es <- function(d) {
  if (is.na(d)) return(NA_character_)
  c("lunes","martes","miércoles","jueves","viernes","sábado","domingo")[wday(d, week_start = 1)]
}

.read_csv_safe <- function(path, col_types = cols(.default = col_guess())) {
  if (is.null(path) || !file.exists(path)) return(NULL)
  suppressWarnings(readr::read_csv(path, col_types = col_types, show_col_types = FALSE))
}

.canonicalize_event_cols <- function(df) {
  if (is.null(df) || nrow(df) == 0) {
    return(tibble(event_date = as.Date(character()),
                  band_name = character(),
                  event_time = character(),
                  venue = character(),
                  source_image = character()))
  }
  nm <- names(df)
  out <- df

  # Date
  date_col <- intersect(nm, c("event_date","date","fecha"))
  if (length(date_col) == 0) {
    if (all(c("year","month","day") %in% nm)) {
      out <- out %>% mutate(event_date = suppressWarnings(lubridate::make_date(year, month, day)))
    } else {
      out$event_date <- as.Date(NA)
    }
  } else {
    out <- out %>% rename(event_date = !!date_col[1])
    if (!inherits(out$event_date, "Date")) {
      out$event_date <- suppressWarnings(lubridate::as_date(out$event_date))
    }
  }

  # Band
  band_col <- intersect(nm, c("band_name","band","artista","artiste"))
  if (length(band_col) == 0) out$band_name <- NA_character_ else out <- out %>% rename(band_name = !!band_col[1])

  # Time
  time_col <- intersect(nm, c("event_time","time","hora","horario"))
  if (length(time_col) == 0) out$event_time <- NA_character_ else out <- out %>% rename(event_time = !!time_col[1])

  # Venue
  venue_col <- intersect(nm, c("venue","venue_name","nombre","lugar","sala","espacio"))
  if (length(venue_col) == 0) {
    if (!"venue" %in% names(out)) out$venue <- NA_character_
  } else {
    out <- out %>% rename(venue = !!venue_col[1])
  }

  if (!"source_image" %in% names(out)) out$source_image <- NA_character_

  out %>%
    mutate(
      venue      = .norm_venue(venue),
      band_name  = ifelse(is.na(band_name), NA_character_, str_squish(band_name)),
      event_time = ifelse(is.na(event_time), NA_character_, str_squish(event_time))
    ) %>%
    select(any_of(c("source_image")), venue, event_date, band_name, event_time)
}

# Robust join to venues.csv with flexible schemas
.join_venues_metadata <- function(df, venues_df) {
  # Prepare output columns regardless
  df$municipality <- NA_character_
  df$state <- NA_character_
  df$latitude <- NA_real_
  df$longitude <- NA_real_
  df$venue_id <- NA_character_

  if (is.null(venues_df) || nrow(venues_df) == 0) return(df)

  v <- venues_df %>% rename_with(tolower)

  # Candidate columns for venue name
  venue_name_col <- intersect(names(v), c("venue","venue_name","name","nombre","lugar","sala","espacio"))
  if (length(venue_name_col) == 0) {
    cli::cli_alert_warning("venues.csv has no recognizable venue name column (tried: venue,venue_name,name,nombre,lugar,sala,espacio). Skipping venue join.")
    return(df)
  }

  # Candidate columns for municipality/state and lat/lon
  muni_col <- intersect(names(v), c("municipality","municipio"))
  state_col <- intersect(names(v), c("state","estado"))
  lat_col <- intersect(names(v), c("latitude","lat"))
  lon_col <- intersect(names(v), c("longitude","lon","lng","long"))

  venue_id_col <- intersect(names(v), c("venue_id","id","codigo","clave"))

  v <- v %>%
    mutate(
      venue_norm = .norm_venue(.data[[venue_name_col[1]]]),
      municipality = if (length(muni_col)) .data[[muni_col[1]]] else NA_character_,
      state        = if (length(state_col)) .data[[state_col[1]]] else NA_character_,
      latitude     = suppressWarnings(as.numeric(if (length(lat_col)) .data[[lat_col[1]]] else NA_real_)),
      longitude    = suppressWarnings(as.numeric(if (length(lon_col)) .data[[lon_col[1]]] else NA_real_)),
      venue_id     = if (length(venue_id_col)) as.character(.data[[venue_id_col[1]]]) else NA_character_
    ) %>%
    select(venue_norm, municipality, state, latitude, longitude, venue_id) %>%
    distinct()

  df %>%
    mutate(venue_norm = .norm_venue(venue)) %>%
    left_join(v, by = "venue_norm") %>%
    mutate(
      municipality = coalesce(municipality.x, municipality.y),
      state        = coalesce(state.x, state.y),
      latitude     = coalesce(latitude.x, latitude.y),
      longitude    = coalesce(longitude.x, longitude.y),
      venue_id     = coalesce(venue_id.x, venue_id.y)
    ) %>%
    select(-ends_with(".x"), -ends_with(".y"))
}

.merge_with_manual <- function(auto_df, manual_df) {
  if (is.null(manual_df) || nrow(manual_df) == 0) return(auto_df)
  a <- .canonicalize_event_cols(auto_df)
  m <- .canonicalize_event_cols(manual_df)
  key_cols <- c("venue","event_date")
  a <- a %>% mutate(.origin = "auto")
  m <- m %>% mutate(.origin = "manual")
  bind_rows(a, m) %>%
    arrange(desc(.origin)) %>%
    group_by(across(all_of(key_cols))) %>%
    slice(1) %>%
    ungroup() %>%
    select(-.origin)
}

.apply_name_patches <- function(df, patches_path = "data/name_patches.csv") {
  if (!file.exists(patches_path)) return(df)
  patches <- suppressWarnings(readr::read_csv(patches_path, show_col_types = FALSE))
  req <- c("from","to")
  if (!all(req %in% names(patches))) return(df)
  out <- df
  for (i in seq_len(nrow(patches))) {
    from <- patches$from[i]; to <- patches$to[i]
    out$band_name <- ifelse(!is.na(out$band_name) & out$band_name == from, to, out$band_name)
    out$venue     <- ifelse(!is.na(out$venue) & out$venue == from, to, out$venue)
  }
  out
}

# ---- main exported function ----
validate <- function(extracted, meta, venues_path = "data/venues.csv", images_dir = "../carteleras") {
  cli::cli_h1("[validate] minimal + per-venue manual overrides + safe-venue-cols")

  if (missing(extracted) || is.null(extracted)) {
    extracted <- tibble(source_image = character(),
                        event_date = as.Date(character()),
                        band_name = character(),
                        event_time = character(),
                        venue = character())
  }
  if (missing(meta) || is.null(meta)) {
    meta <- tibble(source_image = character(),
                   venue_guess = character(),
                   year = integer(),
                   month_name_es = character(),
                   file_num = integer())
  }

  # Warn if auto-extracted is empty
  auto_rows <- extracted %>% mutate(has = !is.na(event_date) & nzchar(coalesce(band_name, "")))
  if (sum(auto_rows$has, na.rm = TRUE) == 0) {
    cli::cli_alert_warning("No auto-extracted rows detected (only manual overrides likely). Check OCR/LLM pipeline and OCR_* env vars.")
  }

  # Canonicalize and backfill venue from meta
  ext <- .canonicalize_event_cols(extracted) %>%
    left_join(meta %>% select(source_image, venue_guess), by = "source_image") %>%
    mutate(
      venue = .norm_venue(coalesce(venue, venue_guess))
    ) %>%
    select(-venue_guess)

  # Determine state per row for manual overrides path
  state_col <- mapply(.get_state_from_path, path = ext$source_image, MoreArgs = list(images_dir = images_dir), USE.NAMES = FALSE)
  ext$state_from_path <- ifelse(is.na(state_col), NA_character_, state_col)

  # Load venue metadata
  venues_df <- tryCatch(.read_csv_safe(venues_path), error = function(e) NULL)

  # Load manual overrides
  manual_all <- list()
  if (nrow(ext) > 0) {
    combos <- ext %>% distinct(state_from_path, venue) %>% filter(!is.na(venue) & nzchar(venue))
    for (i in seq_len(nrow(combos))) {
      st  <- combos$state_from_path[i]
      ven <- combos$venue[i]
      safe_venue <- paste0(ven, ".csv")
      candidate_paths <- c()
      if (!is.na(st) && nzchar(st)) {
        candidate_paths <- c(candidate_paths, file.path("data", "manual_events", st, safe_venue))
      }
      if (length(candidate_paths) == 0 || !file.exists(candidate_paths[1])) {
        glob <- Sys.glob(file.path("data", "manual_events", "*", safe_venue))
        candidate_paths <- c(candidate_paths, glob)
      }
      cand <- NULL
      if (length(candidate_paths) > 0) {
        for (p in candidate_paths) {
          if (file.exists(p)) { cand <- p; break }
        }
      }
      if (!is.null(cand)) {
        dfm <- tryCatch(.read_csv_safe(cand), error = function(e) NULL)
        if (!is.null(dfm) && nrow(dfm) > 0) {
          if (!"venue" %in% names(dfm)) dfm$venue <- ven
          manual_all[[length(manual_all) + 1]] <- dfm
          cli::cli_alert_info("Loaded manual overrides: {cand} ({nrow(dfm)} rows)")
        }
      }
    }
  }
  manual_df <- if (length(manual_all)) bind_rows(manual_all) else tibble()

  # Merge auto + manual (manual wins)
  merged <- .merge_with_manual(ext, manual_df)

  # Apply optional name patches
  merged <- .apply_name_patches(merged, patches_path = "data/name_patches.csv")

  # Add weekday (Spanish)
  merged <- merged %>%
    mutate(
      weekday = ifelse(!is.na(event_date),
                       vapply(event_date, weekday_es, FUN.VALUE = character(1)),
                       NA_character_)
    )

  # Join venue metadata safely
  merged <- .join_venues_metadata(merged, venues_df)

  # Final schema
  final_cols <- c("venue","event_date","weekday","band_name","event_time",
                  "municipality","state","latitude","longitude","venue_id")
  for (cc in final_cols) if (!cc %in% names(merged)) merged[[cc]] <- NA

  performances <- merged %>%
    select(all_of(final_cols)) %>%
    arrange(event_date, venue, band_name)

  # Aggregations
  agg_municipality <- performances %>%
    filter(!is.na(event_date)) %>%
    count(state, municipality, name = "events", sort = TRUE)

  agg_state <- performances %>%
    filter(!is.na(event_date)) %>%
    count(state, name = "events", sort = TRUE)

  list(
    performances = performances,
    agg_municipality = agg_municipality,
    agg_state = agg_state
  )
}
