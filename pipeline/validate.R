suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(stringr)
})

# If venue_rules.R was sourced in run_monthly.R, we can use it here
if (!exists("VENUE_RULES")) VENUE_RULES <- list()

norm_venue_key <- function(x){
  x <- tolower(as.character(x))
  x <- gsub("_", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

# --- Vectorized time normalizer
normalize_time_vec <- function(tt) {
  if (is.null(tt)) return(character(0))
  tt <- as.character(tt); if (!length(tt)) return(tt)
  tt <- tolower(tt)
  m <- stringr::str_match(tt, "(\\d{1,2})(?::(\\d{2}))?\\s*(am|pm|h|hrs?)?")
  hh <- suppressWarnings(as.integer(m[, 2]))
  mm <- suppressWarnings(as.integer(ifelse(is.na(m[, 3]), 0L, m[, 3])))
  suf <- m[, 4]
  pm <- !is.na(suf) & suf == "pm" & !is.na(hh)
  am <- !is.na(suf) & suf == "am" & !is.na(hh)
  hh[pm & hh < 12] <- hh[pm & hh < 12] + 12
  hh[am & hh == 12] <- 0
  ok <- !is.na(hh)
  out <- rep(NA_character_, length(tt))
  out[ok] <- sprintf("%02d:%02d", hh[ok] %% 24, mm[ok] %% 60)
  out
}

# Optional global regex/name patches (data/name_patches.csv)
apply_name_patches <- function(df){
  pth <- file.path("data","name_patches.csv")
  if (!file.exists(pth)) return(df)
  patches <- tryCatch(readr::read_csv(pth, show_col_types = FALSE), error=function(e) tibble())
  if (!nrow(patches)) return(df)

  # supports: from, to [, venue, year, month]  and/or from_rx (regex)
  norm <- function(s) tolower(gsub("\\s+"," ", trimws(as.character(s))))
  df$band_name <- as.character(df$band_name)
  for (i in seq_len(nrow(patches))){
    to <- as.character(patches$to[i])
    venue <- if ("venue" %in% names(patches)) as.character(patches$venue[i]) else NA_character_
    year  <- if ("year"  %in% names(patches)) suppressWarnings(as.integer(patches$year[i]))  else NA_integer_
    month <- if ("month" %in% names(patches)) suppressWarnings(as.integer(patches$month[i])) else NA_integer_

    cond <- rep(TRUE, nrow(df))
    if (!is.na(venue)) cond <- cond & (tolower(df$venue) == tolower(venue))
    if (!is.na(year))  cond <- cond & (df$year  == year)
    if (!is.na(month)) cond <- cond & (df$month == month)

    if ("from_rx" %in% names(patches) && is.character(patches$from_rx[i]) && nzchar(patches$from_rx[i])) {
      cond <- cond & grepl(patches$from_rx[i], df$band_name, perl = TRUE)
    } else if ("from" %in% names(patches)) {
      cond <- cond & (norm(df$band_name) == norm(patches$from[i]))
    } else {
      next
    }
    df$band_name[cond] <- to
  }
  df
}

validate_events <- function(df){
  if (!nrow(df)) return(df)

  # Build allowlists from venue rules: vector of band names per venue_key
  ALLOWED <- list()
  if (length(VENUE_RULES)) {
    for (vk in names(VENUE_RULES)){
      rr <- VENUE_RULES[[vk]]$residencies
      if (length(rr)) {
        ab <- unique(unlist(lapply(rr, function(r) r$allowed_bands %||% character(0))))
        if (length(ab)) ALLOWED[[vk]] <- tolower(trimws(ab))
      }
    }
  }

  df <- df %>%
    mutate(
      band_name   = stringr::str_squish(as.character(band_name)),
      event_title = ifelse(is.na(event_title) | !nzchar(event_title), NA_character_, stringr::str_squish(event_title)),
      event_time  = normalize_time_vec(event_time),
      venue_key   = norm_venue_key(venue),
      has_title   = !is.na(event_title) & nzchar(event_title),
      # Prefer rows that look like well-parsed residencies:
      pref_rank   = dplyr::case_when(
                      has_title & source == "tesseract"   ~ 3L,  # our rules live on this path
                      has_title & source == "openai-text" ~ 2L,
                      has_title                            ~ 1L,
                      TRUE                                 ~ 0L
                    )
    ) %>%
    arrange(desc(pref_rank), source)

  # Drop bogus Salsa-Wednesday rows for venues with an allowlist
  if (length(ALLOWED)) {
    df <- df %>%
      mutate(
        weekday_num = lubridate::wday(event_date, week_start = 1),
        et_low      = tolower(coalesce(event_title, "")),
        band_low    = tolower(band_name)
      ) %>%
      rowwise() %>%
      mutate(keep = {
        ab <- ALLOWED[[venue_key]]
        if (is.null(ab)) TRUE else {
          # Only enforce allowlist on Wednesdays and when title mentions Salsa
          if (weekday_num == 3 && grepl("salsa", et_low)) band_low %in% ab else TRUE
        }
      }) %>%
      ungroup() %>%
      filter(keep) %>%
      select(-keep, -weekday_num, -et_low, -band_low)
  }

  # Deduplicate across sources AFTER ranking
  df <- df %>%
    distinct(venue, event_date, band_name, event_time, .keep_all = TRUE) %>%
    select(-venue_key, -has_title, -pref_rank)

  # Apply global regex/name patches at the end (e.g., Explosión -> Exploración)
  df <- apply_name_patches(df)
  df
}
