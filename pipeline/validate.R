suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(stringr)
})

# If venue_rules.R was sourced in run_monthly.R, we can use its data here
if (!exists("VENUE_RULES")) VENUE_RULES <- list()

norm_venue_key <- function(x){
  x <- tolower(as.character(x))
  x <- gsub("_", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

# --- Vectorized time normalizer: returns HH:MM (24h) or NA
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

# Optional CSV patches (supports exact or regex)
apply_name_patches <- function(df){
  pth <- file.path("data","name_patches.csv")
  if (!file.exists(pth)) return(df)
  patches <- tryCatch(readr::read_csv(pth, show_col_types = FALSE), error=function(e) tibble())
  if (!nrow(patches)) return(df)

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
    } else next

    df$band_name[cond] <- to
  }
  df
}

validate_events <- function(df){
  if (!nrow(df)) return(df)

  # Core normalization
  df <- df %>%
    mutate(
      band_name   = stringr::str_squish(as.character(band_name)),
      event_title = ifelse(is.na(event_title) | !nzchar(event_title), NA_character_, stringr::str_squish(event_title)),
      event_time  = normalize_time_vec(event_time),
      venue_key   = norm_venue_key(venue),
      rule_name   = ifelse("rule_name" %in% names(.), as.character(.data$rule_name), NA_character_),
      has_title   = !is.na(event_title) & nzchar(event_title),
      weekday_num = lubridate::wday(.data$event_date, week_start = 1),

      # Guarantee year/month columns exist and are numeric (avoid masking lubridate::year())
      year  = if ("year"  %in% names(.)) suppressWarnings(as.integer(.data$year))  else as.integer(lubridate::year(.data$event_date)),
      month = if ("month" %in% names(.)) suppressWarnings(as.integer(.data$month)) else as.integer(lubridate::month(.data$event_date))
    )

  # Prefer our rule-derived rows, then rows with titles
  df <- df %>%
    mutate(pref_rank = dplyr::case_when(
      !is.na(.data$rule_name) & .data$has_title ~ 4L,
      !is.na(.data$rule_name)                   ~ 3L,
      .data$has_title                           ~ 2L,
      TRUE                                      ~ 1L
    )) %>%
    arrange(desc(.data$pref_rank), .data$source)

  # Build per-venue allowlists from rules
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

  # GOLD Salsa pairs: exactly what our Salsa rule emitted
  salsa_gold <- df %>%
    filter(.data$rule_name == "miercoles_de_salsa") %>%
    transmute(
      venue_key = .data$venue_key,
      year      = .data$year,
      month     = .data$month,
      band_name = .data$band_name,
      event_date = .data$event_date
    )

  if (nrow(salsa_gold)) {
    # Drop any Wednesday "Salsa-like" rows that don't match a gold pair
    df <- df %>%
      mutate(
        is_salsa_ctx = .data$weekday_num == 3 & grepl("salsa", tolower(coalesce(.data$event_title, ""))),
        in_gold = dplyr::coalesce(
          paste(.data$venue_key, .data$year, .data$month, .data$band_name, .data$event_date) %in%
            paste(salsa_gold$venue_key, salsa_gold$year, salsa_gold$month, salsa_gold$band_name, salsa_gold$event_date),
          FALSE
        )
      ) %>%
      filter(!(is_salsa_ctx & !in_gold)) %>%
      select(-.data$is_salsa_ctx, -.data$in_gold)
  }

  # If venue has an allowlist, enforce it on Salsa Wednesdays
  if (length(ALLOWED)) {
    df <- df %>%
      mutate(
        et_low = tolower(coalesce(.data$event_title, "")),
        band_low = tolower(.data$band_name),
        keep_allowed = purrr::pmap_lgl(
          list(.data$venue_key, .data$weekday_num, .data$et_low, .data$band_low),
          function(vk, wd, et, bl){
            ab <- ALLOWED[[vk]]
            if (is.null(ab)) return(TRUE)
            if (wd == 3 && grepl("salsa", et)) return(bl %in% ab)
            TRUE
          }
        )
      ) %>%
      filter(.data$keep_allowed) %>%
      select(-.data$keep_allowed, -.data$et_low, -.data$band_low)
  }

  # Last-mile name fixes for Jazzatlán (in case any slip through):
  df <- df %>%
    mutate(
      band_name = ifelse(
        norm_venue_key(.data$venue) == "jazzatlán cholula" &
          grepl("explosi", .data$band_name, ignore.case = TRUE) &
          grepl("latina",  .data$band_name, ignore.case = TRUE),
        "Exploración Latina", .data$band_name),
      band_name = ifelse(
        norm_venue_key(.data$venue) == "jazzatlán cholula" &
          grepl("^\\s*son(\\b|eros)", .data$band_name, ignore.case = TRUE),
        "Soneros Son", .data$band_name)
    )

  # Drop any Salsa rows that ended up without band_name
  df <- df %>%
    filter(!(grepl("salsa", tolower(coalesce(.data$event_title, ""))) &
             (is.na(.data$band_name) | !nzchar(.data$band_name))))

  # Deduplicate after all preferences/filters
  df <- df %>%
    distinct(.data$venue, .data$event_date, .data$band_name, .data$event_time, .keep_all = TRUE) %>%
    select(-.data$venue_key, -.data$weekday_num, -.data$has_title, -.data$pref_rank)

  # Apply CSV patches last
  df <- apply_name_patches(df)
  df
}
