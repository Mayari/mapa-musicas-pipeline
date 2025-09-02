suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(stringr)
})

if (!exists("VENUE_RULES")) VENUE_RULES <- list()

norm_venue_key <- function(x){
  x <- tolower(as.character(x))
  x <- gsub("_", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

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

# Optional CSV patches
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

  df <- df %>%
    mutate(
      band_name   = stringr::str_squish(as.character(band_name)),
      event_title = ifelse(is.na(event_title) | !nzchar(event_title), NA_character_, stringr::str_squish(event_title)),
      event_time  = normalize_time_vec(event_time),
      venue_key   = norm_venue_key(venue),
      rule_name   = ifelse("rule_name" %in% names(.), as.character(rule_name), NA_character_),
      has_title   = !is.na(event_title) & nzchar(event_title),
      weekday_num = lubridate::wday(event_date, week_start = 1)
    )

  # Prefer our rule-derived rows, then rows with titles
  df <- df %>%
    mutate(pref_rank = dplyr::case_when(
      !is.na(rule_name) & has_title ~ 4L,
      !is.na(rule_name)            ~ 3L,
      has_title                    ~ 2L,
      TRUE                         ~ 1L
    )) %>%
    arrange(desc(pref_rank), source)

  # Build per-venue allowlists from rules
  ALLOWED <- list()
  for (vk in names(VENUE_RULES)){
    rr <- VENUE_RULES[[vk]]$residencies
    if (length(rr)) {
      ab <- unique(unlist(lapply(rr, function(r) r$allowed_bands %||% character(0))))
      if (length(ab)) ALLOWED[[vk]] <- tolower(trimws(ab))
    }
  }

  # GOLD Salsa pairs: exactly what our Salsa rule emitted
  salsa_gold <- df %>%
    filter(rule_name == "miercoles_de_salsa") %>%
    transmute(venue_key, year, month, band_name, event_date)

  if (nrow(salsa_gold)) {
    # Drop any Wednesday "Salsa-like" rows that don't match a gold pair (prevents LLM stray dates)
    df <- df %>%
      mutate(
        is_salsa_ctx = weekday_num == 3 & grepl("salsa", tolower(coalesce(event_title, ""))),
        in_gold = dplyr::coalesce(
          paste(venue_key, year, month, band_name, event_date) %in%
            paste(salsa_gold$venue_key, salsa_gold$year, salsa_gold$month, salsa_gold$band_name, salsa_gold$event_date),
          FALSE
        )
      ) %>%
      filter(!(is_salsa_ctx & !in_gold)) %>%
      select(-is_salsa_ctx, -in_gold)
  }

  # If venue has an allowlist, enforce it on Salsa Wednesdays
  if (length(ALLOWED)) {
    df <- df %>%
      mutate(
        et_low = tolower(coalesce(event_title, "")),
        band_low = tolower(band_name),
        keep_allowed = purrr::pmap_lgl(list(venue_key, weekday_num, et_low, band_low), function(vk, wd, et, bl){
          ab <- ALLOWED[[vk]]
          if (is.null(ab)) return(TRUE)
          if (wd == 3 && grepl("salsa", et)) return(bl %in% ab)
          TRUE
        })
      ) %>%
      filter(keep_allowed) %>%
      select(-keep_allowed, -et_low, -band_low)
  }

  # Last-mile name fixes for Jazzatlán (in case any slip through):
  df <- df %>%
    mutate(
      band_name = ifelse(
        venue_key == "jazzatlán cholula" & grepl("explosi", band_name, ignore.case = TRUE) & grepl("latina", band_name, ignore.case = TRUE),
        "Exploración Latina", band_name),
      band_name = ifelse(
        venue_key == "jazzatlán cholula" & grepl("^\\s*son(\\b|eros)", band_name, ignore.case = TRUE),
        "Soneros Son", band_name)
    )

  # Drop any Salsa rows that ended up without band_name
  df <- df %>%
    filter(!(grepl("salsa", tolower(coalesce(event_title, ""))) & (is.na(band_name) | !nzchar(band_name))))

  # Deduplicate after all preferences/filters
  df <- df %>%
    distinct(venue, event_date, band_name, event_time, .keep_all = TRUE) %>%
    select(-venue_key, -weekday_num, -has_title, -pref_rank)

  # Apply CSV patches (regex or exact), then return
  df <- apply_name_patches(df)
  df
}
