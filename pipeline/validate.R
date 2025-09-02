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

merge_manual_events <- function(df){
  pth <- file.path("data","manual_events.csv")
  if (!file.exists(pth)) return(df)
  man <- tryCatch(readr::read_csv(pth, show_col_types = FALSE), error=function(e) tibble())
  if (!nrow(man)) return(df)

  if (!("event_date" %in% names(man))) {
    if ("date" %in% names(man)) {
      man$event_date <- as.Date(man$date)
    } else if (all(c("year","month","day") %in% names(man))) {
      man$event_date <- as.Date(sprintf("%04d-%02d-%02d",
                                        as.integer(man$year),
                                        as.integer(man$month),
                                        as.integer(man$day)))
    }
  }
  man <- man %>%
    transmute(
      venue        = as.character(venue),
      venue_id     = NA_character_,
      event_date   = as.Date(event_date),
      band_name    = stringr::str_squish(as.character(band_name)),
      event_title  = if ("event_title" %in% names(.)) as.character(event_title) else NA_character_,
      event_time   = if ("event_time"  %in% names(.)) as.character(event_time)  else NA_character_,
      source_image = if ("source_image" %in% names(.)) as.character(source_image) else NA_character_
    ) %>%
    mutate(event_time = normalize_time_vec(event_time)) %>%
    filter(!is.na(event_date), nzchar(venue), nzchar(band_name))

  if (!nrow(man)) return(df)

  df %>%
    anti_join(man %>% select(venue, event_date), by = c("venue","event_date")) %>%
    bind_rows(man)
}

validate_events <- function(df){
  if (!nrow(df)) return(df)

  df <- df %>%
    mutate(
      band_name   = stringr::str_squish(as.character(band_name)),
      event_title = ifelse(is.na(event_title) | !nzchar(event_title), NA_character_, stringr::str_squish(event_title)),
      event_time  = normalize_time_vec(event_time),
      venue_key   = norm_venue_key(venue),
      rule_name   = ifelse("rule_name" %in% names(.), as.character(.data$rule_name), NA_character_),
      has_title   = !is.na(event_title) & nzchar(event_title),
      weekday_num = lubridate::wday(.data$event_date, week_start = 1),
      year  = if ("year"  %in% names(.)) suppressWarnings(as.integer(.data$year))  else as.integer(lubridate::year(.data$event_date)),
      month = if ("month" %in% names(.)) suppressWarnings(as.integer(.data$month)) else as.integer(lubridate::month(.data$event_date))
    ) %>%
    mutate(pref_rank = dplyr::case_when(
      !is.na(.data$rule_name) & .data$has_title ~ 4L,
      !is.na(.data$rule_name)                   ~ 3L,
      .data$has_title                           ~ 2L,
      TRUE                                      ~ 1L
    )) %>%
    arrange(desc(.data$pref_rank), .data$source)

  # --- Build allowlists for Salsa (from rules) ---
  ALLOWED <- list()
  VENUE_SKIP <- character(0)
  if (length(VENUE_RULES)) {
    for (vk in names(VENUE_RULES)){
      rr <- VENUE_RULES[[vk]]$residencies
      if (length(rr)) {
        ab <- unique(unlist(lapply(rr, function(r) r$allowed_bands %||% character(0))))
        if (length(ab)) ALLOWED[[vk]] <- tolower(trimws(ab))
      }
      if (isTRUE(VENUE_RULES[[vk]]$skip_residency_if_single)) {
        VENUE_SKIP <- c(VENUE_SKIP, vk)
      }
    }
  }

  # --- GOLD Salsa pairs (exact output from Salsa rule) ---
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

  # --- Enforce allowlist on Salsa Wednesdays ---
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

  # --- Jazzatlán-type precedence: singles > residencies on the same date ---
  if (length(VENUE_SKIP)) {
    df <- df %>%
      mutate(
        is_residency = (!is.na(.data$rule_name) & .data$rule_name %in% c("miercoles_de_salsa","martes_de_jam")) |
                       grepl("\\b(salsa|jam)\\b", tolower(coalesce(.data$event_title, ""))),
        venue_skip = .data$venue_key %in% VENUE_SKIP
      ) %>%
      group_by(.data$venue_key, .data$event_date) %>%
      mutate(any_single_here = any(!.data$is_residency)) %>%
      ungroup() %>%
      filter(!(venue_skip & is_residency & any_single_here)) %>%
      select(-.data$is_residency, -.data$venue_skip, -.data$any_single_here)
  }

  # --- Last-mile name fixes for Jazzatlán ---
  df <- df %>%
    mutate(
      band_name = ifelse(
        norm_venue_key(.data$venue) == "jazzatlán cholula" &
          grepl("explosi|expropiaci", .data$band_name, ignore.case = TRUE) &
          grepl("latina",            .data$band_name, ignore.case = TRUE),
        "Exploración Latina", .data$band_name),
      band_name = ifelse(
        norm_venue_key(.data$venue) == "jazzatlán cholula" &
          grepl("^\\s*son(\\b|eros)", .data$band_name, ignore.case = TRUE),
        "Soneros Son", .data$band_name)
    )

  # Drop Salsa rows with empty band
  df <- df %>%
    filter(!(grepl("salsa", tolower(coalesce(.data$event_title, ""))) &
             (is.na(.data$band_name) | !nzchar(.data$band_name))))

  # Deduplicate
  df <- df %>%
    distinct(.data$venue, .data$event_date, .data$band_name, .data$event_time, .keep_all = TRUE) %>%
    select(-.data$venue_key, -.data$weekday_num, -.data$has_title, -.data$pref_rank)

  # Patches then manual overrides last
  df <- apply_name_patches(df)
  df <- merge_manual_events(df)
  df
}
