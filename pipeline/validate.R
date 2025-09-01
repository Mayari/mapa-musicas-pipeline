suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(stringr)
})

# Exact headings we move from band_name -> event_title (only if it's just the heading)
HEADING_SET <- tolower(c(
  "miércoles de salsa","miercoles de salsa","martes de jam","jam session",
  "miércoles de jazz","miercoles de jazz","noche de salsa","noche de jazz"
))

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

# Optional band-name correction patches (data/name_patches.csv)
apply_name_patches <- function(df){
  pth <- file.path("data","name_patches.csv")
  if (!file.exists(pth)) return(df)
  patches <- tryCatch(readr::read_csv(pth, show_col_types = FALSE), error=function(e) tibble())
  if (!nrow(patches)) return(df)

  norm <- function(s) tolower(gsub("\\s+"," ", trimws(as.character(s))))
  patches <- patches %>%
    mutate(
      from_n = norm(from),
      to     = as.character(to),
      venue  = ifelse("venue" %in% names(.), as.character(venue), NA_character_),
      year   = ifelse("year"  %in% names(.), suppressWarnings(as.integer(year)), NA_integer_),
      month  = ifelse("month" %in% names(.), suppressWarnings(as.integer(month)), NA_integer_)
    )

  df$band_name <- as.character(df$band_name)
  bnorm <- norm(df$band_name)

  for (i in seq_len(nrow(patches))){
    cond <- bnorm == patches$from_n[i]
    if (!is.na(patches$venue[i])) cond <- cond & (tolower(df$venue) == tolower(patches$venue[i]))
    if (!is.na(patches$year[i]))  cond <- cond & (df$year  == patches$year[i])
    if (!is.na(patches$month[i])) cond <- cond & (df$month == patches$month[i])
    df$band_name[cond] <- patches$to[i]
    # refresh normalized cache for cascading patches
    bnorm[cond] <- norm(patches$to[i])
  }
  df
}

validate_events <- function(df){
  if (!nrow(df)) return(df)

  df <- df %>%
    mutate(
      band_name   = stringr::str_squish(as.character(band_name)),
      event_title = ifelse(is.na(event_title) | !nzchar(event_title), NA_character_, stringr::str_squish(event_title)),
      band_low    = tolower(band_name) %>% gsub("[^\\p{L}\\s]", "", .) %>% gsub("\\s+", " ", .) %>% trimws(),

      # Only treat as heading if the whole band_name is an exact known heading
      is_exact_heading = band_low %in% HEADING_SET,

      event_title = dplyr::coalesce(event_title, ifelse(is_exact_heading, band_name, NA_character_)),
      band_name   = ifelse(is_exact_heading, NA_character_, band_name),

      # Normalize times
      event_time  = normalize_time_vec(event_time)
    ) %>%
    filter(!is.na(band_name) & nzchar(band_name)) %>%
    select(-band_low) %>%
    arrange(source) %>%
    distinct(venue, event_date, band_name, event_time, .keep_all = TRUE)

  # Apply optional manual patches (e.g., Explosión -> Exploración)
  df <- apply_name_patches(df)
  df
}
