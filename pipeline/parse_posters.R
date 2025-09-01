suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(stringr)
})

message("[parse_posters.R] v2025-09-01 salsa-window+snap+jam")

weekday_index <- c(
  "lunes"=1, "martes"=2, "miércoles"=3, "miercoles"=3,
  "jueves"=4, "viernes"=5, "sábado"=6, "sabado"=6, "domingo"=7
)

dates_for <- function(year, month, wd){
  first <- as.Date(sprintf("%04d-%02d-01", year, month))
  last  <- first + months(1) - days(1)
  all   <- seq(first, last, by = "1 day")
  all[lubridate::wday(all, week_start = 1) == wd]
}

# If OCR read a wrong day (e.g., 19 instead of 17), snap within ±2 days to the target weekday.
snap_day_to_weekday <- function(day, year, month, wd_idx){
  cand <- day + c(0, -1, +1, -2, +2)
  cand <- cand[cand >= 1 & cand <= 31]
  for (d in cand){
    dt <- as.Date(sprintf("%04d-%02d-%02d", year, month, d))
    if (lubridate::wday(dt, week_start = 1) == wd_idx) return(d)
  }
  NA_integer_
}

parse_day_list <- function(s){
  s <- tolower(s)
  s <- gsub("[^0-9y, ]", "", s)     # keep digits, y, commas, spaces
  s <- gsub("\\s*y\\s*", ",", s)    # "10 y 24" -> "10,24"
  toks <- unlist(strsplit(s, "[, ]+"))
  as.integer(unique(toks[nchar(toks) > 0]))
}

title_case_es <- function(s){
  if (is.na(s) || !nzchar(s)) return(NA_character_)
  s <- tolower(s)
  s <- gsub("\\bmiercoles\\b", "miércoles", s)
  s <- gsub("\\bsabado\\b", "sábado", s)
  tools::toTitleCase(s)
}

# Extract a bounded block of lines under a heading, to avoid picking stray matches elsewhere.
block_after_heading <- function(lines_lc, start_idx, max_lines = 14){
  # Next heading or section delimiter pattern
  stop_pat <- "(^\\s*(lunes|martes|mi[eé]rcoles|miercoles|jueves|viernes|s[áa]bado|sabado|domingo)\\b)|residen|big\\s*band|programaci[oó]n|cartelera|covers?|cover|entrada|reserv"
  end_idx <- length(lines_lc)
  nxt <- which(grepl(stop_pat, lines_lc))
  nxt <- nxt[nxt > start_idx]
  if (length(nxt)) end_idx <- min(end_idx, min(nxt) - 1L)
  end_idx <- min(end_idx, start_idx + max_lines)
  c(start_idx, end_idx)
}

parse_one <- function(row){
  txt <- row$ocr_text
  if (is.na(txt) || !nzchar(txt)) return(tibble())

  # Work line-by-line for better locality
  raw_lines <- unlist(strsplit(txt, "\\r?\\n"))
  lines <- stringr::str_squish(raw_lines)
  lines_lc <- tolower(lines)

  out <- list()

  # ---- 1) Miércoles de Salsa block, then parse "Band: 10 y 24" lines within that block ----
  salsa_idx <- which(grepl("mi[eé]rcoles\\s+de\\s+salsa", lines_lc))
  if (length(salsa_idx)) {
    rng <- block_after_heading(lines_lc, salsa_idx[1], max_lines = 16)
    block <- paste(lines[rng[1]:rng[2]], collapse = "\n")

    m <- stringr::str_match_all(
      tolower(block),
      "([\\p{L}\\d &'\\.\\-]+?)\\s*[:\\-]\\s*([0-9]{1,2}(?:\\s*(?:,|y)\\s*[0-9]{1,2})+)"
    )
    if (length(m) && length(m[[1]]) > 0){
      mm <- m[[1]]
      bands <- trimws(mm[,2])
      dayss <- mm[,3]

      WED <- weekday_index["miercoles"] # 3
      for (i in seq_along(bands)){
        dn0 <- parse_day_list(dayss[i])
        dn  <- unique(na.omit(sapply(dn0, snap_day_to_weekday, year=row$year, month=row$month, wd_idx=WED)))
        if (length(dn)){
          # Venue-specific guard: Jazzatlán posters sometimes OCR "Orquesta ..." noise; drop those for Salsa block.
          vn <- tolower(row$venue)
          b_clean <- stringr::str_squish(stringr::str_to_title(bands[i]))
          if (grepl("jazzatl[aá]n", vn) && grepl("^\\s*orquesta\\b", tolower(b_clean))) next

          dates <- as.Date(sprintf("%04d-%02d-%02d", row$year, row$month, dn))
          out[[length(out)+1]] <- tibble(
            venue        = row$venue,
            venue_id     = NA_character_,
            event_date   = dates,
            band_name    = b_clean,
            event_title  = title_case_es("Miércoles de Salsa"),
            event_time   = NA_character_,
            source_image = row$image_path
          )
        }
      }
    }
  }

  # ---- 2) Martes de Jam / Jam Session residencies ----
  low <- tolower(txt)
  jam_present <- grepl("\\bjam\\b", low) || grepl("jam\\s*session", low)
  jam_martes  <- grepl("(cada|todos?\\s+los)\\s+martes", low) ||
                 grepl("martes\\s+de\\s+jam", low) ||
                 grepl("martes[^\\n]{0,20}jam", low) ||
                 grepl("jam[^\\n]{0,20}martes", low)

  if (jam_present && jam_martes) {
    tues <- dates_for(row$year, row$month, weekday_index["martes"])
    if (length(tues)){
      out[[length(out)+1]] <- tibble(
        venue        = row$venue,
        venue_id     = NA_character_,
        event_date   = tues,
        band_name    = "Jam de la casa",
        event_title  = title_case_es("Martes de Jam"),
        event_time   = NA_character_,
        source_image = row$image_path
      )
    }
  }

  if (!length(out)) return(tibble())
  dplyr::bind_rows(out)
}

# ocr_raw must have: image_path, ocr_text, venue, year, month
parse_poster_events <- function(ocr_raw){
  purrr::map_dfr(seq_len(nrow(ocr_raw)), function(i) parse_one(ocr_raw[i,]))
}
