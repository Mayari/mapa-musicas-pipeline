suppressPackageStartupMessages({
  library(tidyverse)
  library(stringr)
  library(lubridate)
})

# Map full + short Spanish month forms (we lowercase OCR text elsewhere)
month_map <- c(
  "enero"=1, "ene"=1,
  "febrero"=2, "feb"=2,
  "marzo"=3, "mar"=3,
  "abril"=4, "abr"=4,
  "mayo"=5, "may"=5,
  "junio"=6, "jun"=6,
  "julio"=7, "jul"=7,
  "agosto"=8, "ago"=8,
  "septiembre"=9, "sep"=9, "setiembre"=9, "set"=9,
  "octubre"=10, "oct"=10,
  "noviembre"=11, "nov"=11,
  "diciembre"=12, "dic"=12
)

parse_poster_events <- function(df){
  if (!nrow(df)) return(tibble())
  df |>
    rowwise() |>
    mutate(parsed = list(parse_one(ocr_text, venue, year, month, source_image))) |>
    unnest(parsed)
}

parse_one <- function(txt, venue, year, month, src){
  if (is.na(txt) || !nzchar(txt)) return(tibble())

  # normalize text a bit
  t <- tolower(txt)
  t <- str_replace_all(t, "\\r", "\n")
  t <- str_replace_all(t, " +", " ")
  lines <- strsplit(t, "\n")[[1]] |> str_trim() |> discard(~.x == "")

  # allow full + short month names
  mo_re <- "(enero|ene|febrero|feb|marzo|mar|abril|abr|mayo|may|junio|jun|julio|jul|agosto|ago|septiembre|sep|setiembre|set|octubre|oct|noviembre|nov|diciembre|dic)"
  date_pat <- paste0("(?<![0-9])([0-3]?[0-9])(?:\\s*de\\s*", mo_re, ")?")

  idx <- which(str_detect(lines, date_pat))
  if (!length(idx)) return(tibble())

  out <- map_dfr(idx, function(i){
    line <- lines[i]
    m <- str_match(line, date_pat)
    day <- suppressWarnings(as.integer(m[,2]))
    mo_tok <- m[,3]                                     # capture group 3 = month token
    mo_val <- ifelse(!is.na(mo_tok), month_map[gsub("[^a-z]", "", mo_tok)], month)
    dt  <- suppressWarnings(as.Date(sprintf("%04d-%02d-%02d", year, mo_val, day)))

    band <- str_trim(str_remove(line, date_pat))
    if (!nzchar(band)){
      # fallback: take the next non-noisy line
      j <- i + 1
      while(j <= length(lines) && !nzchar(band)){
        cand <- lines[j]
        if (!str_detect(cand, "\\bcover|entrada|hrs|pm|am|dj|\\$|reserv|costo")) band <- cand
        j <- j + 1
      }
    }

    tibble(
      venue = venue, venue_id = NA_character_,
      event_date = dt, band_name = str_to_title(band),
      source_image = src
    )
  })

  out |> filter(!is.na(event_date), nzchar(band_name))
}
