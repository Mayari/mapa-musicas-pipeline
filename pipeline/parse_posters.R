suppressPackageStartupMessages({
  library(tidyverse)
  library(stringr)
  library(lubridate)
})

month_map <- c(
  "enero"=1, "febrero"=2, "marzo"=3, "abril"=4, "mayo"=5, "junio"=6, "julio"=7,
  "agosto"=8, "septiembre"=9, "octubre"=10, "noviembre"=11, "diciembre"=12
)

parse_poster_events <- function(df){
  if (!nrow(df)) return(tibble())
  df |> rowwise() |> mutate(parsed = list(parse_one(ocr_text, venue, year, month, source_image))) |> unnest(parsed)
}

parse_one <- function(txt, venue, year, month, src){
  if (is.na(txt) || !nzchar(txt)) return(tibble())
  t <- tolower(txt)
  t <- str_replace_all(t, "\\r", "\n")
  t <- str_replace_all(t, " +", " ")
  lines <- strsplit(t, "\n")[[1]] |> str_trim() |> discard(~.x == "")
  date_pat <- "(?<![0-9])([0-3]?[0-9])([ ]*de[ ]*(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre))?"
  idx <- which(str_detect(lines, date_pat))
  if (!length(idx)) return(tibble())

  out <- map_dfr(idx, function(i){
    line <- lines[i]
    m <- str_match(line, date_pat)
    day <- suppressWarnings(as.integer(m[,2]))
    mo  <- ifelse(!is.na(m[,3]), month_map[m[,4]], month)
    dt  <- suppressWarnings(as.Date(sprintf("%04d-%02d-%02d", year, mo, day)))
    band <- str_trim(str_remove(line, date_pat))
    if (!nzchar(band)){
      j <- i + 1
      while(j <= length(lines) && !nzchar(band)){
        cand <- lines[j]
        if (!str_detect(cand, "\\bcover|entrada|hrs|pm|am|dj|\\$|reserv|costo")) band <- cand
        j <- j + 1
      }
    }
    tibble(venue = venue, venue_id = NA_character_, event_date = dt, band_name = str_to_title(band), source_image = src)
  })
  out |> filter(!is.na(event_date), nzchar(band_name))
}
