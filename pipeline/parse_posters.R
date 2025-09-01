suppressPackageStartupMessages({
  library(tidyverse)
  library(stringr)
  library(lubridate)
})

# Month names (full + short)
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

# Weekday map (Spanish; we compute actual days for residencias)
wday_map <- c(
  "lunes"=1, "lun"=1,
  "martes"=2, "mar"=2,
  "miercoles"=3, "miércoles"=3, "mie"=3, "mié"=3,
  "jueves"=4, "jue"=4,
  "viernes"=5, "vie"=5,
  "sabado"=6, "sábado"=6, "sab"=6, "sáb"=6,
  "domingo"=7, "dom"=7
)

# --- helpers ---
extract_time_24h <- function(s){
  # Finds first time token and normalizes to HH:MM (24h)
  tkn <- str_match(tolower(s),
                   "\\b(\\d{1,2})(?:[:\\.](\\d{2}))?\\s*(h|hrs?)?\\b|\\b(\\d{1,2})\\s*(:\\s*(\\d{2}))?\\s*(am|pm)\\b")
  if (all(is.na(tkn))) return(NA_character_)
  # Cases: 24h (groups 2..3) OR 12h (groups 4..7)
  if (!is.na(tkn[,2])) {
    hh <- as.integer(tkn[,2]); mm <- ifelse(is.na(tkn[,3]), 0L, as.integer(tkn[,3]))
    if (hh >= 0 && hh <= 24 && mm >= 0 && mm < 60) return(sprintf("%02d:%02d", hh %% 24, mm))
  } else {
    hh <- as.integer(tkn[,4]); mm <- ifelse(is.na(tkn[,6]), 0L, as.integer(tkn[,6]))
    ampm <- tkn[,7]
    if (!is.na(hh)) {
      if (tolower(ampm) == "pm" && hh < 12) hh <- hh + 12
      if (tolower(ampm) == "am" && hh == 12) hh <- 0
      if (hh >= 0 && hh < 24 && mm >= 0 && mm < 60) return(sprintf("%02d:%02d", hh, mm))
    }
  }
  NA_character_
}

dates_for_weekday <- function(year, month, wday){
  days <- seq.Date(ymd(sprintf("%04d-%02d-01", year, month)),
                   ymd(sprintf("%04d-%02d-01", year, month)) + months(1) - days(1),
                   by = "1 day")
  days[wday(days, week_start = 1) == wday]
}

parse_poster_events <- function(df){
  if (!nrow(df)) return(tibble())
  df |>
    rowwise() |>
    mutate(parsed = list(parse_one(ocr_text, venue, year, month, source_image))) |>
    unnest(parsed)
}

parse_one <- function(txt, venue, year, month, src){
  if (is.na(txt) || !nzchar(txt)) return(tibble())

  t <- tolower(txt)
  t <- str_replace_all(t, "\\r", "\n")
  t <- str_replace_all(t, " +", " ")
  lines <- strsplit(t, "\n")[[1]] |> str_trim()
  lines <- lines[lines != ""]

  # --- 1) Direct “day number + band” lines ---
  mo_re <- "(enero|ene|febrero|feb|marzo|mar|abril|abr|mayo|may|junio|jun|julio|jul|agosto|ago|septiembre|sep|setiembre|set|octubre|oct|noviembre|nov|diciembre|dic)"
  date_pat <- paste0("(?<![0-9])([0-3]?[0-9])(?:\\s*de\\s*", mo_re, ")?")
  idx <- which(str_detect(lines, date_pat))
  out <- map_dfr(idx, function(i){
    line <- lines[i]
    m <- str_match(line, date_pat)
    day <- suppressWarnings(as.integer(m[,2]))
    mo_tok <- m[,3]
    mo_val <- ifelse(!is.na(mo_tok), month_map[gsub("[^a-z]", "", mo_tok)], month)
    dt  <- suppressWarnings(as.Date(sprintf("%04d-%02d-%02d", year, mo_val, day)))
    band <- str_trim(str_remove(line, date_pat))
    if (!nzchar(band)){
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
      event_time = extract_time_24h(paste(line, collapse=" ")),
      source_image = src
    )
  }) |> filter(!is.na(event_date), nzchar(band_name))

  # --- 2) Residencias: "cada martes" / "todos los miércoles" ---
  resid_idx <- which(str_detect(lines, "(cada|todos los)\\s+(lunes|martes|miercoles|miércoles|jueves|viernes|sabado|sábado|domingo)"))
  if (length(resid_idx)){
    for (i in resid_idx){
      line <- lines[i]
      mm <- str_match(line, "(cada|todos los)\\s+(lunes|martes|miercoles|miércoles|jueves|viernes|sabado|sábado|domingo)")
      wd_tok <- mm[,3]
      wd <- wday_map[gsub("[^a-záéíóúñ]", "", wd_tok)]
      # band candidate: same line remainder or next non-noise line
      band_text <- str_trim(str_remove(line, "(cada|todos los)\\s+\\S+"))
      if (!nzchar(band_text)){
        k <- i + 1
        while(k <= length(lines) && !nzchar(band_text)){
          cand <- lines[k]
          if (!str_detect(cand, "\\bcover|entrada|reserv|costo|hrs|pm|am|dj|\\$|semana|mes")) band_text <- cand
          k <- k + 1
        }
      }
      if (!nzchar(band_text)) next

      # If there is a list of day numbers near this block, use those instead of expanding all weekdays
      near <- paste(lines[pmax(1, i-2):pmin(length(lines), i+2)], collapse=" ")
      day_nums <- str_match_all(near, "(?<!\\d)([0-3]?\\d)(?!\\d)")[[1]][,2] |> as.integer() |> unique()
      day_nums <- day_nums[day_nums >= 1 & day_nums <= 31]

      timestr <- extract_time_24h(near)
      dates <- if (length(day_nums) >= 1){
        suppressWarnings(as.Date(sprintf("%04d-%02d-%02d", year, month, day_nums)))
      } else if (!is.na(wd)) {
        dates_for_weekday(year, month, wd)
      } else {
        as.Date(character())
      }

      if (length(dates)){
        out <- bind_rows(out, tibble(
          venue = venue, venue_id = NA_character_,
          event_date = dates,
          band_name = str_to_title(band_text),
          event_time = timestr,
          source_image = src
        ))
      }
    }
  }

  out |> distinct(venue, event_date, band_name, event_time, .keep_all = TRUE)
}
