# pipeline/validate.R
# robust manual CSV parsing, safe column handling, venue join, manual overrides

suppressPackageStartupMessages({
  library(readr); library(dplyr); library(stringr); library(lubridate)
  library(tidyr); library(tibble); library(purrr)
})

`%||%` <- function(a, b) if (!is.null(a)) a else b

.norm_space <- function(x) {
  x <- as.character(x %||% "")
  x <- gsub("[\u00A0]", " ", x, fixed = TRUE)
  x <- gsub("[_]+", " ", x)
  x <- gsub("\\s+", " ", trimws(x))
  x
}
.norm_key <- function(x) tolower(.norm_space(x))

weekday_es <- function(d) {
  dias <- c("domingo","lunes","martes","miércoles","jueves","viernes","sábado")
  dias[lubridate::wday(d)]
}

.safe_get <- function(df, nm) if (nm %in% names(df)) df[[nm]] else NULL

.maybe_make_date <- function(y, m, d) {
  if (is.null(y) || is.null(m) || is.null(d)) return(NA_Date_)
  suppressWarnings(as.Date(sprintf("%04d-%02d-%02d", as.integer(y), as.integer(m), as.integer(d))))
}
.parse_date_flex <- function(x) {
  x <- as.character(x %||% "")
  if (!nzchar(x)) return(as.Date(NA))
  a <- suppressWarnings(ymd(x)); if (!is.na(a)) return(a)
  b <- suppressWarnings(dmy(x)); if (!is.na(b)) return(b)
  as.Date(NA)
}

.apply_name_patches <- function(df) {
  if (!file.exists("data/name_patches.csv")) return(df)
  patches <- tryCatch(readr::read_csv("data/name_patches.csv", show_col_types = FALSE), error=function(e) NULL)
  if (is.null(patches) || nrow(patches)==0) return(df)
  names(patches) <- tolower(names(patches))
  from_col <- intersect(names(patches), c("from","src","old"))[1]
  to_col   <- intersect(names(patches), c("to","dest","new"))[1]
  if (is.na(from_col) || is.na(to_col) || !("venue" %in% names(df))) return(df)
  key_from <- .norm_key(patches[[from_col]]); key_to <- as.character(patches[[to_col]])
  map <- setNames(key_to, key_from)
  df$venue <- vapply(df$venue, function(v) {
    k <- .norm_key(v); mapped <- map[k]; if (length(mapped)==1 && !is.na(mapped)) as.character(mapped) else v
  }, FUN.VALUE = character(1))
  df
}

.read_manual_events <- function() {
  root <- "data/manual_events"
  if (!dir.exists(root)) return(tibble(source="manual", venue=character(), event_date=as.Date(character()), band_name=character(), event_time=character()))
  files <- list.files(root, pattern="\\.csv$", recursive=TRUE, full.names=TRUE)
  if (!length(files)) return(tibble(source="manual", venue=character(), event_date=as.Date(character()), band_name=character(), event_time=character()))

  purrr::map_dfr(files, function(fp) {
    df <- tryCatch(readr::read_csv(fp, show_col_types = FALSE), error=function(e) NULL)
    if (is.null(df) || nrow(df)==0) return(tibble(source="manual", venue=character(), event_date=as.Date(character()), band_name=character(), event_time=character()))
    names(df) <- tolower(names(df))
    venue <- .safe_get(df, "venue"); if (is.null(venue)) venue <- tools::file_path_sans_ext(basename(fp))
    d1 <- .safe_get(df, "event_date"); d2 <- .safe_get(df, "date")
    y  <- .safe_get(df, "year"); mo <- .safe_get(df, "month"); dy <- .safe_get(df, "day")
    date_vec <- if (!is.null(d1)) d1 else if (!is.null(d2)) d2 else NULL
    if (is.null(date_vec) && !is.null(y) && !is.null(mo) && !is.null(dy)) date_vec <- .maybe_make_date(y, mo, dy)
    if (is.null(date_vec)) ev <- as.Date(rep(NA, nrow(df)))
    else if (inherits(date_vec, "Date")) ev <- as.Date(date_vec)
    else ev <- vapply(as.character(date_vec), .parse_date_flex, FUN.VALUE = as.Date(NA))
    band <- .safe_get(df, "band_name") %||% .safe_get(df, "band"); if (is.null(band)) band <- rep("", nrow(df))
    time_raw <- .safe_get(df, "event_time") %||% .safe_get(df, "time") %||% .safe_get(df, "hora"); if (is.null(time_raw)) time_raw <- rep("", nrow(df))
    tibble(source="manual", venue=.norm_space(venue), event_date=as.Date(ev), band_name=.norm_space(band), event_time=as.character(time_raw)) %>%
      filter(!is.na(event_date) & nzchar(band_name))
  })
}

.read_venues <- function(venues_path) {
  if (!file.exists(venues_path)) {
    return(tibble(venue=character(), municipality=character(), state=character(), latitude=numeric(), longitude=numeric(), venue_id=character()))
  }
  v <- tryCatch(readr::read_csv(venues_path, show_col_types = FALSE), error=function(e) NULL)
  if (is.null(v) || nrow(v)==0) {
    return(tibble(venue=character(), municipality=character(), state=character(), latitude=numeric(), longitude=numeric(), venue_id=character()))
  }
  names(v) <- tolower(names(v))
  vname <- intersect(names(v), c("venue","name","nombre","lugar","sala","espacio"))[1]
  if (is.na(vname)) {
    return(tibble(venue=character(), municipality=character(), state=character(), latitude=numeric(), longitude=numeric(), venue_id=character()))
  }
  muni <- intersect(names(v), c("municipality","municipio","municipalidad"))[1]
  st   <- intersect(names(v), c("state","estado"))[1]
  lat  <- intersect(names(v), c("latitude","lat"))[1]
  lon  <- intersect(names(v), c("longitude","lon","lng"))[1]
  vid  <- intersect(names(v), c("venue_id","id","slug"))[1]

  tibble(
    venue = .norm_space(v[[vname]]),
    municipality = if (!is.na(muni)) as.character(v[[muni]]) else "",
    state        = if (!is.na(st))   as.character(v[[st]])   else "",
    latitude     = suppressWarnings(if (!is.na(lat)) as.numeric(v[[lat]]) else as.numeric(NA)),
    longitude    = suppressWarnings(if (!is.na(lon)) as.numeric(v[[lon]]) else as.numeric(NA)),
    venue_id     = if (!is.na(vid))  as.character(v[[vid]]) else NA_character_
  ) %>%
    mutate(
      venue        = .norm_space(venue),
      municipality = .norm_space(municipality),
      state        = .norm_space(state)
    ) %>%
    distinct(venue, .keep_all = TRUE)
}

validate <- function(extracted, meta, venues_path="data/venues.csv") {
  stopifnot(all(c("source_image","event_date","band_name","event_time") %in% names(extracted)))

  extracted <- extracted %>%
    mutate(
      event_date = as.Date(event_date),
      band_name  = .norm_space(band_name),
      event_time = as.character(event_time %||% "")
    )

  meta2 <- meta %>%
    mutate(venue_guess = .norm_space(.data$venue_guess %||% "")) %>%
    select(source_image, venue_guess) %>%
    distinct()

  extracted_enriched <- extracted %>%
    left_join(meta2, by="source_image") %>%
    mutate(venue = .norm_space(venue_guess)) %>%
    filter(!is.na(event_date) & nzchar(band_name) & nzchar(venue)) %>%
    select(venue, event_date, band_name, event_time)

  manual <- .read_manual_events() %>% .apply_name_patches() %>% filter(nzchar(venue))

  # Manual overrides win per (venue + event_date)
  if (nrow(manual) > 0) {
    keys <- manual %>% mutate(k=paste0(.norm_key(venue),"||",as.character(event_date))) %>% pull(k) %>% unique()
    extracted_enriched <- extracted_enriched %>%
      mutate(k=paste0(.norm_key(venue),"||",as.character(event_date))) %>%
      filter(!(k %in% keys)) %>% select(-k)
  }

  combined <- bind_rows(manual, extracted_enriched) %>%
    distinct(venue, event_date, band_name, event_time, .keep_all = TRUE)

  venues_df <- .read_venues(venues_path)
  joined <- if (nrow(venues_df)==0) {
    combined %>% mutate(municipality="", state="", latitude=as.numeric(NA), longitude=as.numeric(NA), venue_id=NA_character_)
  } else {
    combined %>% left_join(venues_df, by="venue")
  }

  performances <- joined %>%
    mutate(weekday = weekday_es(event_date)) %>%
    select(venue, event_date, weekday, band_name, event_time,
           municipality, state, latitude, longitude, venue_id) %>%
    arrange(state, municipality, venue, event_date, band_name)

  # Aggregations
  if (nrow(performances)) {
    perf2 <- performances %>% mutate(month=floor_date(event_date, "month"))
    agg_municipality <- perf2 %>%
      group_by(state, municipality, month) %>% summarise(events=n(), .groups="drop") %>%
      arrange(state, municipality, month)
    agg_state <- perf2 %>%
      group_by(state, month) %>% summarise(events=n(), .groups="drop") %>%
      arrange(state, month)
  } else {
    agg_municipality <- tibble(state=character(), municipality=character(), month=as.Date(character()), events=integer())
    agg_state <- tibble(state=character(), month=as.Date(character()), events=integer())
  }

  list(performances=performances, agg_municipality=agg_municipality, agg_state=agg_state)
}
