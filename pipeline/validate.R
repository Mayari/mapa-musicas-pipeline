# pipeline/validate.R
# v3.0.0 robust join + manual overrides + venue metadata + aggregates
suppressPackageStartupMessages({
  library(dplyr); library(readr); library(stringr); library(lubridate); library(tibble)
})

.norm_venue <- function(x) {
  x <- x %||% ""
  x <- stringr::str_replace_all(x, "[_]+", " ")
  x <- stringr::str_squish(x)
  x
}
`%||%` <- function(a,b) if (!is.null(a)) a else b

# Spanish weekday
weekday_es <- function(d) {
  d <- as.Date(d)
  # base::weekdays respects locale; map manually to be stable
  c("domingo","lunes","martes","miércoles","jueves","viernes","sábado")[as.POSIXlt(d)$wday + 1]
}

# Read manual overrides: data/manual_events/<State>/<Venue>.csv
.read_manual_events <- function(images_dir) {
  root <- file.path(dirname(images_dir), "pipeline_repo", "data", "manual_events") # typical in Actions
  alt  <- file.path("data", "manual_events")                                     # local dev run
  dir_path <- if (dir.exists(root)) root else alt
  if (!dir.exists(dir_path)) return(tibble())

  files <- list.files(dir_path, pattern="\\.csv$", recursive=TRUE, full.names=TRUE)
  if (!length(files)) return(tibble())
  purrr::map_dfr(files, function(f) {
    df <- tryCatch(readr::read_csv(f, show_col_types = FALSE), error=function(e) tibble())
    if (!nrow(df)) return(tibble())
    names(df) <- tolower(names(df))
    df <- df %>%
      mutate(
        event_date = coalesce(as.Date(.data$event_date), as.Date(.data$date),
                              as.Date(sprintf("%s-%s-%s", .data$year, .data$month, .data$day), quiet=TRUE)),
        band_name  = coalesce(.data$band_name, .data$band),
        event_time = coalesce(.data$event_time, .data$time, .data$hora),
        venue      = .norm_venue(coalesce(.data$venue, "")))
    df %>% select(event_date, band_name, event_time, venue) %>% filter(!is.na(event_date) & nzchar(band_name))
  })
}

validate <- function(extracted, meta, venues_path, images_dir) {
  if (missing(extracted) || !nrow(extracted)) {
    return(list(
      performances = tibble(venue=character(), event_date=as.Date(character()), weekday=character(),
                            band_name=character(), event_time=character(), municipality=character(),
                            state=character(), latitude=double(), longitude=double(), venue_id=character()),
      agg_municipality = tibble(), agg_state = tibble()
    ))
  }

  # Basic normalize extracted
  ex <- extracted %>%
    transmute(source_image,
              event_date = as.Date(event_date),
              band_name  = coalesce(band_name, ""),
              event_time = coalesce(event_time, "")) %>%
    filter(!is.na(event_date) & nzchar(band_name))

  # Join filename meta to get venue_guess
  meta2 <- meta %>% transmute(source_image, venue_guess = .data$venue_guess)
  ex2 <- ex %>% left_join(meta2, by = "source_image") %>%
    mutate(venue = .norm_venue(coalesce(venue_guess, ""))) %>%
    select(-venue_guess)

  # Manual overrides (win on same date/venue)
  man <- .read_manual_events(images_dir)
  if (nrow(man)) {
    # prefer manual rows; union with model rows not duplicated
    model_rows <- ex2 %>% anti_join(man, by = c("venue","event_date","band_name"))
    ex2 <- bind_rows(man, model_rows)
  }

  # Venue metadata
  ven <- tryCatch(readr::read_csv(venues_path, show_col_types = FALSE), error=function(e) tibble())
  if (!nrow(ven)) {
    ven <- tibble(venue=character(), municipality=character(), state=character(), latitude=double(), longitude=double(), venue_id=character())
  }
  names(ven) <- tolower(names(ven))
  if (!"venue" %in% names(ven)) {
    # try guess a name column
    cname <- intersect(names(ven), c("name","nombre","lugar","espacio","sala"))
    if (length(cname)) {
      ven <- ven %>% rename(venue = !!cname[1])
    } else {
      ven <- ven %>% mutate(venue = "")
    }
  }
  ven <- ven %>%
    transmute(
      venue = .norm_venue(venue),
      municipality = coalesce(municipality, ""),
      state = coalesce(state, ""),
      latitude = suppressWarnings(as.double(latitude)),
      longitude = suppressWarnings(as.double(longitude)),
      venue_id = coalesce(venue_id, "")
    )

  perf <- ex2 %>%
    mutate(weekday = weekday_es(event_date)) %>%
    left_join(ven, by = "venue") %>%
    transmute(
      venue, event_date, weekday,
      band_name, event_time,
      municipality = municipality %||% "",
      state        = state %||% "",
      latitude     = latitude %||% NA_real_,
      longitude    = longitude %||% NA_real_,
      venue_id     = venue_id %||% ""
    ) %>%
    arrange(event_date, venue, band_name)

  # Aggregations
  agg_mun <- perf %>% group_by(state, municipality) %>%
    summarise(events = n(), first_date = min(event_date), last_date = max(event_date), .groups="drop")
  agg_st  <- perf %>% group_by(state) %>%
    summarise(events = n(), first_date = min(event_date), last_date = max(event_date), .groups="drop")

  list(performances = perf, agg_municipality = agg_mun, agg_state = agg_st)
}
