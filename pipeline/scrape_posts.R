#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(tidyverse); library(lubridate); library(httr2); library(readr); library(rvest); library(stringr); library(jsonlite)
})

cat(">> scrape_posts v1 (Facebook Graph + simple websites -> scraped_candidates.csv)\n")

# --- config & helpers
args <- commandArgs(trailingOnly = TRUE)
get_arg <- function(flag, default=NULL){ hit <- which(args==flag); if(length(hit) && hit < length(args)) args[hit+1] else default }
sources_path <- get_arg("--sources_path","data/venue_sources.csv")
out_dir <- get_arg("--out_dir","data")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

OPENAI_API_KEY <- Sys.getenv("OPENAI_API_KEY")
TEXT_MODEL <- Sys.getenv("OPENAI_TEXT_MODEL"); FALLBACK <- Sys.getenv("OPENAI_MODEL")
if (!nzchar(TEXT_MODEL)) TEXT_MODEL <- FALLBACK
if (!nzchar(TEXT_MODEL)) TEXT_MODEL <- "gpt-4.1"
FB_TOKEN <- Sys.getenv("FB_GRAPH_TOKEN")

`%||%` <- function(a,b) if (!is.null(a) && !is.na(a) && length(a)>0 && nzchar(a)) a else b

# --- tiny OpenAI post parser (resolves relative dates by post date)
extract_from_post <- function(text, post_date, venue){
  if (!nzchar(OPENAI_API_KEY) || !nzchar(TEXT_MODEL) || !nzchar(text)) return(tibble())
  pd <- as.Date(post_date)
  prmpt <- sprintf(
'Tu tarea: a partir del texto de una publicación (fecha de publicación: %s), devuelve SOLO JSON minificado:
{"venue":"%s","events":[{"date":"YYYY-MM-DD","band":"<artista>","time":null}]}
Reglas:
- Si el texto usa expresiones relativas: "hoy", "mañana", "este viernes", "próximo miércoles", calcula la fecha exacta usando la fecha de publicación.
- Si hay varias fechas o listas de días, emite una entrada por actuación.
- Incluye "time" si se menciona (e.g., "20:00" o "8 pm"), en 24h si posible; si no, null.
- NO inventes; si hay duda, omite. SOLO JSON, sin texto extra.
---
%s
---', as.character(pd), venue, substr(text,1,8000))

  body <- list(
    model = TEXT_MODEL,
    messages = list(list(role="user", content=list(list(type="text", text=prmpt)))),
    response_format = list(type="json_object")
  )
  req <- request("https://api.openai.com/v1/chat/completions") |>
    req_headers(Authorization = paste("Bearer", OPENAI_API_KEY),
                "Content-Type" = "application/json") |>
    req_body_json(body, auto_unbox = TRUE)

  resp <- tryCatch(req_perform(req), error=function(e) e)
  if (inherits(resp,"error")) return(tibble())
  if (resp_status(resp) < 200 || resp_status(resp) >= 300) return(tibble())
  rj <- tryCatch(resp_body_json(resp), error=function(e) NULL); if (is.null(rj)) return(tibble())

  txt <- tryCatch({
    msg <- rj$choices[[1]]$message
    if (is.character(msg$content)) msg$content else
      if (is.list(msg$content) && length(msg$content)>0 && !is.null(msg$content[[1]]$text))
        paste0(vapply(msg$content, function(p) p$text %||% "", character(1L)), collapse="\n")
      else jsonlite::toJSON(msg, auto_unbox = TRUE)
  }, error=function(e) "")

  # Try JSON parse
  obj <- tryCatch(jsonlite::fromJSON(txt), error=function(e) NULL)
  if (is.null(obj) || is.null(obj$events)) return(tibble())

  ev <- if (is.data.frame(obj$events)) obj$events else tibble::as_tibble(obj$events)
  if (!("date" %in% names(ev) && "band" %in% names(ev))) return(tibble())
  ev %>% mutate(
    venue = obj$venue %||% venue,
    event_date = suppressWarnings(as.Date(.data$date)),
    band_name  = as.character(.data$band),
    event_time = if ("time" %in% names(.)) as.character(.data$time) else NA_character_
  ) %>% filter(!is.na(event_date), nzchar(band_name))
}

# --- Facebook Graph helpers (Pages only)
fb_slug_from_url <- function(u){
  m <- stringr::str_match(u, "facebook\\.com/([^/?#]+)")
  if (is.na(m[1,2])) NA_character_ else m[1,2]
}
fb_page_id <- function(slug){
  if (!nzchar(FB_TOKEN) || !nzchar(slug)) return(NA_character_)
  url <- sprintf("https://graph.facebook.com/v19.0/%s?fields=id&access_token=%s", slug, FB_TOKEN)
  resp <- tryCatch(request(url) |> req_perform(), error=function(e) e)
  if (inherits(resp,"error") || resp_status(resp)>=300) return(NA_character_)
  tryCatch(resp_body_json(resp)$id %||% NA_character_, error=function(e) NA_character_)
}
fb_fetch_posts <- function(page_id, since_days = 120){
  if (!nzchar(FB_TOKEN) || !nzchar(page_id)) return(tibble())
  since_ts <- as.integer(as.POSIXct(Sys.Date()-since_days, tz="UTC"))
  url <- sprintf(
    "https://graph.facebook.com/v19.0/%s/posts?fields=message,created_time,permalink_url&since=%d&limit=50&access_token=%s",
    page_id, since_ts, FB_TOKEN
  )
  resp <- tryCatch(request(url) |> req_perform(), error=function(e) e)
  if (inherits(resp,"error") || resp_status(resp)>=300) return(tibble())
  rj <- tryCatch(resp_body_json(resp), error=function(e) NULL); if (is.null(rj)) return(tibble())
  dat <- rj$data
  if (is.null(dat) || !length(dat)) return(tibble())
  tibble(
    created_time = as.POSIXct(vapply(dat, function(x) x$created_time %||% NA_character_, character(1L)), tz="UTC"),
    message      = vapply(dat, function(x) x$message %||% "", character(1L)),
    permalink    = vapply(dat, function(x) x$permalink_url %||% "", character(1L))
  )
}

# --- Simple website scraper (plain text)
site_fetch_text <- function(url){
  if (!nzchar(url)) return("")
  resp <- tryCatch(request(url) |> req_perform(), error=function(e) e)
  if (inherits(resp,"error") || resp_status(resp)>=300) return("")
  doc <- tryCatch(read_html(resp_body_string(resp)), error=function(e) NULL)
  if (is.null(doc)) return("")
  txt <- doc %>% html_elements("body") %>% html_text2()
  stringr::str_squish(paste(txt, collapse="\n"))
}

# --- Main
if (!file.exists(sources_path)) {
  cat("[scrape_posts] No sources file at", sources_path, "-> nothing to do.\n")
  write_csv(tibble(), file.path(out_dir, "scraped_candidates.csv"))
  quit(status = 0)
}
sources <- suppressMessages(readr::read_csv(sources_path, show_col_types = FALSE)) %>%
  mutate(platform = tolower(platform))

out <- list()

# Facebook paths
if (nrow(sources %>% filter(platform=="facebook")) && nzchar(FB_TOKEN)) {
  fb_rows <- sources %>% filter(platform=="facebook")
  for (i in seq_len(nrow(fb_rows))){
    ven <- fb_rows$venue[i]; url <- fb_rows$url[i]
    slug <- fb_slug_from_url(url)
    pid  <- fb_page_id(slug)
    if (!nzchar(pid)) next
    posts <- fb_fetch_posts(pid)
    if (!nrow(posts)) next
    for (j in seq_len(nrow(posts))){
      txt <- posts$message[j] %||% ""
      if (!nzchar(txt)) next
      evs <- extract_from_post(txt, as.Date(posts$created_time[j]), ven)
      if (nrow(evs)) {
        evs$source_url <- posts$permalink[j]
        evs$platform   <- "facebook"
        out[[length(out)+1]] <- evs
      }
    }
  }
} else {
  if (!nzchar(FB_TOKEN)) cat("[scrape_posts] FB_GRAPH_TOKEN not set; skipping Facebook.\n")
}

# Website paths (very heuristic)
wb_rows <- sources %>% filter(platform=="website")
if (nrow(wb_rows)) {
  for (i in seq_len(nrow(wb_rows))){
    ven <- wb_rows$venue[i]; url <- wb_rows$url[i]
    txt <- site_fetch_text(url)
    if (!nzchar(txt)) next
    # Use "today" as publication date fallback for relative phrasing (best effort)
    evs <- extract_from_post(txt, Sys.Date(), ven)
    if (nrow(evs)) {
      evs$source_url <- url
      evs$platform   <- "website"
      out[[length(out)+1]] <- evs
    }
  }
}

res <- if (length(out)) bind_rows(out) else tibble()
outfile <- file.path(out_dir, "scraped_candidates.csv")
readr::write_csv(res, outfile)
cat("Wrote:", outfile, "rows:", nrow(res), "\n")
cat("Done.\n")
