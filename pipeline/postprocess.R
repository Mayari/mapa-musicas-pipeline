#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(tidyverse); library(lubridate); library(readr)
})

cat(">> postprocess v1 (aggregations for both outputs)\n")

args <- commandArgs(trailingOnly = TRUE)
get_arg <- function(flag, default=NULL){ hit <- which(args==flag); if(length(hit) && hit < length(args)) args[hit+1] else default }
out_dir <- get_arg("--out_dir","data")

ensure_cols <- function(df){
  need <- c("venue","event_date","band_name","municipality","state")
  for (n in need) if (!n %in% names(df)) df[[n]] <- NA
  df
}

agg_write <- function(df, dir_out){
  dir.create(dir_out, showWarnings = FALSE, recursive = TRUE)
  if (!nrow(df)) {
    write_csv(tibble(), file.path(dir_out,"events_by_municipality.csv"))
    write_csv(tibble(), file.path(dir_out,"events_by_state.csv"))
    write_csv(tibble(), file.path(dir_out,"events_by_state_venue.csv"))
    return(invisible(NULL))
  }
  df2 <- df %>% mutate(year = year(event_date), month = month(event_date))
  muni <- df2 %>% count(municipality, year, month, name="events") %>% arrange(municipality, year, month)
  st   <- df2 %>% count(state, year, month, name="events") %>% arrange(state, year, month)
  st_v <- df2 %>% count(state, venue, year, month, name="events") %>% arrange(state, venue, year, month)
  write_csv(muni, file.path(dir_out,"events_by_municipality.csv"))
  write_csv(st,   file.path(dir_out,"events_by_state.csv"))
  write_csv(st_v, file.path(dir_out,"events_by_state_venue.csv"))
}

# ---- OpenAI output
oa_path <- file.path(out_dir, "performances_monthly.csv")
if (file.exists(oa_path)) {
  cat("[postprocess] OpenAI file found:", oa_path, "\n")
  oa <- suppressMessages(readr::read_csv(oa_path, show_col_types = FALSE)) %>% ensure_cols()
  agg_write(oa, file.path(out_dir, "aggregations"))
} else {
  cat("[postprocess] OpenAI file NOT found, writing empty aggregations.\n")
  agg_write(tibble(), file.path(out_dir, "aggregations"))
}

# ---- GVision output
gv_path <- file.path(out_dir, "performances_monthly_gvision.csv")
if (file.exists(gv_path)) {
  cat("[postprocess] GVision file found:", gv_path, "\n")
  gv <- suppressMessages(readr::read_csv(gv_path, show_col_types = FALSE)) %>% ensure_cols()
  agg_write(gv, file.path(out_dir, "aggregations_gvision"))
} else {
  cat("[postprocess] GVision file NOT found, writing empty aggregations.\n")
  agg_write(tibble(), file.path(out_dir, "aggregations_gvision"))
}

cat("Done.\n")
