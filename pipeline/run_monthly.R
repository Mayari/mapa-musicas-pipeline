# pipeline/run_monthly.R
# v3.1.2 Vision-first → text-only LLM → optional image LLM fallback + robust validate
suppressPackageStartupMessages({
  library(readr); library(dplyr); library/stringr; library(lubridate)
  library(tidyr); library(cli); library(glue); library(purrr); library(tibble)
})

`%||%` <- function(a, b) if (!is.null(a)) a else b

cli::cli_h1(">> run_monthly v3.1.2 (Vision-first → text-only LLM → optional image LLM fallback + robust validate)")

# ---------------- ARGS ----------------
args <- commandArgs(trailingOnly = TRUE)
get_arg <- function(flag, default = NULL) { i <- which(args == flag); if (length(i)==0 || i==length(args)) return(default); args[i+1] }
images_dir  <- get_arg("--images_dir", "../carteleras")
venues_path <- get_arg("--venues_path", "data/venues.csv")
out_dir     <- get_arg("--out_dir", "data")
agg_dir     <- get_arg("--agg_dir", "data/aggregations")
debug_dir   <- get_arg("--debug_dir", NULL)
dir.create(out_dir, showWarnings=FALSE, recursive=TRUE)
dir.create(agg_dir, showWarnings=FALSE, recursive=TRUE)
if (!is.null(debug_dir)) dir.create(debug_dir, showWarnings=FALSE, recursive=TRUE)

# Debug snapshot
if (!is.null(debug_dir)) {
  options(mapa.debug_dir = debug_dir)
  envdump <- c(
    sprintf("OCR_PROVIDER=%s", Sys.getenv("OCR_PROVIDER", "vision")),
    sprintf("GCP_VISION_API_KEY set? %s", ifelse(nzchar(Sys.getenv("GCP_VISION_API_KEY")), "yes","no")),
    sprintf("OPENAI_API_KEY set? %s", ifelse(nzchar(Sys.getenv("OPENAI_API_KEY")), "yes","no")),
    sprintf("OPENAI_TEXT_MODEL=%s", Sys.getenv("OPENAI_TEXT_MODEL", "gpt-4.1")),
    sprintf("OPENAI_IMAGE_MODEL=%s", Sys.getenv("OPENAI_IMAGE_MODEL", "gpt-4o")),
    sprintf("OPENAI_THROTTLE_SEC=%s", Sys.getenv("OPENAI_THROTTLE_SEC","0")),
    sprintf("GV_TEXT_MODE=%s", Sys.getenv("GV_TEXT_MODE", "both")),
    sprintf("GV_LANG_HINTS=%s", Sys.getenv("GV_LANG_HINTS", "")),
    sprintf("GV_ADVANCED_OCR=%s", Sys.getenv("GV_ADVANCED_OCR", "")),
    sprintf("GV_TEXT_CONF=%s", Sys.getenv("GV_TEXT_CONF", "off")),
    sprintf("GV_PREPROCESS=%s", Sys.getenv("GV_PREPROCESS", "on")),
    sprintf("EXTRACT_IMAGE_FALLBACK=%s", Sys.getenv("EXTRACT_IMAGE_FALLBACK", "off"))
  )
  writeLines(envdump, file.path(debug_dir, "env.txt"))
}

# ---------------- DISCOVER FILES ----------------
is_poster <- function(x) grepl("\\.(jpg|jpeg|png|pdf)$", x, ignore.case=TRUE)
poster_paths <- list.files(images_dir, pattern = NULL, recursive = TRUE, full.names = TRUE)
poster_paths <- poster_paths[is_poster(poster_paths)]
cli::cli_alert_info(glue("Discovered {length(poster_paths)} poster file(s) under: {images_dir}"))
if (length(poster_paths) > 0) cli::cli_alert_info(glue("Example: {paste(head(poster_paths, 3), collapse = ' | ')}"))

# ---------------- PARSE FILENAMES ----------------
parse_filename <- function(path) {
  b <- basename(path)
  m <- stringr::str_match(b, "^(.+?)_(\\d{4})([A-Za-zñÑ]+)_(\\d+)\\.(jpg|jpeg|png|pdf)$")
  if (is.na(m[1,1])) {
    return(tibble(source_image=path, venue_guess=NA_character_, year=NA_integer_, month_name_es=NA_character_, file_num=NA_integer_))
  }
  tibble(
    source_image  = path,
    venue_guess   = m[1,2],
    year          = as.integer(m[1,3]),
    month_name_es = m[1,4],
    file_num      = as.integer(m[1,5])
  )
}
meta_df <- purrr::map_df(poster_paths, parse_filename)
usable_meta <- meta_df %>% filter(!is.na(venue_guess), !is.na(year))
cli::cli_alert_info(glue("Parsed metadata rows (usable): {nrow(usable_meta)}"))

# ---------------- PDF → PNG FIRST PAGE ----------------
pdf_to_png_once <- function(pdf_path, out_dir_) {
  base <- tools::file_path_sans_ext(basename(pdf_path))
  out  <- file.path(out_dir_, paste0(base, "_page1"))
  cmd  <- sprintf("pdftoppm -png -f 1 -l 1 -singlefile %s %s", shQuote(pdf_path), shQuote(out))
  system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE)
  png_path <- paste0(out, ".png")
  if (file.exists(png_path)) png_path else pdf_path
}
eff_dir <- if (is.null(debug_dir)) tempdir() else file.path(debug_dir, "pdf_firstpage")
dir.create(eff_dir, showWarnings=FALSE, recursive=TRUE)

eff_df <- usable_meta %>%
  mutate(ext = tolower(tools::file_ext(source_image)),
         effective_path = ifelse(ext=="pdf", pdf_to_png_once(source_image, eff_dir), source_image)) %>%
  select(source_image, effective_path, year, month_name_es)

# ---------------- OPTIONAL PREPROCESS (ImageMagick) ----------------
has_convert <- nzchar(Sys.which("convert"))
preprocess_on <- tolower(Sys.getenv("GV_PREPROCESS", "on")) != "off" && has_convert
if (!has_convert && !is.null(debug_dir)) writeLines("convert not found; skipping preprocess", file.path(debug_dir, "preprocess_note.txt"))

preproc_image_cli <- function(in_path, out_dir_, target_width = 1600L) {
  ext <- tolower(tools::file_ext(in_path)); if (!ext %in% c("jpg","jpeg","png")) return(in_path)
  if (!nzchar(Sys.which("convert"))) return(in_path)
  out <- file.path(out_dir_, basename(in_path))
  cmd <- sprintf("convert %s -auto-orient -colorspace Gray -filter Lanczos -resize %dx -deskew 40%% -unsharp 0x1+0.75+0.02 -strip %s",
                 shQuote(in_path), as.integer(target_width), shQuote(out))
  system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE)
  if (file.exists(out)) out else in_path
}
pre_dir <- if (is.null(debug_dir)) tempdir() else file.path(debug_dir, "preproc")
dir.create(pre_dir, showWarnings=FALSE, recursive=TRUE)

eff_df <- eff_df %>%
  mutate(preproc_path = if (preprocess_on) purrr::map_chr(effective_path, ~ preproc_image_cli(.x, pre_dir)) else effective_path)

if (!is.null(debug_dir)) {
  readr::write_csv(eff_df %>% mutate(size_effective=file.info(effective_path)$size,
                                     size_preproc  =file.info(preproc_path)$size),
                   file.path(debug_dir, "image_sizes.csv"))
}

# ---------------- PROVIDER (Vision) ----------------
provider <- tolower(Sys.getenv("OCR_PROVIDER", "vision"))
has_vision <- nzchar(Sys.getenv("GCP_VISION_API_KEY")) || nzchar(Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
if (provider == "auto") provider <- if (has_vision) "vision" else "openai"
cli::cli_alert_info(glue("OCR provider selection: {provider} (vision_cred={has_vision})"))

# ---------------- OCR: GOOGLE VISION ----------------
source("pipeline/extract_gvision.R")
gv_df <- extract_gvision(eff_df$preproc_path) %>%
  rename(preproc_path = source_image) %>%
  mutate(nchar_ocr = nchar(ocr_text %||% ""))

# Optional: write a quick sample of text into debug_dir
if (!is.null(debug_dir) && nrow(gv_df) > 0) {
  for (i in seq_len(min(3, nrow(gv_df)))) {
    writeLines(substr(gv_df$ocr_text[i] %||% "", 1, 500),
               file.path(debug_dir, paste0("vision_preview_", i, ".txt")))
  }
}

ocr_df <- eff_df %>% left_join(gv_df, by = "preproc_path")

# ---------------- EXTRACT (TEXT-ONLY LLM) ----------------
source("pipeline/extract_openai_text.R")
cli::cli_alert_info("[extract_openai_text.R] strict-month (band+date+time only)")

llm_in <- ocr_df %>% filter(nchar_ocr > 0) %>% transmute(source_image, ocr_text, month_name_es, year)
from_text <- if (nrow(llm_in) > 0) extract_openai_text(llm_in) else
  tibble(source_image=character(), event_date=as.Date(character()), band_name=character(), event_time=character())

# ---------------- OPTIONAL IMAGE-LLM FALLBACK ----------------
use_img_fallback <- tolower(Sys.getenv("EXTRACT_IMAGE_FALLBACK","off")) %in% c("on","true","1","yes")
from_image <- tibble(source_image=character(), event_date=as.Date(character()), band_name=character(), event_time=character())

if (use_img_fallback) {
  no_rows <- eff_df$source_image[!(eff_df$source_image %in% from_text$source_image)]
  if (length(no_rows) > 0 && nzchar(Sys.getenv("OPENAI_API_KEY",""))) {
    source("pipeline/extract_openai.R")
    idx <- match(no_rows, eff_df$source_image)
    oi <- eff_df[idx, ]
    cli::cli_alert_info(glue("OpenAI image fallback for {nrow(oi)} poster(s)."))
    from_image <- extract_openai(image_paths=oi$preproc_path, month_es=oi$month_name_es, year=oi$year, source_image_ids=oi$source_image)
  }
}

# ---------------- COMBINE ----------------
all_extracted <- bind_rows(from_text, from_image) %>%
  distinct(source_image, event_date, band_name, event_time, .keep_all = TRUE)

# Ensure every poster appears at least once in per-image counts
all_extracted <- eff_df %>% select(source_image) %>% distinct() %>% left_join(all_extracted, by="source_image")

per_image_counts <- all_extracted %>%
  group_by(source_image) %>%
  summarise(events = sum(!is.na(event_date) & nzchar(coalesce(band_name,""))), .groups="drop")
cli::cli_alert_info("Per-image extracted events:"); print(per_image_counts, n = nrow(per_image_counts))

# ---------------- VALIDATE & WRITE ----------------
source("pipeline/validate.R")
cli::cli_alert_info("[validate.R] robust join + manual overrides + venue metadata")
validated <- validate(extracted=all_extracted, meta=usable_meta, venues_path=venues_path, images_dir=images_dir)

perf_path <- file.path(out_dir, "performances_monthly.csv")
dir.create(dirname(perf_path), showWarnings=FALSE, recursive=TRUE)
write_csv(validated$performances, perf_path)
cli::cli_alert_success(glue("Wrote: {perf_path}"))

dir.create(agg_dir, showWarnings=FALSE, recursive=TRUE)
write_csv(validated$agg_municipality, file.path(agg_dir, "events_by_municipality.csv"))
write_csv(validated$agg_state,       file.path(agg_dir, "events_by_state.csv"))
cli::cli_alert_success(glue("Wrote aggregations to: {agg_dir}"))
cli::cli_alert_success("Done.")
