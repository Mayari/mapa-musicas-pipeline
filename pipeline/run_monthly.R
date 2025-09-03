# pipeline/run_monthly.R
# Minimal, Vision-only run: OCR → rules parser → validate → write

suppressPackageStartupMessages({
  library(readr); library(dplyr); library(stringr); library(lubridate)
  library(tidyr); library(cli); library(glue); library(purrr); library(tibble)
})

`%||%` <- function(a,b) if (!is.null(a)) a else b
cli::cli_h1(">> run_monthly v0.9.0 (Vision-only + rule parser + manual overrides)")

# ---- args ----
args <- commandArgs(trailingOnly = TRUE)
get_arg <- function(flag, default=NULL){ i <- which(args == flag); if (length(i)==0 || i==length(args)) return(default); args[i+1] }
images_dir  <- get_arg("--images_dir", "../carteleras")
venues_path <- get_arg("--venues_path", "data/venues.csv")
out_dir     <- get_arg("--out_dir", "data")
agg_dir     <- get_arg("--agg_dir", "data/aggregations")
debug_dir   <- get_arg("--debug_dir", NULL)

dir.create(out_dir, showWarnings=FALSE, recursive=TRUE)
dir.create(agg_dir, showWarnings=FALSE, recursive=TRUE)
if (!is.null(debug_dir)) dir.create(debug_dir, showWarnings=FALSE, recursive=TRUE)
if (!is.null(debug_dir)) options(mapa.debug_dir = debug_dir)

# ---- discover files ----
is_poster <- function(x) grepl("\\.(jpg|jpeg|png|pdf)$", x, ignore.case=TRUE)
poster_paths <- list.files(images_dir, pattern = NULL, recursive = TRUE, full.names = TRUE)
poster_paths <- poster_paths[is_poster(poster_paths)]
cli::cli_alert_info(glue("Discovered {length(poster_paths)} poster file(s) under: {images_dir}"))
if (length(poster_paths)>0) cli::cli_alert_info(glue("Example: {paste(head(poster_paths,3), collapse=' | ')}"))

# ---- parse filenames ----
parse_filename <- function(path) {
  b <- basename(path)
  m <- stringr::str_match(b, "^(.+?)_(\\d{4})([A-Za-zñÑ]+)_(\\d+)\\.(jpg|jpeg|png|pdf)$")
  if (is.na(m[1,1])) return(tibble(source_image=path, venue_guess=NA_character_, year=NA_integer_, month_name_es=NA_character_, file_num=NA_integer_))
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

# ---- PDF → PNG first page (if any) ----
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

# ---- optional preprocess (ImageMagick) ----
has_convert <- nzchar(Sys.which("convert"))
preproc_on  <- tolower(Sys.getenv("GV_PREPROCESS","on")) != "off" && has_convert
pre_dir <- if (is.null(debug_dir)) tempdir() else file.path(debug_dir, "preproc")
dir.create(pre_dir, showWarnings=FALSE, recursive=TRUE)

preproc_image_cli <- function(in_path, out_dir_, target_width = as.integer(Sys.getenv("GV_MAX_DIM","1600"))) {
  ext <- tolower(tools::file_ext(in_path)); if (!ext %in% c("jpg","jpeg","png")) return(in_path)
  if (!nzchar(Sys.which("convert"))) return(in_path)
  out <- file.path(out_dir_, basename(in_path))
  cmd <- sprintf("convert %s -auto-orient -colorspace Gray -filter Lanczos -resize %dx -deskew 40%% -unsharp 0x1+0.75+0.02 -strip %s",
                 shQuote(in_path), as.integer(target_width), shQuote(out))
  system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE)
  if (file.exists(out)) out else in_path
}

eff_df <- eff_df %>%
  mutate(preproc_path = if (preproc_on) purrr::map_chr(effective_path, ~ preproc_image_cli(.x, pre_dir)) else effective_path)

if (!is.null(debug_dir)) {
  readr::write_csv(eff_df %>% mutate(size_effective=file.info(effective_path)$size, size_preproc=file.info(preproc_path)$size),
                   file.path(debug_dir, "image_sizes.csv"))
}

# ---- OCR (Vision only) ----
source("pipeline/extract_gvision.R")
cli::cli_alert_info("Running Google Vision OCR…")
gv_df <- extract_gvision(eff_df$preproc_path) %>%
  rename(preproc_path = source_image) %>%
  mutate(nchar_ocr = nchar(ocr_text %||% ""))

ocr_df <- eff_df %>% left_join(gv_df, by = "preproc_path")
if (!is.null(debug_dir)) writeLines(capture.output(head(ocr_df, 3)), file.path(debug_dir, "ocr_head.txt"))

# ---- Parse OCR text (rules, no LLM) ----
source("pipeline/parse_text_rules.R")
cli::cli_alert_info("Parsing OCR text (rule-based)…")
parsed <- parse_ocr_df(ocr_df %>% transmute(source_image, ocr_text, year, month_name_es))

# per-image counts
per_image_counts <- eff_df %>% select(source_image) %>%
  left_join(parsed %>% group_by(source_image) %>% summarise(events=n(), .groups="drop"),
            by="source_image") %>% mutate(events = events %||% 0L)
cli::cli_alert_info("Per-image parsed events:"); print(per_image_counts, n=nrow(per_image_counts))

if (!is.null(debug_dir)) readr::write_csv(per_image_counts, file.path(debug_dir, "per_image_counts.csv"))

# ---- Validate + write ----
source("pipeline/validate.R")
cli::cli_alert_info("[validate.R] join metadata + manual overrides")
validated <- validate(extracted = parsed %>% mutate(event_time = as.character(event_time %||% "")),
                      meta = usable_meta,
                      venues_path = venues_path)

perf_path <- file.path(out_dir, "performances_monthly.csv")
dir.create(dirname(perf_path), showWarnings=FALSE, recursive=TRUE)
write_csv(validated$performances, perf_path)
cli::cli_alert_success(glue("Wrote: {perf_path}"))

dir.create(agg_dir, showWarnings=FALSE, recursive=TRUE)
write_csv(validated$agg_municipality, file.path(agg_dir, "events_by_municipality.csv"))
write_csv(validated$agg_state,       file.path(agg_dir, "events_by_state.csv"))
cli::cli_alert_success(glue("Wrote aggregations to: {agg_dir}"))
cli::cli_alert_success("Done.")
