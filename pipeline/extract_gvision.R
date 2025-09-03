# pipeline/extract_gvision.R
# v2.1 Google Vision OCR helper (multi-variant, multi-mode, debug, confidences)
# Returns: tibble(source_image, ocr_text, feature_used, variant_used, chars)
# Env toggles:
#   GCP_VISION_API_KEY         : required if no GOOGLE_APPLICATION_CREDENTIALS
#   GV_TEXT_MODE               : "both" (default), "text", or "document"
#   GV_ADVANCED_OCR            : "legacy_layout" to enable heuristic layout
#   GV_TEXT_CONF               : "on" to enable confidence scores
#   GV_LANG_HINTS              : CSV list, e.g. "es,en" (empty = auto-detect)
#   GV_PREPROCESS              : "on" (default) | "off"   (preproc variants)
#   GV_MAX_DIM                 : max width for resize (default 1800)
# Debug:
#   options(mapa.debug_dir="ocr_debug") from run_monthly.R; saves vision_*.json

suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(base64enc)
  library(cli)
  library(dplyr)
  library(purrr)
  library(stringr)
  library(readr)
  library(tibble)
})

`%||%` <- function(a,b) if (!is.null(a)) a else b
.debug_dir <- function() getOption("mapa.debug_dir", NULL)

# --------- small helpers ----------
.b64_from_file <- function(path) {
  sz <- file.info(path)$size
  if (is.na(sz) || sz <= 0) return(NULL)
  base64enc::base64encode(readBin(path, "raw", n = sz))
}

# Minimal ImageMagick CLI preprocessing variants
#  - "raw"         : no changes
#  - "gray"        : grayscale + deskew + resize + unsharp
#  - "binary"      : grayscale + adaptive-threshold (high contrast)
#  - "contrast"    : modest contrast stretch
.preproc_variants <- function(path, outdir, max_dim = 1800L, enable = TRUE) {
  out <- list(raw = path)
  if (!enable) return(out)
  dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
  base <- tools::file_path_sans_ext(basename(path))

  # gray
  gray_out <- file.path(outdir, paste0(base, "_gray.png"))
  cmd_gray <- sprintf(
    "convert %s -auto-orient -colorspace Gray -filter Lanczos -resize %dx -deskew 40%% -unsharp 0x1+0.7+0.02 -strip %s",
    shQuote(path), as.integer(max_dim), shQuote(gray_out)
  )
  system(cmd_gray, ignore.stdout = TRUE, ignore.stderr = TRUE)
  if (file.exists(gray_out)) out$gray <- gray_out

  # binary (adaptive threshold)
  bin_out <- file.path(outdir, paste0(base, "_binary.png"))
  cmd_bin <- sprintf(
    "convert %s -auto-orient -colorspace Gray -resize %dx -adaptive-threshold 15x15+10%% -strip %s",
    shQuote(path), as.integer(max_dim), shQuote(bin_out)
  )
  system(cmd_bin, ignore.stdout = TRUE, ignore.stderr = TRUE)
  if (file.exists(bin_out)) out$binary <- bin_out

  # contrast stretch (keep color)
  con_out <- file.path(outdir, paste0(base, "_contrast.png"))
  cmd_con <- sprintf(
    "convert %s -auto-orient -filter Lanczos -resize %dx -contrast-stretch 2%%x2%% -unsharp 0x1+0.5+0.02 -strip %s",
    shQuote(path), as.integer(max_dim), shQuote(con_out)
  )
  system(cmd_con, ignore.stdout = TRUE, ignore.stderr = TRUE)
  if (file.exists(con_out)) out$contrast <- con_out

  out
}

# Score OCR text by length + presence of dates/times
.score_text <- function(txt) {
  if (is.null(txt) || !nzchar(txt)) return(0)
  n <- nchar(txt)
  # Spanish months & dd / dd.mm / dd/mm, hours like 21:00
  month_pat <- "(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)"
  date_pat  <- "(\\b\\d{1,2}\\s*(de)?\\s*%s\\b)|\\b\\d{1,2}[./-]\\d{1,2}\\b"
  date_pat  <- sprintf(date_pat, month_pat)
  time_pat  <- "\\b([01]?\\d|2[0-3])[:.][0-5]\\d\\b"
  # weight: length + 50*(date hits) + 30*(time hits)
  n + 50L*length(unlist(regmatches(txt, gregexpr(date_pat, txt, ignore.case=TRUE)))) +
    30L*length(unlist(regmatches(txt, gregexpr(time_pat, txt))))
}

# --------- main Vision call ----------
.gvision_annotate <- function(b64, feature_type = c("TEXT_DETECTION","DOCUMENT_TEXT_DETECTION"),
                              language_hints = NULL, adv_opt = NULL, conf = FALSE, api_key = NULL) {
  feature_type <- match.arg(feature_type)
  url <- if (nzchar(api_key %||% "")) {
    paste0("https://vision.googleapis.com/v1/images:annotate?key=", api_key)
  } else {
    # If ADC (service account) is configured, omit key and use auth header via httr2 auth (not implemented here).
    "https://vision.googleapis.com/v1/images:annotate"
  }

  img_ctx <- list()
  if (!is.null(language_hints) && length(language_hints) > 0) {
    # Note: for Latin scripts, Google says hints are *usually* unnecessary; keep configurable.  (docs)
    img_ctx$languageHints <- language_hints
  }

  tdp <- list()
  if (!is.null(adv_opt) && nzchar(adv_opt)) {
    # Valid option (as of docs): "legacy_layout"
    tdp$advancedOcrOptions <- list(adv_opt)
  }
  if (isTRUE(conf)) {
    tdp$enableTextDetectionConfidenceScore <- TRUE
  }
  if (length(tdp)) img_ctx$textDetectionParams <- tdp

  body <- list(
    requests = list(list(
      image = list(content = b64),
      features = list(list(type = feature_type)),
      imageContext = if (length(img_ctx)) img_ctx else NULL
    ))
  )
  req <- request(url) |>
    req_headers(`Content-Type` = "application/json") |>
    req_body_json(body, auto_unbox = TRUE)

  # If you need OAuth for ADC, you’d add req_auth_bearer_jwt(...) here.
  resp <- req_perform(req)
  status <- resp_status(resp)
  body_s <- resp_body_string(resp)
  list(status = status, body = body_s)
}

# Extract text (fullTextAnnotation or textAnnotations[1])
.extract_text_from_resp <- function(js) {
  if (is.null(js$responses) || length(js$responses) == 0) return("")
  r <- js$responses[[1]]
  txt <- r$fullTextAnnotation$text %||% ""
  if (!nzchar(txt)) {
    txt <- r$textAnnotations[[1]]$description %||% ""
  }
  if (!nzchar(txt)) txt <- ""
  txt
}

# --------- public function ----------
extract_gvision <- function(image_paths,
                            language_hints = c(),              # default: no hints (auto); set via GV_LANG_HINTS
                            feature = "DOCUMENT_TEXT_DETECTION" # kept for signature; real control via GV_TEXT_MODE
) {
  if (length(image_paths) == 0) return(tibble(source_image=character(), ocr_text=character()))
  api_key <- Sys.getenv("GCP_VISION_API_KEY", "")
  if (!nzchar(api_key) && Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS", "") == "") {
    cli::cli_alert_danger("Vision: no credentials (GCP_VISION_API_KEY or GOOGLE_APPLICATION_CREDENTIALS missing).")
    return(tibble(source_image=image_paths, ocr_text=""))
  }

  dbg <- .debug_dir()
  # Modes: both | text | document
  mode <- tolower(Sys.getenv("GV_TEXT_MODE", "both"))
  modes <- switch(mode,
                  "text" = c("TEXT_DETECTION"),
                  "document" = c("DOCUMENT_TEXT_DETECTION"),
                  c("TEXT_DETECTION","DOCUMENT_TEXT_DETECTION"))

  # Language hints (CSV) if provided
  lang_env <- trimws(Sys.getenv("GV_LANG_HINTS", ""))
  if (nzchar(lang_env)) {
    language_hints <- strsplit(lang_env, ",")[[1]] |> trimws()
  }

  adv_opt <- Sys.getenv("GV_ADVANCED_OCR", "")
  if (!nzchar(adv_opt)) adv_opt <- NULL  # only send if set (e.g., "legacy_layout")
  conf_on <- tolower(Sys.getenv("GV_TEXT_CONF","off")) == "on"

  pre_on  <- tolower(Sys.getenv("GV_PREPROCESS","on")) != "off"
  maxdim  <- suppressWarnings(as.integer(Sys.getenv("GV_MAX_DIM","1800"))); if (is.na(maxdim)) maxdim <- 1800L

  # For each image: make variants, then call every selected feature, pick best
  out <- purrr::map_dfr(image_paths, function(p) {
    # Prepare variants
    var_dir <- if (is.null(dbg)) tempdir() else file.path(dbg, "vision_variants")
    variants <- .preproc_variants(p, var_dir, max_dim = maxdim, enable = pre_on)
    # Always include raw first
    variant_order <- names(variants)

    best_txt <- ""
    best_score <- -Inf
    best_feature <- NA_character_
    best_variant <- NA_character_

    for (vname in variant_order) {
      vpath <- variants[[vname]]
      b64 <- .b64_from_file(vpath)
      if (is.null(b64)) next

      for (ftype in modes) {
        res <- .gvision_annotate(b64, feature_type = ftype,
                                 language_hints = language_hints,
                                 adv_opt = adv_opt,
                                 conf = conf_on,
                                 api_key = api_key)
        # Debug dump
        if (!is.null(dbg)) {
          fn <- paste0("vision_", ftype, "_", vname, "_", basename(p), ".json")
          writeLines(res$body, file.path(dbg, fn))
        }
        if (res$status >= 400) next

        js  <- tryCatch(jsonlite::fromJSON(res$body), error = function(e) NULL)
        txt <- if (is.null(js)) "" else .extract_text_from_resp(js)
        s   <- .score_text(txt)
        if (s > best_score) {
          best_score <- s; best_txt <- txt; best_feature <- ftype; best_variant <- vname
        }
      }
    }

    tibble(
      source_image = p,
      ocr_text = best_txt %||% "",
      feature_used = best_feature %||% NA_character_,
      variant_used = best_variant %||% NA_character_,
      chars = nchar(best_txt %||% "")
    )
  })

  out
}
