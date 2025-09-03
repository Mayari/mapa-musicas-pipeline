# pipeline/extract_gvision.R
# v2.6.1 — Backward-compatible signature + robust text pickup + debug dumps
# Compatible with calls like:
#   gv(eff_df$preproc_path, language_hints = c("es","en"), feature = "DOCUMENT_TEXT_DETECTION")
#
# Behavior:
# - Accepts a character vector of image paths *or* a data.frame with
#   a 'preproc_path' or 'source_image' column.
# - Optional args `language_hints` and `feature` are honored (or overridden by env).
# - Pulls text from fullTextAnnotation$text OR textAnnotations[[1]]$description
#   (chooses the longer non-empty string).
# - Writes raw JSON and plain-text to ocr_debug/ if options(mapa.debug_dir) is set.
#
# ENV toggles (optional):
#   GV_TEXT_MODE     : "both" (default), "text", "document"  (overridden if `feature` arg is set)
#   GV_ADVANCED_OCR  : set to "legacy_layout" to enable heuristic layout
#   GV_TEXT_CONF     : "on" to request confidence scores
#   GV_LANG_HINTS    : CSV list like "es,en" (overridden by `language_hints` arg if provided)

suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(dplyr)
  library(purrr)
  library(stringr)
  library(glue)
  library(base64enc)
  library(tibble)
})

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0) b else a
.debug_dir <- function() getOption("mapa.debug_dir", NULL)

# ---------- helpers -----------------------------------------------------------

.as_paths <- function(x) {
  # Accept a vector of paths or a data.frame with preproc_path/source_image
  if (is.data.frame(x)) {
    if ("preproc_path" %in% names(x)) return(as.character(x$preproc_path))
    if ("source_image" %in% names(x)) return(as.character(x$source_image))
    stop("extract_gvision: data.frame input must contain 'preproc_path' or 'source_image'.")
  } else if (is.character(x)) {
    return(x)
  } else {
    stop("extract_gvision: expected character vector of paths or a data.frame.")
  }
}

.read_b64 <- function(path) {
  sz <- file.info(path)$size
  if (is.na(sz) || sz <= 0) return(NULL)
  base64enc::base64encode(readBin(path, what = "raw", n = sz))
}

# Prefer fullTextAnnotation$text; fallback to textAnnotations[[1]]$description.
.pick_vision_text <- function(resp_list) {
  r <- resp_list$responses[[1]]
  if (is.null(r)) return("")
  t1 <- r$fullTextAnnotation$text %||% ""
  t2 <- if (!is.null(r$textAnnotations) && length(r$textAnnotations) > 0) {
    r$textAnnotations[[1]]$description %||% ""
  } else ""
  txt <- c(t1, t2) |> keep(nzchar) |> unique()
  if (!length(txt)) return("")
  best <- txt[[which.max(nchar(txt, keepNA = FALSE))]]
  best |>
    str_replace_all("\\r\\n?", "\n") |>
    str_replace_all("[\u00A0]", " ") |>
    str_replace_all("[ \t]+", " ") |>
    str_squish()
}

# ---------- request construction ---------------------------------------------

.gv_modes_from_env_or_arg <- function(feature_arg = NULL) {
  # If caller passed a single feature, respect it; else use GV_TEXT_MODE
  if (is.character(feature_arg) && length(feature_arg) == 1) {
    ft <- toupper(feature_arg)
    if (ft %in% c("TEXT_DETECTION", "DOCUMENT_TEXT_DETECTION")) return(ft)
  }
  mode <- tolower(Sys.getenv("GV_TEXT_MODE", "both"))
  if (mode == "text")        return("TEXT_DETECTION")
  if (mode %in% c("doc","document")) return("DOCUMENT_TEXT_DETECTION")
  "BOTH"
}

.gv_lang_hints_from_env_or_arg <- function(language_hints = NULL) {
  if (!is.null(language_hints)) return(language_hints)
  raw <- Sys.getenv("GV_LANG_HINTS", "")
  if (nzchar(raw)) strsplit(raw, ",")[[1]] |> trimws() else NULL
}

.gvision_body <- function(b64, modes, lang_hints, adv_opt, want_conf) {
  feats <- list()
  if (identical(modes, "BOTH")) {
    feats <- list(list(type = "DOCUMENT_TEXT_DETECTION"), list(type = "TEXT_DETECTION"))
  } else {
    feats <- list(list(type = modes))
  }

  img_ctx <- list()
  if (!is.null(lang_hints) && length(lang_hints)) {
    img_ctx$languageHints <- lang_hints
  }

  # textDetectionParams flags
  tdp <- list()
  if (isTRUE(want_conf)) {
    # use both keys to maximize compatibility across Vision backends
    tdp$enableTextDetectionConfidenceScore <- TRUE
    tdp$enableTextConfidence <- TRUE
  }
  if (is.character(adv_opt) && identical(tolower(adv_opt), "legacy_layout")) {
    tdp$advancedOcrOptions <- list("legacy_layout")
  }
  if (length(tdp)) img_ctx$textDetectionParams <- tdp

  list(
    requests = list(list(
      image     = list(content = b64),
      features  = feats,
      imageContext = if (length(img_ctx)) img_ctx else NULL
    )))
}

# ---------- main function -----------------------------------------------------

extract_gvision <- function(source_image,
                            language_hints = NULL,
                            feature = NULL) {
  paths <- .as_paths(source_image)

  api_key <- Sys.getenv("GCP_VISION_API_KEY", "")
  if (!nzchar(api_key) && Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS", "") == "") {
    stop("Google Vision credentials missing: set GCP_VISION_API_KEY or GOOGLE_APPLICATION_CREDENTIALS.")
  }

  dbg <- .debug_dir()
  if (!is.null(dbg)) dir.create(dbg, showWarnings = FALSE, recursive = TRUE)

  modes      <- .gv_modes_from_env_or_arg(feature)
  lang_hints <- .gv_lang_hints_from_env_or_arg(language_hints)
  adv_opt    <- Sys.getenv("GV_ADVANCED_OCR", "")
  want_conf  <- tolower(Sys.getenv("GV_TEXT_CONF", "off")) %in% c("on","true","1","yes")

  cli::cli_alert_info("→ Google Vision OCR (modes={modes}) on {length(paths)} image(s)")

  results <- map_dfr(paths, function(p) {
    b64 <- .read_b64(p)
    if (is.null(b64)) {
      cli::cli_alert_warning("Vision: unreadable file {basename(p)}")
      return(tibble(source_image = p, ocr_text = "", provider = "vision", chars = 0))
    }

    body <- .gvision_body(b64, modes, lang_hints, adv_opt, want_conf)

    url <- if (nzchar(api_key)) {
      paste0("https://vision.googleapis.com/v1/images:annotate?key=", api_key)
    } else {
      "https://vision.googleapis.com/v1/images:annotate"
    }

    req <- request(url) |>
      req_method("POST") |>
      req_headers(`Content-Type` = "application/json") |>
      req_body_json(body, auto_unbox = TRUE)

    resp <- req_perform(req)
    js   <- tryCatch(resp_body_json(resp, simplifyVector = FALSE), error = function(e) NULL)

    if (!is.null(dbg)) {
      json_path <- file.path(dbg, paste0("vision_resp_", basename(p), ".json"))
      writeLines(if (is.null(js)) resp_body_string(resp) else jsonlite::toJSON(js, auto_unbox = TRUE, pretty = TRUE),
                 json_path, useBytes = TRUE)
    }

    txt <- if (is.null(js)) "" else .pick_vision_text(js)
    if (!is.null(dbg)) {
      txt_path <- file.path(dbg, paste0("vision_text_", tools::file_path_sans_ext(basename(p)), ".txt"))
      writeLines(txt, txt_path, useBytes = TRUE)
    }

    tibble(
      source_image = p,
      ocr_text     = txt %||% "",
      provider     = "vision",
      chars        = nchar(txt %||% "")
    )
  })

  results
}
