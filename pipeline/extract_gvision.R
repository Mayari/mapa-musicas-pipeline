# pipeline/extract_gvision.R
# v1.2 Google Vision OCR helper (mode switch + advanced OCR options)
# - Mode via env GV_TEXT_MODE: auto | document | text | both  (default: both)
# - Advanced options via env GV_ADVANCED_OCR (comma list, default: legacyLayout)
# - Enable TEXT_DETECTION confidences via env GV_TEXT_CONF=on
# - Returns tibble: source_image, ocr_text

suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(base64enc)
  library(cli)
  library(tibble)
  library(purrr)
})

`%||%` <- function(a, b) if (!is.null(a)) a else b

.gv_get_auth <- function() {
  key <- Sys.getenv("GCP_VISION_API_KEY", "")
  if (nzchar(key)) {
    list(mode = "apikey", key = key, token = NULL)
  } else {
    sa <- Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if (!nzchar(sa)) cli::cli_abort("No GCP_VISION_API_KEY or GOOGLE_APPLICATION_CREDENTIALS found.")
    if (!requireNamespace("googleAuthR", quietly = TRUE)) {
      cli::cli_abort("Service-account auth requires googleAuthR. Install it or provide GCP_VISION_API_KEY.")
    }
    googleAuthR::gar_auth_service(json_file = sa,
                                  scope = "https://www.googleapis.com/auth/cloud-platform")
    tok <- googleAuthR::gar_token()
    list(mode = "oauth", key = NULL, token = tok$credentials$access_token)
  }
}

.build_text_params <- function() {
  adv <- Sys.getenv("GV_ADVANCED_OCR", "legacyLayout")
  adv_list <- if (nzchar(trimws(adv))) strsplit(adv, ",")[[1]] else character(0)
  adv_list <- trimws(adv_list)
  list(
    enableTextDetectionConfidenceScore = identical(tolower(Sys.getenv("GV_TEXT_CONF")), "on"),
    advancedOcrOptions = unname(adv_list)
  )
}

.do_annotate <- function(endpoint, auth_mode, token, img_b64, feature_type,
                         language_hints = c("es","en"), text_params = NULL) {
  body <- list(
    requests = list(list(
      image = list(content = img_b64),
      features = list(list(type = feature_type)),
      imageContext = list(
        languageHints = unname(language_hints),
        textDetectionParams = text_params
      )
    ))
  )
  req <- request(endpoint) |>
    req_headers(`Content-Type` = "application/json") |>
    req_body_json(body, auto_unbox = TRUE)
  if (auth_mode == "oauth") req <- req_auth_bearer_token(req, token = token)
  resp <- req_perform(req)
  cont <- resp_body_json(resp, simplifyVector = TRUE)
  # Prefer fullTextAnnotation; fallback to textAnnotations[1]
  txt <- tryCatch(cont$responses[[1]]$fullTextAnnotation$text %||% "", error = function(e) "")
  if (!nzchar(txt)) {
    txt <- tryCatch(cont$responses[[1]]$textAnnotations[[1]]$description %||% "", error = function(e) "")
  }
  txt
}

# Public entrypoint
extract_gvision <- function(image_paths,
                            language_hints = c("es","en"),
                            feature = NULL) {
  if (length(image_paths) == 0) return(tibble(source_image = character(), ocr_text = character()))

  auth <- .gv_get_auth()
  endpoint <- "https://vision.googleapis.com/v1/images:annotate"
  if (auth$mode == "apikey") endpoint <- paste0(endpoint, "?key=", auth$key)

  # Decide mode
  mode <- tolower(Sys.getenv("GV_TEXT_MODE", "both"))
  if (!mode %in% c("auto","document","text","both")) mode <- "both"
  # If caller passed a single feature, honor it unless mode explicitly set
  if (!is.null(feature) && mode == "auto") {
    mode <- if (toupper(feature) == "TEXT_DETECTION") "text" else "document"
  }

  text_params <- .build_text_params()

  results <- map_df(image_paths, function(pth) {
    cli::cli_alert("GV OCR ({mode}): {basename(pth)}")

    size <- tryCatch(file.info(pth)$size, error = function(e) 0)
    if (is.na(size) || size <= 0) {
      cli::cli_alert_warning("File not readable or empty: {pth}")
      return(tibble(source_image = pth, ocr_text = ""))
    }
    img_b64 <- base64enc::base64encode(readBin(pth, what = "raw", n = size))

    # Strategy
    want_doc  <- mode %in% c("document","both")
    want_text <- mode %in% c("text","both")
    txt_doc <- txt_text <- ""

    if (want_doc) {
      txt_doc <- tryCatch(
        .do_annotate(endpoint, auth$mode, auth$token, img_b64,
                     feature_type = "DOCUMENT_TEXT_DETECTION",
                     language_hints = language_hints,
                     text_params = text_params),
        error = function(e) { cli::cli_alert_warning("DOCUMENT_TEXT_DETECTION failed: {e$message}"); "" }
      )
    }
    if (want_text) {
      txt_text <- tryCatch(
        .do_annotate(endpoint, auth$mode, auth$token, img_b64,
                     feature_type = "TEXT_DETECTION",
                     language_hints = language_hints,
                     text_params = text_params),
        error = function(e) { cli::cli_alert_warning("TEXT_DETECTION failed: {e$message}"); "" }
      )
    }

    # Pick the longer of the two; if only one was requested, that wins
    best <- if (mode == "document") txt_doc else if (mode == "text") txt_text else {
      if (nchar(txt_text) > nchar(txt_doc)) txt_text else txt_doc
    }

    if (nchar(best) < 80) cli::cli_alert_warning("Very short Vision OCR text for {basename(pth)}")
    tibble(source_image = pth, ocr_text = best %||% "")
  })

  results
}

# Compat alias
extract_google_vision <- extract_gvision
