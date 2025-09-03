# pipeline/extract_gvision.R
# v1.1 Google Vision OCR helper for MAPA músicas
# - Prefers API key via env GCP_VISION_API_KEY; falls back to service account (googleAuthR)
# - Tries DOCUMENT_TEXT_DETECTION first, then TEXT_DETECTION if very short, and picks the longer text
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

.do_annotate <- function(endpoint, auth_mode, token, img_b64, feature_type = "DOCUMENT_TEXT_DETECTION", language_hints = c("es","en")) {
  body <- list(
    requests = list(list(
      image = list(content = img_b64),
      features = list(list(type = feature_type)),
      imageContext = list(languageHints = unname(language_hints))
    ))
  )
  req <- request(endpoint) |>
    req_headers(`Content-Type` = "application/json") |>
    req_body_json(body, auto_unbox = TRUE)
  if (auth_mode == "oauth") req <- req_auth_bearer_token(req, token = token)
  resp <- req_perform(req)
  cont <- resp_body_json(resp, simplifyVector = TRUE)
  # fullTextAnnotation preferred; fallback to textAnnotations[1]
  txt <- tryCatch(cont$responses[[1]]$fullTextAnnotation$text %||% "", error = function(e) "")
  if (!nzchar(txt)) {
    txt <- tryCatch(cont$responses[[1]]$textAnnotations[[1]]$description %||% "", error = function(e) "")
  }
  txt
}

extract_gvision <- function(image_paths, language_hints = c("es","en"), feature = "DOCUMENT_TEXT_DETECTION") {
  if (length(image_paths) == 0) return(tibble(source_image = character(), ocr_text = character()))
  auth <- .gv_get_auth()

  endpoint <- "https://vision.googleapis.com/v1/images:annotate"
  if (auth$mode == "apikey") endpoint <- paste0(endpoint, "?key=", auth$key)

  results <- map_df(image_paths, function(pth) {
    cli::cli_alert("GV OCR: {basename(pth)}")
    size <- tryCatch(file.info(pth)$size, error = function(e) 0)
    if (is.na(size) || size <= 0) {
      cli::cli_alert_warning("File not readable or empty: {pth}")
      return(tibble(source_image = pth, ocr_text = ""))
    }

    img_bytes <- readBin(pth, what = "raw", n = size)
    img_b64   <- base64enc::base64encode(img_bytes)

    # First try: DOCUMENT_TEXT_DETECTION (good for dense/printed text
