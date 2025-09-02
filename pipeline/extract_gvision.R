# pipeline/extract_gvision.R
# Google Vision OCR helper for MAPA músicas
# - Prefers API key via env GCP_VISION_API_KEY
# - Falls back to OAuth service account via GOOGLE_APPLICATION_CREDENTIALS
# - Returns tibble: source_image, ocr_text
# - Exports BOTH names: extract_gvision() and extract_google_vision() for compatibility.

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
    if (!nzchar(sa)) {
      cli::cli_abort("No GCP_VISION_API_KEY or GOOGLE_APPLICATION_CREDENTIALS found.")
    }
    if (!requireNamespace("googleAuthR", quietly = TRUE)) {
      cli::cli_abort("Service-account auth requires googleAuthR. Install it or provide GCP_VISION_API_KEY.")
    }
    googleAuthR::gar_auth_service(json_file = sa,
                                  scope = "https://www.googleapis.com/auth/cloud-platform")
    tok <- googleAuthR::gar_token()
    list(mode = "oauth", key = NULL, token = tok$credentials$access_token)
  }
}

# Core implementation (returns tibble: source_image, ocr_text)
.gvision_impl <- function(image_paths, language_hints = c("es","en"),
                          feature = "DOCUMENT_TEXT_DETECTION") {
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

    body <- list(
      requests = list(list(
        image = list(content = img_b64),
        features = list(list(type = feature)),
        imageContext = list(languageHints = unname(language_hints))
      ))
    )

    req <- request(endpoint) |>
      req_headers(`Content-Type` = "application/json") |>
      req_body_json(body, auto_unbox = TRUE)

    if (auth$mode == "oauth") {
      req <- req_auth_bearer_token(req, token = auth$token)
    }

    resp <- tryCatch(req_perform(req), error = function(e) e)
    if (inherits(resp, "error")) {
      cli::cli_alert_danger("Vision request failed: {resp$message}")
      return(tibble(source_image = pth, ocr_text = ""))
    }

    cont <- resp_body_json(resp, simplifyVector = TRUE)
    txt <- tryCatch({
      cont$responses[[1]]$fullTextAnnotation$text %||% ""
    }, error = function(e) "")

    if (!nzchar(txt)) {
      tmp <- tryCatch(cont$responses[[1]]$textAnnotations[[1]]$description %||% "", error = function(e) "")
      txt <- tmp
    }

    if (nchar(txt) < 80) {
      cli::cli_alert_warning("Very short Vision OCR text for {basename(pth)}")
    }

    tibble(source_image = pth, ocr_text = txt)
  })

  results
}

# Public names (both)
extract_gvision <- function(image_paths, language_hints = c("es","en"),
                            feature = "DOCUMENT_TEXT_DETECTION") {
  .gvision_impl(image_paths, language_hints, feature)
}
extract_google_vision <- extract_gvision
