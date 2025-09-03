# pipeline/extract_gvision.R
# v1.3 Google Vision OCR helper (mode switch + advanced OCR options + status checks + debug dumps)

suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(base64enc)
  library(cli)
  library(tibble)
  library(purrr)
})

`%||%` <- function(a, b) if (!is.null(a)) a else b
.debug_dir <- function() { getOption("mapa.debug_dir", NULL) }

.gv_get_auth <- function() {
  key <- Sys.getenv("GCP_VISION_API_KEY", "")
  if (nzchar(key)) {
    list(mode="apikey", key=key, token=NULL)
  } else {
    sa <- Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if (!nzchar(sa)) cli::cli_abort("No GCP_VISION_API_KEY or GOOGLE_APPLICATION_CREDENTIALS found.")
    if (!requireNamespace("googleAuthR", quietly = TRUE)) cli::cli_abort("Service-account auth requires googleAuthR.")
    googleAuthR::gar_auth_service(json_file = sa, scope = "https://www.googleapis.com/auth/cloud-platform")
    tok <- googleAuthR::gar_token()
    list(mode="oauth", key=NULL, token=tok$credentials$access_token)
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

.do_annotate <- function(endpoint, auth_mode, token, img_b64, feature_type, language_hints, text_params, tag) {
  body <- list(
    requests = list(list(
      image = list(content = img_b64),
      features = list(list(type = feature_type)),
      imageContext = list(languageHints = unname(language_hints), textDetectionParams = text_params)
    ))
  )
  req <- request(endpoint) |> req_headers(`Content-Type`="application/json") |> req_body_json(body, auto_unbox=TRUE)
  if (auth_mode == "oauth") req <- req_auth_bearer_token(req, token = token)

  resp   <- req_perform(req)
  status <- resp_status(resp)
  if (status >= 400) {
    msg <- tryCatch(resp_body_string(resp), error=function(e) "<no body>")
    cli::cli_alert_danger("Vision {feature_type} HTTP {status}: {msg}")
    if (!is.null(.debug_dir())) writeLines(msg, file.path(.debug_dir(), sprintf("vision_%s_error_%s.txt", tag, status)))
    return("")
  }

  cont <- resp_body_json(resp, simplifyVector = TRUE)
  if (!is.null(.debug_dir())) writeLines(jsonlite::toJSON(cont, auto_unbox=TRUE, pretty=TRUE),
                                         file.path(.debug_dir(), sprintf("vision_%s.json", tag)))

  txt <- tryCatch(cont$responses[[1]]$fullTextAnnotation$text %||% "", error = function(e) "")
  if (!nzchar(txt)) txt <- tryCatch(cont$responses[[1]]$textAnnotations[[1]]$description %||% "", error=function(e) "")
  txt
}

extract_gvision <- function(image_paths, language_hints = c("es","en"), feature = NULL) {
  if (length(image_paths) == 0) return(tibble(source_image=character(), ocr_text=character()))

  auth <- .gv_get_auth()
  endpoint <- "https://vision.googleapis.com/v1/images:annotate"
  if (auth$mode == "apikey") endpoint <- paste0(endpoint, "?key=", auth$key)

  mode <- tolower(Sys.getenv("GV_TEXT_MODE", "both"))
  if (!mode %in% c("auto","document","text","both")) mode <- "both"
  if (!is.null(feature) && mode == "auto") mode <- if (toupper(feature) == "TEXT_DETECTION") "text" else "document"

  text_params <- .build_text_params()

  results <- map_df(seq_along(image_paths), function(i) {
    pth <- image_paths[[i]]
    base <- basename(pth)
    cli::cli_alert("GV OCR ({mode}): {base}")

    size <- tryCatch(file.info(pth)$size, error=function(e) 0)
    if (is.na(size) || size <= 0) {
      cli::cli_alert_warning("File not readable or empty: {pth}")
      return(tibble(source_image = pth, ocr_text = ""))
    }
    img_b64 <- base64enc::base64encode(readBin(pth, what="raw", n=size))

    want_doc  <- mode %in% c("document","both")
    want_text <- mode %in% c("text","both")
    txt_doc <- txt_text <- ""

    if (want_doc)  txt_doc  <- .do_annotate(endpoint, auth$mode, auth$token, img_b64, "DOCUMENT_TEXT_DETECTION", language_hints, text_params, tag = paste0("doc_", base))
    if (want_text) txt_text <- .do_annotate(endpoint, auth$mode, auth$token, img_b64, "TEXT_DETECTION", language_hints, text_params, tag = paste0("text_", base))

    best <- if (mode == "document") txt_doc else if (mode == "text") txt_text else if (nchar(txt_text) > nchar(txt_doc)) txt_text else txt_doc
    if (nchar(best) < 80) cli::cli_alert_warning("Very short Vision OCR text for {base}")
    tibble(source_image = pth, ocr_text = best %||% "")
  })

  results
}

extract_google_vision <- extract_gvision
