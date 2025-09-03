# pipeline/extract_gvision.R
# v3.1.1 — separate TEXT vs DOCUMENT calls; remove unsupported fields; robust debug
suppressPackageStartupMessages({
  library(httr2); library(jsonlite); library(dplyr); library(purrr)
  library(stringr); library(base64enc); library(glue); library(tibble)
})

`%||%` <- function(a,b) if (is.null(a) || length(a)==0) b else a
.debug_dir <- function() getOption("mapa.debug_dir", NULL)

.pick_text <- function(js) {
  r <- js$responses[[1]]
  if (is.null(r)) return("")
  t1 <- r$fullTextAnnotation$text %||% ""
  t2 <- if (!is.null(r$textAnnotations) && length(r$textAnnotations)>0) r$textAnnotations[[1]]$description %||% "" else ""
  cand <- c(t1,t2) |> purrr::keep(nzchar) |> unique()
  if (!length(cand)) return("")
  txt <- cand[[which.max(nchar(cand, keepNA=FALSE))]]
  txt |>
    stringr::str_replace_all("\\r\\n?", "\n") |>
    stringr::str_replace_all("[\u00A0]", " ") |>
    stringr::str_replace_all("[ \t]+", " ") |>
    stringr::str_squish()
}

# Build a one-feature request body
.build_body <- function(b64, feature_type, lang_hints, adv_layout, want_conf) {
  img_ctx <- list()
  if (!is.null(lang_hints) && length(lang_hints)) img_ctx$languageHints <- lang_hints

  # Only include fields that are documented for classic v1
  tdp <- list()
  if (isTRUE(want_conf)) {
    # documented key
    tdp$enableTextDetectionConfidenceScore <- TRUE
  }
  if (identical(adv_layout, "legacy_layout")) {
    tdp$advancedOcrOptions <- list("legacy_layout")
  }
  if (length(tdp)) img_ctx$textDetectionParams <- tdp

  list(requests = list(list(
    image = list(content = b64),
    features = list(list(type = feature_type)),
    imageContext = if (length(img_ctx)) img_ctx else NULL
  )))
}

# Call Vision once
.call_vision <- function(b64, feature_type, lang_hints, adv_layout, want_conf, api_key, img_name) {
  body <- .build_body(b64, feature_type, lang_hints, adv_layout, want_conf)
  url <- if (nzchar(api_key)) paste0("https://vision.googleapis.com/v1/images:annotate?key=", api_key)
         else "https://vision.googleapis.com/v1/images:annotate"

  req <- request(url) |>
    req_headers(`Content-Type`="application/json") |>
    req_body_json(body, auto_unbox=TRUE)

  resp <- req_perform(req)

  # Write raw response (string) on any HTTP status for easier debugging
  if (!is.null(.debug_dir())) {
    fp <- file.path(.debug_dir(), paste0("vision_resp_", feature_type, "_", basename(img_name), ".json"))
    writeLines(resp_body_string(resp), fp, useBytes=TRUE)
  }

  # Parse JSON if possible; return NULL on parse error
  tryCatch(resp_body_json(resp, simplifyVector=FALSE), error=function(e) NULL)
}

extract_gvision <- function(image_paths) {
  if (length(image_paths)==0) return(tibble(source_image=character(), ocr_text=character(), chars=integer()))
  api_key <- Sys.getenv("GCP_VISION_API_KEY", "")
  if (!nzchar(api_key) && Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS","")== "") {
    stop("Vision credentials missing: set GCP_VISION_API_KEY or GOOGLE_APPLICATION_CREDENTIALS.")
  }

  mode <- tolower(Sys.getenv("GV_TEXT_MODE","both"))
  modes <- switch(mode,
                  "text"     = c("TEXT_DETECTION"),
                  "document" = c("DOCUMENT_TEXT_DETECTION"),
                  c("TEXT_DETECTION","DOCUMENT_TEXT_DETECTION"))

  adv   <- tolower(Sys.getenv("GV_ADVANCED_OCR",""))
  adv   <- if (identical(adv,"legacy_layout")) "legacy_layout" else NULL
  conf  <- tolower(Sys.getenv("GV_TEXT_CONF","off")) %in% c("on","true","1","yes")

  hints_env <- Sys.getenv("GV_LANG_HINTS","")
  lang_hints <- if (nzchar(hints_env)) strsplit(hints_env, ",")[[1]] |> trimws() else NULL

  purrr::map_dfr(image_paths, function(p) {
    sz <- file.info(p)$size; if (is.na(sz) || sz<=0) return(tibble(source_image=p, ocr_text="", chars=0))
    b64 <- base64enc::base64encode(readBin(p, "raw", n = sz))

    best_txt <- ""; best_len <- -1L

    for (ft in modes) {
      js <- .call_vision(b64, ft, lang_hints, adv, conf, api_key, p)
      txt <- if (is.null(js)) "" else .pick_text(js)
      if (!is.null(.debug_dir())) {
        writeLines(txt, file.path(.debug_dir(), paste0("vision_text_", ft, "_", tools::file_path_sans_ext(basename(p)), ".txt")), useBytes=TRUE)
      }
      if (nchar(txt) > best_len) {
        best_txt <- txt; best_len <- nchar(txt)
      }
    }

    tibble(source_image=p, ocr_text=best_txt, chars=nchar(best_txt))
  })
}
