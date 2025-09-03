# pipeline/extract_gvision.R
# v3.0.0 — robust Vision text pickup (fullTextAnnotation OR textAnnotations[1])
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

extract_gvision <- function(image_paths) {
  if (length(image_paths)==0) return(tibble(source_image=character(), ocr_text=character(), chars=integer()))
  api_key <- Sys.getenv("GCP_VISION_API_KEY", "")
  if (!nzchar(api_key) && Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS","")== "") {
    stop("Vision credentials missing: set GCP_VISION_API_KEY or GOOGLE_APPLICATION_CREDENTIALS.")
  }

  modes <- tolower(Sys.getenv("GV_TEXT_MODE","both"))
  if (!modes %in% c("both","text","document")) modes <- "both"
  adv   <- tolower(Sys.getenv("GV_ADVANCED_OCR",""))
  conf  <- tolower(Sys.getenv("GV_TEXT_CONF","off")) %in% c("on","true","1","yes")
  hints_env <- Sys.getenv("GV_LANG_HINTS","")
  lang_hints <- if (nzchar(hints_env)) strsplit(hints_env, ",")[[1]] |> trimws() else NULL

  feats <- switch(modes,
                  "text"     = list(list(type="TEXT_DETECTION")),
                  "document" = list(list(type="DOCUMENT_TEXT_DETECTION")),
                  list(list(type="DOCUMENT_TEXT_DETECTION"), list(type="TEXT_DETECTION")))

  out <- purrr::map_dfr(image_paths, function(p) {
    sz <- file.info(p)$size; if (is.na(sz) || sz<=0) return(tibble(source_image=p, ocr_text="", chars=0))
    b64 <- base64enc::base64encode(readBin(p, "raw", n = sz))
    img_ctx <- list()
    if (!is.null(lang_hints) && length(lang_hints)) img_ctx$languageHints <- lang_hints
    tdp <- list()
    if (conf) {
      tdp$enableTextDetectionConfidenceScore <- TRUE
      tdp$enableTextConfidence <- TRUE
    }
    if (identical(adv, "legacy_layout")) tdp$advancedOcrOptions <- list("legacy_layout")
    if (length(tdp)) img_ctx$textDetectionParams <- tdp

    body <- list(requests = list(list(image=list(content=b64), features=feats,
                                      imageContext = if (length(img_ctx)) img_ctx else NULL)))
    url <- if (nzchar(api_key)) paste0("https://vision.googleapis.com/v1/images:annotate?key=", api_key)
           else "https://vision.googleapis.com/v1/images:annotate"

    resp <- request(url) |>
      req_headers(`Content-Type`="application/json") |>
      req_body_json(body, auto_unbox=TRUE) |>
      req_perform()

    js <- tryCatch(resp_body_json(resp, simplifyVector=FALSE), error=function(e) NULL)
    if (!is.null(.debug_dir())) {
      fp <- file.path(.debug_dir(), paste0("vision_resp_", basename(p), ".json"))
      writeLines(if (is.null(js)) resp_body_string(resp) else jsonlite::toJSON(js, auto_unbox=TRUE, pretty=TRUE), fp, useBytes=TRUE)
    }
    txt <- if (is.null(js)) "" else .pick_text(js)
    if (!is.null(.debug_dir())) {
      writeLines(txt, file.path(.debug_dir(), paste0("vision_text_", tools::file_path_sans_ext(basename(p)), ".txt")), useBytes=TRUE)
    }
    tibble(source_image=p, ocr_text=txt, chars=nchar(txt))
  })

  out
}
