# pipeline/extract_gvision.R
# Vision-only OCR with optional TEXT/DOCUMENT modes; picks longest text.

suppressPackageStartupMessages({
  library(httr2); library(jsonlite); library(base64enc)
  library(dplyr); library(purrr); library(stringr); library(tibble)
})

`%||%` <- function(a,b) if (is.null(a) || length(a)==0) b else a
.debug_dir <- function() getOption("mapa.debug_dir", NULL)

.pick_text <- function(js) {
  r <- js$responses[[1]]
  if (is.null(r)) return("")
  t1 <- r$fullTextAnnotation$text %||% ""
  t2 <- if (!is.null(r$textAnnotations) && length(r$textAnnotations)>0) r$textAnnotations[[1]]$description %||% "" else ""
  cand <- unique(c(t1, t2))
  cand <- cand[nchar(cand) > 0]
  if (!length(cand)) return("")
  cand[[which.max(nchar(cand))]]
}

.build_body <- function(b64, feature_type, lang_hints=NULL) {
  img_ctx <- list()
  if (!is.null(lang_hints) && length(lang_hints)) img_ctx$languageHints <- lang_hints
  list(requests = list(list(
    image = list(content = b64),
    features = list(list(type = feature_type)),
    imageContext = if (length(img_ctx)) img_ctx else NULL
  )))
}

.call_vision <- function(b64, feature_type, lang_hints, api_key, tag) {
  body <- .build_body(b64, feature_type, lang_hints)
  url <- paste0("https://vision.googleapis.com/v1/images:annotate?key=", api_key)
  req <- request(url) |>
    req_headers(`Content-Type`="application/json") |>
    req_body_json(body, auto_unbox=TRUE)
  resp <- req_perform(req)

  # write raw body for debug
  if (!is.null(.debug_dir())) {
    fn <- file.path(.debug_dir(), sprintf("vision_resp_%s_%s.json", feature_type, tag))
    writeLines(resp_body_string(resp), fn, useBytes=TRUE)
  }
  resp_body_json(resp, simplifyVector = FALSE)
}

extract_gvision <- function(image_paths) {
  if (!length(image_paths)) return(tibble(source_image=character(), ocr_text=character(), chars=integer()))

  api_key <- Sys.getenv("GCP_VISION_API_KEY","")
  if (!nzchar(api_key)) stop("Missing GCP_VISION_API_KEY")

  mode <- tolower(Sys.getenv("GV_TEXT_MODE","document"))
  modes <- switch(mode,
                  "text"     = c("TEXT_DETECTION"),
                  "document" = c("DOCUMENT_TEXT_DETECTION"),
                  c("TEXT_DETECTION","DOCUMENT_TEXT_DETECTION"))
  hints_env <- Sys.getenv("GV_LANG_HINTS","")
  lang_hints <- if (nzchar(hints_env)) strsplit(hints_env, ",")[[1]] |> trimws() else NULL

  purrr::map_dfr(image_paths, function(p) {
    if (!file.exists(p)) return(tibble(source_image=p, ocr_text="", chars=0))
    raw <- readBin(p, "raw", n=file.info(p)$size)
    b64 <- base64enc::base64encode(raw)
    tag <- tools::file_path_sans_ext(basename(p))

    best <- ""
    for (ft in modes) {
      js <- .call_vision(b64, ft, lang_hints, api_key, tag)
      txt <- .pick_text(js) %||% ""
      # write cleaned text for debug
      if (!is.null(.debug_dir())) {
        fn <- file.path(.debug_dir(), sprintf("vision_text_%s_%s.txt", ft, tag))
        writeLines(txt, fn, useBytes=TRUE)
      }
      if (nchar(txt) > nchar(best)) best <- txt
    }

    # normalize whitespace
    best <- best |>
      stringr::str_replace_all("\\r\\n?", "\n") |>
      stringr::str_replace_all("[\u00A0]", " ") |>
      stringr::str_replace_all("[ \t]+", " ") |>
      stringr::str_squish()

    tibble(source_image=p, ocr_text=best, chars=nchar(best))
  })
}
