suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
})

message("[extract_gvision_text.R] vBASE (Vision OCR via API key)")

`%||%` <- function(a,b) if (!is.null(a) && !is.na(a) && length(a)>0 && nzchar(a)) a else b

b64_file <- function(path){
  raw <- readBin(path, what = "raw", n = file.info(path)$size)
  jsonlite::base64_enc(raw)
}

gcv_ocr_text_api_key <- function(path, api_key){
  # Vision v1 REST with TEXT_DETECTION + language hints
  endpoint <- paste0("https://vision.googleapis.com/v1/images:annotate?key=", api_key)
  body <- list(
    requests = list(list(
      image = list(content = b64_file(path)),
      features = list(list(type = "TEXT_DETECTION")),
      imageContext = list(languageHints = list("es","en"))
    ))
  )
  req <- request(endpoint) |> req_headers(`Content-Type`="application/json") |> req_body_json(body, auto_unbox=TRUE)
  resp <- tryCatch(req_perform(req), error=function(e) e)
  if (inherits(resp, "error")) return("")
  st <- resp_status(resp)
  if (st < 200 || st >= 300) return("")
  rj <- tryCatch(resp_body_json(resp), error=function(e) NULL)
  if (is.null(rj) || is.null(rj$responses) || length(rj$responses)<1) return("")
  res <- rj$responses[[1]]
  txt <- res$fullTextAnnotation$text %||% (if (!is.null(res$textAnnotations) && length(res$textAnnotations)) res$textAnnotations[[1]]$description else "")
  txt %||% ""
}

# Public: returns plain OCR text or "" if unavailable
gcv_ocr_text <- function(path){
  api_key <- Sys.getenv("GCV_API_KEY")
  if (nzchar(api_key)) {
    return(gcv_ocr_text_api_key(path, api_key))
  }
  message("[gvision] No GCV_API_KEY provided; skipping Vision OCR for ", basename(path))
  ""
}
