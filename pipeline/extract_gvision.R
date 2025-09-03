# pipeline/extract_gvision.R
# v2.6.0 — robust text pickup (fullTextAnnotation$text OR textAnnotations[[1]]$description)
suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(dplyr)
  library(purrr)
  library(stringr)
  library(glue)
  library(base64enc)
  library(fs)
})

# --- helpers -----------------------------------------------------------------

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0) b else a

.read_bytes_b64 <- function(path) {
  readBin(path, what = "raw", n = file.size(path)) |> base64enc::base64encode()
}

.gv_lang_hints <- function() {
  # e.g. "es,en" -> c("es","en")
  raw <- Sys.getenv("GV_LANG_HINTS", "")
  if (nzchar(raw)) str_split(raw, ",", simplify = TRUE) |> as.character() |> trimws()
  else NULL
}

.gv_text_mode <- function() {
  mode <- tolower(Sys.getenv("GV_TEXT_MODE", "both"))
  if (!mode %in% c("text", "doc", "both")) mode <- "both"
  mode
}

.gv_enable_conf <- function() {
  tolower(Sys.getenv("GV_TEXT_CONF", "off")) %in% c("on","true","1","yes")
}

# Prefer fullTextAnnotation$text; fall back to textAnnotations[[1]]$description.
# If both exist, pick the longer; trim and normalize whitespace.
.extract_text_from_vision <- function(resp) {
  r <- resp$responses[[1]]
  if (is.null(r)) return("")

  txt1 <- r$fullTextAnnotation$text %||% ""
  txt2 <- if (!is.null(r$textAnnotations) && length(r$textAnnotations) > 0) {
    # IMPORTANT: the first entry is the full text in a single string
    r$textAnnotations[[1]]$description %||% ""
  } else ""

  txt <- c(txt1, txt2) |> keep(~ nzchar(.x)) |> unique()
  if (length(txt) == 0) return("")
  # choose longest non-empty candidate
  best <- txt[[which.max(nchar(txt, keepNA = FALSE))]]
  best |>
    str_replace_all("\\r\\n?", "\n") |>
    str_replace_all("[ \t]+", " ") |>
    str_replace_all("[\u00A0]", " ") |>
    str_squish()
}

# Build the Vision annotate request for one image
.build_request_body <- function(b64) {
  feats <- list()
  mode <- .gv_text_mode()
  if (mode %in% c("doc","both")) feats <- append(feats, list(list(type = "DOCUMENT_TEXT_DETECTION")))
  if (mode %in% c("text","both")) feats <- append(feats, list(list(type = "TEXT_DETECTION")))

  img_ctx <- list()
  hints <- .gv_lang_hints()
  if (!is.null(hints)) img_ctx$languageHints <- hints
  if (.gv_enable_conf()) {
    # request confidence scores (field varies by API generation; supported in classic v1 via textDetectionParams)
    img_ctx$textDetectionParams <- list(enableTextConfidence = TRUE, advancedOcrOptions = c("legacy_layout"))
  }

  list(
    requests = list(
      list(
        image = list(content = b64),
        features = feats,
        imageContext = img_ctx
      )
    )
  )
}

.call_vision <- function(img_path, api_key) {
  b64 <- .read_bytes_b64(img_path)
  body <- .build_request_body(b64)

  req <- request(glue("https://vision.googleapis.com/v1/images:annotate?key={api_key}")) |>
    req_method("POST") |>
    req_headers(`Content-Type` = "application/json") |>
    req_body_json(body, auto_unbox = TRUE)

  resp <- req_perform(req)
  resp |> resp_body_json(simplifyVector = FALSE)
}

# --- main entry --------------------------------------------------------------

# poster_df must include a column 'source_image'
# returns tibble: source_image, ocr_text, provider, char_n
extract_gvision <- function(poster_df, debug_dir = "ocr_debug") {
  if (!dir_exists(debug_dir)) dir_create(debug_dir, recurse = TRUE)

  api_key <- Sys.getenv("GCP_VISION_API_KEY", "")
  if (!nzchar(api_key)) {
    stop("GCP_VISION_API_KEY is not set.")
  }

  mode <- .gv_text_mode()
  cli::cli_alert_info("→ Google Vision OCR ({mode}) on {nrow(poster_df)} image(s)")

  map_dfr(seq_len(nrow(poster_df)), function(i) {
    img <- poster_df$source_image[[i]]
    base <- fs::path_ext_remove(fs::path_file(img))

    # call API
    resp <- tryCatch(.call_vision(img, api_key), error = function(e) e)

    if (inherits(resp, "error")) {
      cli::cli_alert_warning("GV error for {fs::path_file(img)}: {resp$message}")
      return(tibble(
        source_image = img,
        ocr_text = "",
        provider = "vision",
        char_n = 0
      ))
    }

    # write debug JSON + plaintext
    json_path <- fs::path(debug_dir, glue("vision_doc_{base}.json"))
    writeLines(jsonlite::toJSON(resp, auto_unbox = TRUE, pretty = TRUE), json_path, useBytes = TRUE)

    text <- .extract_text_from_vision(resp)
    txt_path <- fs::path(debug_dir, glue("vision_text_{base}.txt"))
    writeLines(text, txt_path, useBytes = TRUE)

    tibble(
      source_image = img,
      ocr_text = text,
      provider = "vision",
      char_n = nchar(text, keepNA = FALSE)
    )
  })
}
