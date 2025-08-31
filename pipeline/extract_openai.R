suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(tidyverse)
  library(glue)
})

img_to_data_uri <- function(path){
  ext <- tools::file_ext(path)
  mime <- ifelse(tolower(ext) %in% c("jpg","jpeg"), "image/jpeg", "image/png")
  raw <- readBin(path, what = "raw", n = file.info(path)$size)
  paste0("data:", mime, ";base64,", jsonlite::base64_enc(raw))
}

extract_openai_events <- function(image_path, venue_name, year, month){
  if (!nzchar(Sys.getenv("OPENAI_API_KEY"))) return(tibble())
  img <- img_to_data_uri(image_path)

  prompt <- glue(
"Devuelve SOLO JSON minificado con este esquema:
{{\"venue\":\"{venue_name}\",\"year\":{year},\"month\":{month},\"events\":[{{\"date\":\"YYYY-MM-DD\",\"band\":\"<artista/banda>\"}}]}}
Reglas: si el día no tiene mes/año explícitos, usa month={month}, year={year}. Ignora precios/horas/patrocinios. Sin markdown ni texto extra."
  )

  body <- list(
    model = "gpt-4o",
    input = list(list(
      role = "user",
      content = list(
        list(type = "input_text", text = prompt),
        list(type = "input_image", image_url = img)
      )
    ))
  )

  resp <- request("https://api.openai.com/v1/responses") |>
    req_headers(Authorization = paste("Bearer", Sys.getenv("OPENAI_API_KEY"))) |>
    req_body_json(body, auto_unbox = TRUE) |>
    req_perform()

  resj <- resp_body_json(resp)
  out_txt <- resj$output_text %||% (tryCatch(resj$choices[[1]]$message$content[[1]]$text, error = function(e) NA))
  if (is.na(out_txt)) return(tibble())
  parsed <- tryCatch(jsonlite::fromJSON(out_txt, simplifyVector = TRUE), error=function(e) NULL)
  if (is.null(parsed) || is.null(parsed$events)) return(tibble())

  tibble(
    venue = parsed$venue %||% venue_name,
    venue_id = NA_character_,
    event_date = suppressWarnings(as.Date(parsed$events$date)),
    band_name = parsed$events$band,
    source_image = image_path
  )
}
`%||%` <- function(a,b) if (!is.null(a)) a else b
