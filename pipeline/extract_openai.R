suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(tidyverse)
  library(glue)
})

message("[extract_openai.R] v2025-08-31+rugged-fallbacks")

# ---------- config ----------
get_num <- function(x, fallback) {
  v <- suppressWarnings(as.numeric(Sys.getenv(x, unset = as.character(fallback))))
  if (is.null(v) || length(v) == 0 || is.na(v) || !is.finite(v)) return(fallback)
  v
}
default_model <- Sys.getenv("OPENAI_MODEL", unset = "gpt-4.1")  # you control via secret
throttle_sec  <- get_num("OPENAI_THROTTLE_SEC", 2)

`%||%` <- function(a,b) if (!is.null(a)) a else b

# ---------- utils ----------
img_to_data_uri <- function(path){
  if (!file.exists(path)) stop("Image not found: ", path)
  ext  <- tolower(tools::file_ext(path))
  mime <- ifelse(ext %in% c("jpg","jpeg"), "image/jpeg", "image/png")
  sz <- suppressWarnings(as.integer(file.info(path)$size))
  raw <- if (!is.na(sz) && sz > 0) readBin(path, "raw", n = sz) else {
    con <- file(path, "rb"); on.exit(close(con), add = TRUE); readBin(con, "raw", n = 1e8)
  }
  if (length(raw) == 0) stop("Zero-length image bytes for: ", path)
  paste0("data:", mime, ";base64,", jsonlite::base64_enc(raw))
}

# Return an httr2_response even when httr2 throws on 4xx
perform_safely <- function(req){
  obj <- tryCatch(req_perform(req), error = function(e) e)
  if (inherits(obj, "httr2_response")) return(obj)
  # httr2_http_error usually has $response
  resp <- tryCatch(obj$response, error=function(e) NULL)
  if (inherits(resp, "httr2_response")) return(resp)
  stop(obj)
}

should_retry_without_schema <- function(body_txt){
  if (is.null(body_txt) || !nzchar(body_txt)) return(FALSE)
  grepl("json_schema|response_format|schema.*not.*supported|invalid.*response_format", tolower(body_txt))
}

# ---------- main extractor ----------
extract_openai_events <- function(image_path, venue_name, year, month){
  key <- Sys.getenv("OPENAI_API_KEY"); if (!nzchar(key)) return(tibble())
  if (!is.null(throttle_sec) && is.finite(throttle_sec) && throttle_sec > 0) Sys.sleep(throttle_sec)

  img <- img_to_data_uri(image_path)

  sys <- "Eres un extractor. Responde SOLO con JSON válido que cumpla el formato pedido."
  rules <- glue(
"- 'band' es el artista/banda (no títulos genéricos como 'Miércoles de Salsa', 'Valentine's Jazz Day', 'Jam', etc. Esos van en 'event_title').
- Expande residencias: 'cada martes' / 'todos los miércoles' => un evento por CADA fecha de ese día en {month}/{year}.
- Si hay varios grupos y días (p.ej. 'A: 7 y 21 / B: 14 y 28'), asigna correctamente.
- Hora: detecta '8 pm', '20:30', '20 h', '20 hrs', '20:30h'. Devuelve HH:MM 24h si hay; si no, omite 'time'.
- Ignora precios/reservas/patrocinios."
  )
  user_prompt <- glue(
"Rellena este JSON:
{{\"venue\":\"{venue_name}\",\"year\":{year},\"month\":{month},\"events\":[{{\"date\":\"YYYY-MM-DD\",\"band\":\"<artista>\",\"event_title\":\"<opcional>\",\"time\":\"HH:MM\"}}]}}
Reglas:
{rules}"
  )

  schema <- list(
    type = "object",
    properties = list(
      venue  = list(type="string"),
      year   = list(type="integer"),
      month  = list(type="integer", minimum=1, maximum=12),
      events = list(
        type="array",
        items=list(
          type="object",
          properties=list(
            date        = list(type="string", pattern="^\\d{4}-\\d{2}-\\d{2}$"),
            band        = list(type="string"),
            event_title = list(type="string"),
            time        = list(type="string", pattern="^(?:[01]\\d|2[0-3]):[0-5]\\d$")
          ),
          required = list("date","band"),
          additionalProperties = FALSE
        )
      )
    ),
    required = list("events"),
    additionalProperties = FALSE
  )

  build_chat_body <- function(model, use_schema = TRUE){
    base <- list(
      model = model,
      messages = list(
        list(role="system", content=sys),
        list(role="user", content=list(
          list(type="text",      text=user_prompt),
          list(type="image_url", image_url=list(url=img))
        ))
      ),
      temperature = 0,
      max_tokens = 1200
    )
    if (use_schema) {
      base$response_format <- list(
        type = "json_schema",
        json_schema = list(name="EventsSchema", schema=schema, strict=TRUE)
      )
    } else {
      base$response_format <- list(type="json_object")
    }
    base
  }

  # Try sequence:
  # 1) chat + schema (model = default_model)
  # 2) chat + json_object (same model)
  # 3) chat + json_object (fallback model gpt-4o)
  # 4) responses API (fallback model gpt-4o)
  try_modes <- tibble::tibble(
    api = c("chat","chat","chat","responses"),
    model = c(default_model, default_model, "gpt-4o", "gpt-4o"),
    use_schema = c(TRUE, FALSE, FALSE, FALSE)
  )

  for (k in seq_len(nrow(try_modes))){
    api   <- try_modes$api[k]
    model <- try_modes$model[k]
    use_schema <- try_modes$use_schema[k]

    message(sprintf("[extract] try #%d api=%s model=%s schema=%s", k, api, model, use_schema))

    if (api == "chat") {
      body <- build_chat_body(model, use_schema = use_schema)
      req <- request("https://api.openai.com/v1/chat/completions") |>
        req_headers(Authorization = paste("Bearer", key), "Content-Type" = "application/json") |>
        req_body_json(body, auto_unbox = TRUE)
      resp <- perform_safely(req)
      st   <- resp_status(resp); message("[extract] http_status=", st)
      if (st >= 300){
        bt <- tryCatch(resp_body_string(resp), error=function(e) "")
        message("[extract] 4xx/5xx body: ", substr(bt, 1, 600))
        # If schema seems unsupported, fall back to json_object
        if (use_schema && should_retry_without_schema(bt)) next
        # Otherwise continue to next mode
        next
      }
      res <- resp_body_json(resp)
      out_txt <- tryCatch(res$choices[[1]]$message$content, error=function(e) NA_character_)
      if (is.null(out_txt) || length(out_txt)==0 || is.na(out_txt) || !nzchar(out_txt)) { next }
      parsed <- tryCatch(jsonlite::fromJSON(out_txt, simplifyVector = TRUE), error=function(e) NULL)
      if (is.null(parsed) || is.null(parsed$events)) { next }

      tib <- tibble(
        venue        = parsed$venue %||% venue_name,
        venue_id     = NA_character_,
        event_date   = suppressWarnings(as.Date(parsed$events$date)),
        band_name    = parsed$events$band,
        event_title  = parsed$events$event_title %||% NA_character_,
        event_time   = parsed$events$time %||% NA_character_,
        source       = paste0("openai:", api, ifelse(use_schema, "+schema", "")),
        source_image = image_path
      ) |> dplyr::filter(!is.na(event_date), nzchar(band_name))
      if (nrow(tib)) return(tib) else next
    }

    if (api == "responses") {
      # Responses API fallback (image+text)
      prompt <- glue(
'Devuelve SOLO JSON minificado con este esquema:
{{"venue":"{venue_name}","year":{year},"month":{month},"events":[{{"date":"YYYY-MM-DD","band":"<artista>","event_title":"<opcional>","time":"HH:MM"}}]}}
Reglas:
{rules}'
      )
      body <- list(
        model = model,
        input = list(list(
          role = "user",
          content = list(
            list(type = "input_text",  text = prompt),
            list(type = "input_image", image_url = img)
          )
        ))
      )
      req <- request("https://api.openai.com/v1/responses") |>
        req_headers(Authorization = paste("Bearer", key), "Content-Type" = "application/json") |>
        req_body_json(body, auto_unbox = TRUE)
      resp <- perform_safely(req)
      st   <- resp_status(resp); message("[extract] http_status=", st)
      if (st >= 300){
        bt <- tryCatch(resp_body_string(resp), error=function(e) "")
        message("[extract] 4xx/5xx body: ", substr(bt, 1, 600))
        next
      }
      res    <- resp_body_json(resp)
      out_txt <- res$output_text %||% (tryCatch(res$choices[[1]]$message$content[[1]]$text, error = function(e) NA_character_))
      if (is.null(out_txt) || length(out_txt)==0 || is.na(out_txt) || !nzchar(out_txt)) { next }
      parsed <- tryCatch(jsonlite::fromJSON(out_txt, simplifyVector = TRUE), error=function(e) NULL)
      if (is.null(parsed) || is.null(parsed$events)) { next }

      tib <- tibble(
        venue        = parsed$venue %||% venue_name,
        venue_id     = NA_character_,
        event_date   = suppressWarnings(as.Date(parsed$events$date)),
        band_name    = parsed$events$band,
        event_title  = parsed$events$event_title %||% NA_character_,
        event_time   = parsed$events$time %||% NA_character_,
        source       = paste0("openai:", api),
        source_image = image_path
      ) |> dplyr::filter(!is.na(event_date), nzchar(band_name))
      if (nrow(tib)) return(tib) else next
    }
  } # for

  # If all attempts returned nothing, return empty tibble
  tibble()
}
