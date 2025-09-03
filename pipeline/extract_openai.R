# pipeline/extract_openai.R
# v2.5.1 Minimal direct image extraction using OpenAI (strict month/year)
# - PRIMARY: Chat Completions with function-calling (tool_call) → strict JSON args
# - FALLBACK: Responses API (json_object)
# - Saves raw API bodies (and any text we tried to parse) into ocr_debug/
# - Optional throttling via OPENAI_THROTTLE_SEC

suppressPackageStartupMessages({
  library(httr2); library(jsonlite); library(base64enc); library(cli)
  library(tibble); library(purrr); library(dplyr); library(lubridate); library(stringr)
})

`%||%` <- function(a, b) if (!is.null(a)) a else b
.debug_dir <- function() getOption("mapa.debug_dir", NULL)

.month_es_to_num <- function(m) {
  if (is.null(m) || is.na(m)) return(NA_integer_)
  m <- tolower(str_trim(m))
  dict <- c("enero"=1,"febrero"=2,"marzo"=3,"abril"=4,"mayo"=5,"junio"=6,
            "julio"=7,"agosto"=8,"septiembre"=9,"setiembre"=9,"octubre"=10,"noviembre"=11,"diciembre"=12)
  dict[[m]] %||% NA_integer_
}

.strip_code_fences <- function(x) {
  x <- gsub("^\\s*```(json)?\\s*", "", x)
  gsub("\\s*```\\s*$", "", x)
}
.json_substring <- function(txt) {
  if (!nzchar(txt)) return(NULL)
  # try to grab the largest JSON object starting at first '{' and ending at last '}'
  open <- regexpr("\\{", txt)
  closes <- gregexpr("\\}", txt)[[1]]
  if (open[1] == -1 || length(closes) == 0 || closes[1] == -1) return(NULL)
  substr(txt, open[1], closes[length(closes)])
}
.parse_items <- function(raw) {
  if (is.null(raw) || !nzchar(raw)) return(NULL)
  raw2 <- .strip_code_fences(raw)
  parsed <- tryCatch(jsonlite::fromJSON(raw2), error = function(e) NULL)
  if (is.null(parsed)) {
    maybe <- .json_substring(raw2)
    if (!is.null(maybe)) parsed <- tryCatch(jsonlite::fromJSON(maybe), error=function(e) NULL)
  }
  if (is.null(parsed)) return(NULL)
  if (!is.null(parsed$items)) return(parsed$items)
  if (is.list(parsed) && !is.null(parsed[[1]]) && !is.null(parsed[[1]]$date)) return(parsed)
  NULL
}
.throttle <- function() {
  sl <- suppressWarnings(as.numeric(Sys.getenv("OPENAI_THROTTLE_SEC", "0")))
  if (!is.na(sl) && sl > 0) Sys.sleep(sl)
}

# --- PRIMARY: Chat Completions with function-calling ---
.call_chat_fc <- function(key, model, data_url, user_text, system_msg, tag) {
  tool_schema <- list(
    type = "function",
    function = list(
      name = "return_items",
      description = "Return extracted events as strictly typed JSON",
      parameters = list(
        type = "object",
        properties = list(
          items = list(
            type = "array",
            items = list(
              type = "object",
              properties = list(
                date = list(type = "string", description = "YYYY-MM-DD"),
                band = list(type = "string"),
                time = list(type = "string", description = "24h HH:MM or empty")
              ),
              required = list("date","band","time"),
              additionalProperties = FALSE
            )
          )
        ),
        required = list("items"),
        additionalProperties = FALSE
      )
    )
  )

  req <- request("https://api.openai.com/v1/chat/completions") |>
    req_headers(Authorization = paste("Bearer", key), `Content-Type` = "application/json") |>
    req_body_json(list(
      model = model,
      temperature = 0,
      max_tokens = 2000,               # ↑ bigger budget to avoid truncation
      messages = list(
        list(role = "system", content = system_msg),
        list(role = "user", content = list(
          list(type = "text", text = user_text),
          list(type = "image_url", image_url = list(url = data_url, detail = "high"))
        ))
      ),
      tools = list(tool_schema),
      tool_choice = list(type = "function", `function` = list(name = "return_items"))
    ), auto_unbox = TRUE)

  resp <- req_perform(req)
  status <- resp_status(resp); body <- resp_body_string(resp)
  if (!is.null(.debug_dir())) writeLines(body, file.path(.debug_dir(), paste0("openai_chat_", tag, ".json")))
  list(status = status, body = body)
}

# --- FALLBACK: Responses API ---
.call_responses <- function(key, model, data_url, user_text, system_msg, tag) {
  req <- request("https://api.openai.com/v1/responses") |>
    req_headers(Authorization = paste("Bearer", key), `Content-Type` = "application/json") |>
    req_body_json(list(
      model = model,
      temperature = 0,
      max_output_tokens = 2000,        # ↑ bigger budget
      response_format = list(type = "json_object"),
      input = list(
        list(role = "system", content = list(list(type = "input_text", text = system_msg))),
        list(role = "user",   content = list(
          list(type = "input_text",  text = user_text),
          list(type = "input_image", image_url = list(url = data_url))
        ))
      )
    ), auto_unbox = TRUE)

  resp <- req_perform(req)
  status <- resp_status(resp); body <- resp_body_string(resp)
  if (!is.null(.debug_dir())) writeLines(body, file.path(.debug_dir(), paste0("openai_resp_", tag, ".json")))
  list(status = status, body = body)
}

extract_openai <- function(image_paths, month_es, year, source_image_ids = NULL,
                           model = Sys.getenv("OPENAI_IMAGE_MODEL", unset = "gpt-4o")) {
  if (length(image_paths) == 0) {
    return(tibble(source_image=character(), event_date=as.Date(character()), band_name=character(), event_time=character()))
  }
  key <- Sys.getenv("OPENAI_API_KEY", ""); if (!nzchar(key)) cli::cli_abort("OPENAI_API_KEY not set.")
  if (is.null(source_image_ids)) source_image_ids <- image_paths

  out_rows <- purrr::map2_dfr(seq_along(image_paths), image_paths, function(i, pth) {
    mo     <- month_es[i] %||% NA_character_
    yr     <- suppressWarnings(as.integer(year[i]))
    src    <- source_image_ids[i]
    mo_num <- .month_es_to_num(mo)

    # Build data URL from file path (preprocessed path is passed in)
    ext  <- tolower(tools::file_ext(pth))
    mime <- if (ext %in% c("jpg","jpeg")) "image/jpeg" else if (ext=="png") "image/png" else "image/*"
    size <- file.info(pth)$size
    if (is.na(size) || size <= 0) {
      cli::cli_alert_warning("OpenAI image: unreadable file {pth}")
      return(tibble(source_image = src, event_date = as.Date(character()), band_name = character(), event_time = character()))
    }
    bytes <- readBin(pth, what="raw", n=size)
    b64   <- base64enc::base64encode(bytes)
    data_url <- paste0("data:", mime, ";base64,", b64)

    assume_line <- if (!is.na(yr) && !is.na(mo_num)) {
      sprintf("Assume the month is '%s' (Spanish) and the year is %d. Only include dates within that month/year; omit anything outside.", mo, yr)
    } else if (!is.na(mo_num)) {
      sprintf("Assume the month is '%s' (Spanish). Only include dates within that month.", mo)
    } else {
      "Use the visible month/year; if uncertain, omit the row."
    }

    system_msg <- "You extract structured gig listings directly from venue posters. Be conservative and omit doubtful rows."
    user_text  <- paste(
      "Extract only confident events.",
      assume_line,
      "Return items with keys: date (YYYY-MM-DD), band (string), time (HH:MM or empty).",
      "If time is missing, use empty string. If unsure about a row, omit it.",
      sep = "\n"
    )
    tag <- basename(pth)

    # 1) Chat with function-calling
    res1 <- tryCatch(.call_chat_fc(key, model, data_url, user_text, system_msg, tag),
                     error = function(e) list(status=0, body=paste("CLIENT_ERROR:", e$message)))
    items <- NULL
    if (res1$status > 0) {
      cont1 <- tryCatch(jsonlite::fromJSON(res1$body, simplifyVector = TRUE), error=function(e) NULL)
      args_json <- tryCatch(cont1$choices[[1]]$message$tool_calls[[1]]$function$arguments, error=function(e) NULL)
      if (!is.null(args_json) && nzchar(args_json)) {
        args <- tryCatch(jsonlite::fromJSON(args_json), error=function(e) NULL)
        if (!is.null(args$items)) items <- args$items
      } else {
        raw <- tryCatch(cont1$choices[[1]]$message$content, error=function(e) "")
        if (!is.null(.debug_dir()) && nzchar(raw))
          writeLines(raw, file.path(.debug_dir(), paste0("openai_chat_content_", tag, ".txt")))
        items <- .parse_items(raw)
      }
      if (res1$status >= 400) cli::cli_alert_danger("OpenAI chat HTTP {res1$status} for {basename(pth)}")
    }

    # 2) Fallback: Responses API
    if (is.null(items) || length(items) == 0) {
      res2 <- tryCatch(.call_responses(key, model, data_url, user_text, system_msg, tag),
                       error = function(e) list(status=0, body=paste("CLIENT_ERROR:", e$message)))
      cont2 <- tryCatch(jsonlite::fromJSON(res2$body, simplifyVector = TRUE), error=function(e) NULL)
      raw2 <- ""
      if (!is.null(cont2)) {
        raw2 <- tryCatch(cont2$output_text, error=function(e) NULL)
        if (is.null(raw2)) {
          raw2 <- tryCatch({
            parts <- cont2$output[[1]]$content
            idx <- which(vapply(parts, function(x) identical(x$type, "output_text"), logical(1)))
            if (length(idx) > 0) parts[[idx[1]]]$text else ""
          }, error=function(e) "")
        }
      }
      if (!is.null(.debug_dir()) && nzchar(raw2))
        writeLines(raw2, file.path(.debug_dir(), paste0("openai_resp_content_", tag, ".txt")))
      items <- .parse_items(raw2)
      if (res2$status >= 400) cli::cli_alert_danger("OpenAI responses HTTP {res2$status} for {basename(pth)}")
    }

    if (is.null(items) || length(items) == 0) {
      cli::cli_alert_warning("OpenAI image returned non-JSON or empty items for {basename(pth)}")
      .throttle()
      return(tibble(source_image = src, event_date = as.Date(character()), band_name = character(), event_time = character()))
    }

    items_df <- as_tibble(items)
    if (!nrow(items_df)) {
      .throttle()
      return(tibble(source_image = src, event_date = as.Date(character()), band_name = character(), event_time = character()))
    }

    out <- items_df %>%
      transmute(
        source_image = src,
        event_date   = suppressWarnings(lubridate::as_date(.data$date)),
        band_name    = .data$band %||% "",
        event_time   = .data$time %||% ""
      ) %>%
      filter(!is.na(event_date) & nzchar(band_name))

    # Strict month/year post-filter
    if (!is.na(mo_num)) out <- out %>% filter(lubridate::month(event_date) == mo_num)
    if (!is.na(yr))     out <- out %>% filter(lubridate::year(event_date)  == yr)

    .throttle()
    out
  })

  out_rows
}
