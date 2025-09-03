# pipeline/extract_openai.R
# v3.1.0 Image-based fallback extractor (adds throttle)
suppressPackageStartupMessages({
  library(httr2); library(jsonlite); library(base64enc); library(cli)
  library(tibble); library(purrr); library(dplyr); library(lubridate); library(stringr)
})
`%||%` <- function(a,b) if (!is.null(a)) a else b
.debug_dir <- function() getOption("mapa.debug_dir", NULL)
.throttle <- function() {
  sl <- suppressWarnings(as.numeric(Sys.getenv("OPENAI_THROTTLE_SEC","0")))
  if (!is.na(sl) && sl > 0) Sys.sleep(sl)
}

.month_es_to_num <- function(m) {
  if (is.null(m) || is.na(m)) return(NA_integer_)
  m <- tolower(str_trim(m))
  dict <- c("enero"=1,"febrero"=2,"marzo"=3,"abril"=4,"mayo"=5,"junio"=6,
            "julio"=7,"agosto"=8,"septiembre"=9,"setiembre"=9,"octubre"=10,"noviembre"=11,"diciembre"=12)
  dict[[m]] %||% NA_integer_
}

.call_chat_fc <- function(key, model, data_url, user_text, system_msg, tag) {
  tool_schema <- list(
    type="function",
    `function`=list(
      name="return_items",
      description="Return extracted events as strictly typed JSON",
      parameters=list(
        type="object",
        properties=list(items=list(type="array", items=list(
          type="object",
          properties=list(
            date=list(type="string"),
            band=list(type="string"),
            time=list(type="string")
          ),
          required=list("date","band","time"),
          additionalProperties=FALSE
        ))),
        required=list("items"),
        additionalProperties=FALSE
      )
    )
  )
  req <- request("https://api.openai.com/v1/chat/completions") |>
    req_headers(Authorization=paste("Bearer", key), `Content-Type`="application/json") |>
    req_body_json(list(
      model=model, temperature=0, max_tokens=2000,
      messages=list(
        list(role="system", content=system_msg),
        list(role="user", content=list(
          list(type="text", text=user_text),
          list(type="image_url", image_url=list(url=data_url, detail="high"))
        ))
      ),
      tools=list(tool_schema),
      tool_choice=list(type="function", `function`=list(name="return_items"))
    ), auto_unbox=TRUE)
  resp <- req_perform(req)
  .throttle()
  list(status=resp_status(resp), body=resp_body_string(resp))
}

.call_responses <- function(key, model, data_url, user_text, system_msg, tag) {
  req <- request("https://api.openai.com/v1/responses") |>
    req_headers(Authorization=paste("Bearer", key), `Content-Type`="application/json") |>
    req_body_json(list(
      model=model, temperature=0, max_output_tokens=2000,
      response_format=list(type="json_object"),
      input=list(
        list(role="system", content=list(list(type="input_text", text=system_msg))),
        list(role="user",   content=list(
          list(type="input_text",  text=user_text),
          list(type="input_image", image_url=list(url=data_url))
        ))
      )
    ), auto_unbox=TRUE)
  resp <- req_perform(req)
  .throttle()
  list(status=resp_status(resp), body=resp_body_string(resp))
}

.parse_items <- function(raw) {
  raw2 <- gsub("^\\s*```(json)?\\s*", "", raw); raw2 <- gsub("\\s*```\\s*$", "", raw2)
  parsed <- tryCatch(jsonlite::fromJSON(raw2), error=function(e) NULL)
  if (is.null(parsed)) {
    open <- regexpr("\\{", raw2); closes <- gregexpr("\\}", raw2)[[1]]
    if (open[1] != -1 && length(closes)>0 && closes[1]!=-1) {
      maybe <- substr(raw2, open[1], closes[length(closes)])
      parsed <- tryCatch(jsonlite::fromJSON(maybe), error=function(e) NULL)
    }
  }
  if (is.null(parsed)) return(NULL)
  if (!is.null(parsed$items)) return(parsed$items)
  if (is.list(parsed) && !is.null(parsed[[1]]$date)) return(parsed)
  NULL
}

extract_openai <- function(image_paths, month_es, year, source_image_ids = NULL,
                           model = Sys.getenv("OPENAI_IMAGE_MODEL","gpt-4o")) {
  if (length(image_paths)==0) return(tibble(source_image=character(), event_date=as.Date(character()), band_name=character(), event_time=character()))
  key <- Sys.getenv("OPENAI_API_KEY",""); if (!nzchar(key)) cli::cli_abort("OPENAI_API_KEY not set.")
  if (is.null(source_image_ids)) source_image_ids <- image_paths

  out <- purrr::map2_dfr(seq_along(image_paths), image_paths, function(i, pth) {
    mo     <- month_es[i] %||% NA_character_
    yr     <- suppressWarnings(as.integer(year[i]))
    src    <- source_image_ids[i]
    mo_num <- .month_es_to_num(mo)

    ext <- tolower(tools::file_ext(pth))
    mime <- if (ext %in% c("jpg","jpeg")) "image/jpeg" else if (ext=="png") "image/png" else "image/*"
    size <- file.info(pth)$size
    if (is.na(size) || size<=0) return(tibble(source_image=src, event_date=as.Date(character()), band_name=character(), event_time=character()))
    b64 <- base64enc::base64encode(readBin(pth, "raw", n=size))
    data_url <- paste0("data:", mime, ";base64,", b64)

    assume <- if (!is.na(yr) && !is.na(mo_num)) sprintf("Assume month '%s' and year %d. Only include dates within that month/year.", mo, yr)
              else if (!is.na(mo_num))           sprintf("Assume month '%s'. Only include dates within that month.", mo)
              else "If uncertain about month/year, omit the row."

    system_msg <- "You extract structured gig listings from posters. Return only confident rows."
    user_text  <- paste(
      "Return JSON with items: date (YYYY-MM-DD), band (string), time (HH:MM or empty).",
      "If multiple bands share a date, return multiple items. Omit doubtful rows.",
      assume,
      sep="\n"
    )

    tag <- basename(pth)
    res1 <- tryCatch(.call_chat_fc(key, model, data_url, user_text, system_msg, tag), error=function(e) list(status=0, body=paste("ERR:",e$message)))
    if (!is.null(.debug_dir())) writeLines(res1$body, file.path(.debug_dir(), paste0("openai_img_chat_", tag, ".json")))
    items <- NULL
    if (res1$status>0) {
      cont1 <- tryCatch(jsonlite::fromJSON(res1$body), error=function(e) NULL)
      args_json <- tryCatch(cont1$choices[[1]]$message$tool_calls[[1]]$`function`$arguments, error=function(e) NULL)
      if (!is.null(args_json) && nzchar(args_json)) {
        args <- tryCatch(jsonlite::fromJSON(args_json), error=function(e) NULL)
        if (!is.null(args$items)) items <- args$items
      } else {
        raw <- tryCatch(cont1$choices[[1]]$message$content, error=function(e) "")
        items <- .parse_items(raw)
      }
    }
    if (is.null(items) || !length(items)) {
      res2 <- tryCatch(.call_responses(key, model, data_url, user_text, system_msg, tag), error=function(e) list(status=0, body=paste("ERR:",e$message)))
      if (!is.null(.debug_dir())) writeLines(res2$body, file.path(.debug_dir(), paste0("openai_img_resp_", tag, ".json")))
      cont2 <- tryCatch(jsonlite::fromJSON(res2$body), error=function(e) NULL)
      raw2  <- tryCatch(cont2$output_text, error=function(e) NULL) %||% ""
      if (!nzchar(raw2)) {
        raw2 <- tryCatch({
          parts <- cont2$output[[1]]$content
          idx <- which(vapply(parts, function(x) identical(x$type, "output_text"), logical(1)))
          if (length(idx)>0) parts[[idx[1]]]$text else ""
        }, error=function(e) "")
      }
      items <- .parse_items(raw2)
    }
    if (is.null(items) || !length(items)) {
      cli::cli_alert_warning("OpenAI image returned non-JSON or empty items for {basename(pth)}")
      return(tibble(source_image=src, event_date=as.Date(character()), band_name=character(), event_time=character()))
    }

    tibble::as_tibble(items) %>%
      transmute(
        source_image = src,
        event_date   = suppressWarnings(lubridate::as_date(.data$date)),
        band_name    = .data$band %||% "",
        event_time   = .data$time %||% ""
      ) %>%
      filter(!is.na(event_date) & nzchar(band_name)) %>%
      { out <- .; if (!is.na(mo_num)) out <- dplyr::filter(out, lubridate::month(event_date)==mo_num); if (!is.na(yr)) out <- dplyr::filter(out, lubridate::year(event_date)==yr); out }
  })
  out
}
