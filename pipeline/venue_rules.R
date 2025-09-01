suppressPackageStartupMessages({
  library(tidyverse)
  library(jsonlite)
})

norm_venue_key <- function(x){
  x <- tolower(as.character(x))
  x <- gsub("_", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

load_venue_rules <- function(dir = "data/venue_rules"){
  if (!dir.exists(dir)) return(list())
  fns <- list.files(dir, pattern = "\\.json$", full.names = TRUE)
  if (!length(fns)) return(list())
  rules <- lapply(fns, function(fp){
    obj <- tryCatch(jsonlite::fromJSON(fp, simplifyVector = TRUE), error=function(e) NULL)
    if (is.null(obj)) return(NULL)
    if (is.null(obj$venue_key)) obj$venue_key <- tools::file_path_sans_ext(basename(fp))
    obj$venue_key <- norm_venue_key(obj$venue_key)
    obj
  })
  rules <- Filter(Negate(is.null), rules)
  out <- setNames(rules, vapply(rules, function(r) r$venue_key, character(1)))
  out
}
