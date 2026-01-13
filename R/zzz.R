.onLoad <- function(libname, pkgname) {

op <- options()
op_wrds <- list(
    wrds.collect_threshold = 200000L,
    wrds.abort_threshold = 2000000L
  )
  toset <- !(names(op_wrds) %in% names(op))
  if (any(toset)) options(op_wrds[toset])
  invisible()
}

.onAttach <- function(libname, pkgname) {
  has_creds <- tryCatch(
    {
      keyring::key_get("wrds_user")
      TRUE
    },
    error = function(e) FALSE
  )

  if (!has_creds) {
    cli::cli_alert_danger(
      "wrds: No credentials found. Run {cli::col_cyan('wrds_set_credentials()')} to configure."
    )
  }
}
