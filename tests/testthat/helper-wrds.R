# Test helpers for wrds package

#' Skip test if WRDS credentials are not available
#'
#' Checks if WRDS credentials are set in the keyring. Tests using this
#' skipper will pass on CRAN (where credentials are unavailable).
skip_if_no_wrds <- function() {
  has_creds <- tryCatch(
    {
      keyring::key_get("wrds_user")
      keyring::key_get("wrds_pw")
      TRUE
    },
    error = \(e) FALSE
  )

  testthat::skip_if_not(
    has_creds,
    "WRDS credentials not available (set via keyring)"
  )
}

#' Create a mock connection object for unit tests
mock_connection <- function() {
  structure(list(), class = c("PqConnection", "DBIConnection"))
}
