#' @keywords internal
"_PACKAGE"
#
#' @importFrom rlang .env
#' @importFrom dplyr tbl
#' @importFrom dbplyr in_schema
NULL

# Global variables used in dplyr pipelines (NSE)
utils::globalVariables(c(
  "datadate", "datafmt", "gvkey", "linkenddt", "n", "popsrc", "score",
  "sic", "sic_header", "sich"
))
