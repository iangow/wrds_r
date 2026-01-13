#' List available libraries
#'
#' Returns a character vector of available schema names (libraries) on WRDS.
#'
#' @param wrds A `DBIConnection` object returned by [wrds_connect()].
#'
#' @return A character vector of library names.
#'
#' @export
#' @examples
#' \dontrun{
#' wrds <- wrds_connect()
#' list_libraries(wrds)
#' wrds_disconnect(wrds)
#' }
list_libraries <- function(wrds) {
  check_connection(wrds)

  sql <- "
    SELECT DISTINCT table_schema
    FROM information_schema.tables
    WHERE table_type = 'VIEW'
    ORDER BY table_schema
  "

  result <- DBI::dbGetQuery(wrds, sql)
  result$table_schema
}

#' List tables in a library
#'
#' Returns a character vector of table names within a WRDS library (schema).
#'
#' @param wrds A `DBIConnection` object returned by [wrds_connect()].
#' @param library Character. The name of the library (schema) to query.
#'
#' @return A character vector of table names.
#'
#' @export
#' @examples
#' \dontrun{
#' wrds <- wrds_connect()
#' list_tables(wrds, "comp")
#' wrds_disconnect(wrds)
#' }
list_tables <- function(wrds, library) {
  check_connection(wrds)

  sql <- "
    SELECT DISTINCT table_name
    FROM information_schema.columns
    WHERE table_schema = $1
    ORDER BY table_name
  "

  result <- DBI::dbGetQuery(wrds, sql, params = list(library))
  result$table_name
}

#' Describe a table
#'
#' Displays a glimpse-like summary of a WRDS table showing column names
#' and types, similar to [dplyr::glimpse()].
#'
#' @param wrds A `DBIConnection` object returned by [wrds_connect()].
#' @param library Character. The name of the library (schema).
#' @param table Character. The name of the table.
#' @param n Integer. Number of sample rows to fetch for value preview.
#'   Default is 20. 
#' @param max_cols Integer. Maximum number of columns to display. Default is 25.
#'
#' @return Invisibly returns a list with components:
#' \describe{
#'   \item{columns}{A data frame with `column_name` and `data_type`}
#'   \item{nrow}{Row count}
#'   \item{sample}{A data frame with sample rows (if `n > 0`)}
#' }
#'
#' @export
#' @examples
#' \dontrun{
#' wrds <- wrds_connect()
#' describe_table(wrds, "comp", "funda")
#' wrds_disconnect(wrds)
#' }
describe_table <- function(wrds, library, table, n = 20, max_cols = 25) {
  check_connection(wrds)

  col_sql <- "
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2
    ORDER BY ordinal_position
  "
  columns <- DBI::dbGetQuery(wrds, col_sql, params = list(library, table))

  count_sql <- sprintf(
    "SELECT COUNT(*) AS n FROM %s.%s",
    DBI::dbQuoteIdentifier(wrds, library),
    DBI::dbQuoteIdentifier(wrds, table)
  )

  nrow <- tryCatch(
    DBI::dbGetQuery(wrds, count_sql)$n,
    error = \(e) NA_integer_
  )

  sample_data <- NULL
  if (n > 0) {
    sample_sql <- sprintf(
      "SELECT * FROM %s.%s LIMIT %d",
      DBI::dbQuoteIdentifier(wrds, library),
      DBI::dbQuoteIdentifier(wrds, table),
      n
    )
    sample_data <- tryCatch(
      DBI::dbGetQuery(wrds, sample_sql),
      error = \(e) NULL
    )
  }

  ncol <- nrow(columns)

  cli::cli_text("{.strong {library}.{table}}")
  cli::cli_text("Rows: {cli::col_blue(format(nrow, big.mark = ','))}")
  cli::cli_text("Columns: {cli::col_blue(ncol)}")

  type_map <- c(
    "character varying" = "chr",
    "text" = "chr",
    "double precision" = "dbl",
    "numeric" = "dbl",
    "real" = "dbl",
    "integer" = "int",
    "smallint" = "int",
    "bigint" = "int",
    "date" = "date",
    "timestamp without time zone" = "dttm",
    "timestamp with time zone" = "dttm",
    "boolean" = "lgl"
  )

  display_cols <- min(ncol, max_cols)
  max_name_width <- max(nchar(columns$column_name[seq_len(display_cols)]))
  type_width <- 6

  # "$ " + name + " " + type + " " + safety margin for ellipsis
  prefix_width <- 2 + max_name_width + 1 + type_width + 1
  available_width <- getOption("width", 80) - prefix_width - 5

  for (i in seq_len(display_cols)) {
    col_name <- columns$column_name[i]
    pg_type <- columns$data_type[i]
    r_type <- type_map[pg_type]
    if (is.na(r_type)) r_type <- pg_type

    padded_name <- format(col_name, width = max_name_width)
    padded_type <- format(paste0("<", r_type, ">"), width = type_width)

    if (!is.null(sample_data) && col_name %in% names(sample_data)) {
      preview <- format_preview(sample_data[[col_name]], r_type, available_width)
      cli::cli_text("$ {padded_name} {.emph {padded_type}} {preview}")
    } else {
      cli::cli_text("$ {padded_name} {.emph {padded_type}}")
    }
  }

  if (ncol > max_cols) {
    remaining <- ncol - max_cols
    cli::cli_text(cli::col_silver("# ... with {remaining} more column{?s}"))
  }

  invisible(list(columns = columns, nrow = nrow, sample = sample_data))
}

#' Format column values for preview display
#' @noRd
format_preview <- function(x, type, max_width = 50) {
  if (all(is.na(x))) {
    na_styled <- cli::col_red("NA")
    return(paste0(na_styled, ", ", na_styled, cli::col_silver(", ...")))
  }

  format_one <- function(val) {
    if (is.na(val)) {
      cli::col_red("NA")
    } else if (type == "chr") {
      s <- as.character(val)
      if (nchar(s) > 12) s <- paste0(substr(s, 1, 10), "\u2026")
      cli::col_silver(paste0("\"", s, "\""))
    } else if (type == "date") {
      cli::col_silver(format(val))
    } else if (type == "dttm") {
      cli::col_silver(format(val, "%Y-%m-%d %H:%M"))
    } else {
      cli::col_silver(format(val, trim = TRUE))
    }
  }

  parts <- character(0)
  current_width <- 0

  for (i in seq_along(x)) {
    val_styled <- format_one(x[[i]])
    val_plain <- cli::ansi_strip(val_styled)
    sep_width <- if (length(parts) == 0) 0 else 2

    if (current_width + sep_width + nchar(val_plain) > max_width - 1) {
      parts <- c(parts, cli::col_silver("\u2026"))
      break
    }

    parts <- c(parts, val_styled)
    current_width <- current_width + sep_width + nchar(val_plain)
  }

  cli::ansi_collapse(parts, sep = ", ", last = ", ")
}
