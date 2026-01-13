# Unit tests

test_that("get_table requires valid connection", {
  expect_error(
    get_table("not a connection", "crsp", "msf"),
    "must be a database connection"
  )
})

test_that("get_table requires single character library", {
  wrds <- mock_connection()
  expect_error(
    get_table(wrds, c("comp", "crsp"), "msf"),
    "must be a single character string"
  )
  expect_error(
    get_table(wrds, 123, "msf"),
    "must be a single character string"
  )
})

test_that("get_table requires single character table", {
  wrds <- mock_connection()
  expect_error(
    get_table(wrds, "crsp", c("msf", "dsf")),
    "must be a single character string"
  )
  expect_error(
    get_table(wrds, "crsp", 123),
    "must be a single character string"
  )
})

# Integration tests

test_that("get_table returns lazy table by default", {
  skip_on_cran()
  skip_if_no_wrds()

  wrds <- wrds_connect()
  withr::defer(wrds_disconnect(wrds))

  result <- get_table(wrds, "crsp", "msf")
  expect_s3_class(result, "tbl_lazy")
})

test_that("get_table collects when lazy = FALSE", {
  skip_on_cran()
  skip_if_no_wrds()

  wrds <- wrds_connect()
  withr::defer(wrds_disconnect(wrds))

  result <- get_table(wrds, "crsp", "msf", n = 10, lazy = FALSE)
  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 10)
})

test_that("get_table respects columns argument", {
  skip_on_cran()
  skip_if_no_wrds()

  wrds <- wrds_connect()
  withr::defer(wrds_disconnect(wrds))

  cols <- c("permno", "date", "ret")
  result <- get_table(wrds, "crsp", "msf", columns = cols, n = 5, lazy = FALSE)

  expect_s3_class(result, "tbl_df")
  expect_true(all(names(result) %in% cols))
})

test_that("get_table works with dplyr verbs", {
  skip_on_cran()
  skip_if_no_wrds()

  wrds <- wrds_connect()
  withr::defer(wrds_disconnect(wrds))

  result <- get_table(wrds, "crsp", "msf") |>
    dplyr::filter(date >= "2023-01-01") |>
    dplyr::select(permno, date, ret) |>
    utils::head(10) |>
    dplyr::collect()

  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 10)
  expect_named(result, c("permno", "date", "ret"))
})
