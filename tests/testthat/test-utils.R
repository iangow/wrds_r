# Tests for smart_collect

test_that("smart_collect alerts before collecting large result sets", {
  # Track order of operations
  operations <- character()

  # Create a mock lazy table that records operations
  mock_tbl <- structure(list(), class = c("tbl_lazy", "tbl"))

  # Mock the pipeline: ungroup -> tally -> pull returns row count
  local_mocked_bindings(
    ungroup = function(x, ...) {
      operations <<- c(operations, "ungroup")
      x
    },
    tally = function(x, ...) {
      operations <<- c(operations, "tally")
      x
    },
    pull = function(x, ...) {
      operations <<- c(operations, "pull")
      150L # Return count exceeding threshold
    },
    collect = function(x, ...) {
      operations <<- c(operations, "collect")
      data.frame(x = seq_len(150))
    },
    .package = "dplyr"
  )

  withr::local_options(wrds.collect_threshold = 100L)

  # cli_alert_warning outputs to stderr, capture with expect_message
  expect_message(
    smart_collect(mock_tbl, wrds = NULL),
    "Query returns",
    class = "cliMessage"
  )

  # Verify count (pull) happens before collect
  pull_idx <- which(operations == "pull")[1]
  collect_idx <- which(operations == "collect")[1]
  expect_true(pull_idx < collect_idx)
})

test_that("smart_collect aborts for very large result sets without collecting", {
  operations <- character()
  mock_tbl <- structure(list(), class = c("tbl_lazy", "tbl"))

  local_mocked_bindings(
    ungroup = function(x, ...) {
      operations <<- c(operations, "ungroup")
      x
    },
    tally = function(x, ...) {
      operations <<- c(operations, "tally")
      x
    },
    pull = function(x, ...) {
      operations <<- c(operations, "pull")
      200L # Exceeds abort threshold
    },
    collect = function(x, ...) {
      operations <<- c(operations, "collect")
      data.frame(x = seq_len(200))
    },
    .package = "dplyr"
  )

  withr::local_options(
    wrds.collect_threshold = 100L,
    wrds.abort_threshold = 150L
  )

  expect_error(
    smart_collect(mock_tbl, wrds = NULL),
    "would return"
  )

  # Collect should NOT have been called
  expect_false("collect" %in% operations)
})

test_that("smart_collect returns lazy table immediately when lazy = TRUE", {
  operations <- character()
  mock_tbl <- structure(list(), class = c("tbl_lazy", "tbl"))

  local_mocked_bindings(
    ungroup = function(x, ...) {
      operations <<- c(operations, "ungroup")
      x
    },
    tally = function(x, ...) {
      operations <<- c(operations, "tally")
      x
    },
    pull = function(x, ...) {
      operations <<- c(operations, "pull")
      1000L
    },
    collect = function(x, ...) {
      operations <<- c(operations, "collect")
      data.frame()
    },
    .package = "dplyr"
  )

  result <- smart_collect(mock_tbl, wrds = NULL, lazy = TRUE)

  expect_s3_class(result, "tbl_lazy")
  # No operations should have been performed
  expect_length(operations, 0)
})

test_that("smart_collect collects without warning for small result sets", {
  mock_tbl <- structure(list(), class = c("tbl_lazy", "tbl"))

  local_mocked_bindings(
    ungroup = function(x, ...) x,
    tally = function(x, ...) x,
    pull = function(x, ...) 10L, # Small count
    collect = function(x, ...) data.frame(x = 1:10),
    .package = "dplyr"
  )

  withr::local_options(wrds.collect_threshold = 100L)

  expect_no_warning(
    result <- smart_collect(mock_tbl, wrds = NULL)
  )
  expect_s3_class(result, "data.frame")
})
