###############################################################################
# Massachusetts Boil Water Notices
#
# Massachusetts BWN data is scraped from the MassDEP's Public Health Orders page
# which provides a full archive of every order issued with termination dates
# filled in for lifted orders.
###############################################################################

#' Pull Massachusetts BWN data.
#' @param config Main config
#' @param dataset_id "raw_ma_bwn"
run_ma_bwn_pipeline <- function(config, dataset_id) {
  update_raw_ma_bwn(config, dataset_id)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Poll #ajaxresponse until it contains the given text and returns its HTML.
#' The popup uses an async AJAX postback, so it can't be read right after
#' the triggering click.
#' @param b A chromote session
#' @param expect_text Text that must appear in the popup before it's considered ready
#' @param timeout_sec Seconds to wait before giving up
#' @param poll_interval_sec Seconds between polls
#' @return The popup's innerHTML once expect_text appears
wait_for_ma_popup <- function(b, expect_text, timeout_sec = 15, poll_interval_sec = 0.25) {
  deadline <- Sys.time() + timeout_sec
  repeat {
    popup_html <- b$Runtime$evaluate(
      "document.getElementById('ajaxresponse').innerHTML"
    )$result$value
    if (!is.null(popup_html) && grepl(expect_text, popup_html, fixed = TRUE)) {
      return(popup_html)
    }
    if (Sys.time() > deadline) {
      stop(sprintf("Timed out waiting for MA BWN popup to contain '%s'.", expect_text),
           call. = FALSE)
    }
    Sys.sleep(poll_interval_sec)
  }
}

#' Wait for the popup to close so it doesn't cover the next row's link. Fails
#' quietly instead of stopping the scrape.
#' @param b A chromote session
#' @param timeout_sec Seconds to wait before stopping attempt
#' @param poll_interval_sec Seconds between polls
wait_for_ma_popup_closed <- function(b, timeout_sec = 5, poll_interval_sec = 0.25) {
  deadline <- Sys.time() + timeout_sec
  repeat {
    still_open <- b$Runtime$evaluate(
      "!!document.querySelector('.pwsinfo')"
    )$result$value
    if (!isTRUE(still_open)) return(invisible(TRUE))
    if (Sys.time() > deadline) return(invisible(FALSE))
    Sys.sleep(poll_interval_sec)
  }
}

#' Scrape the current grid page's advisory table and each row's popup.
#' @param b A chromote session that's already on the search results page
#' @return Data frame
scrape_ma_bwn_grid_page <- function(b) {
  table_html <- b$Runtime$evaluate(
    "document.getElementById('ctl00_ContentPlaceHolder1_UpdatePanel1').innerHTML"
  )$result$value

  page_table <- rvest::read_html(table_html) %>%
    rvest::html_table(fill = TRUE) %>%
    purrr::pluck(1) %>%
    janitor::clean_names()

  # Drop rows with just page numbers
  page_rows <- page_table %>%
    dplyr::filter(!grepl("[0-9]", status)) %>%
    dplyr::select(city_town:date_order_terminated)

  if (nrow(page_rows) == 0) return(page_rows %>% dplyr::mutate(pwsid_raw = character()))

  pwsid_link_ids <- b$Runtime$evaluate(
    "JSON.stringify(Array.from(document.querySelectorAll('a[id*=\"lnkbtnPWS\"]')).map(a => a.id))"
  )$result$value %>% jsonlite::fromJSON()

  # Click each pwsid link and read its popup for the supplier name + pwsid.
  pwsid_raw <- vapply(pwsid_link_ids, function(link_id) {
    # Clear stale content first so the poll won't read the previous row's popup.
    b$Runtime$evaluate(
      "var el = document.getElementById('ajaxresponse'); if (el) el.innerHTML = '';"
    )
    b$Runtime$evaluate(sprintf("document.getElementById('%s').click()", link_id))
    popup_html <- wait_for_ma_popup(b, "PWS ID#:")
    popup_text <- rvest::read_html(popup_html) %>%
      rvest::html_table(fill = TRUE) %>%
      purrr::pluck(1) %>%
      as.data.frame()
    pwsid_i <- popup_text[1, 1]

    # Close the popup then wait for it to clear.
    b$Runtime$evaluate(
      "var el = document.querySelector('.pwsinfo'); if (el) el.click();"
    )
    wait_for_ma_popup_closed(b)
    pwsid_i
  }, character(1))

  page_rows %>% dplyr::mutate(pwsid_raw = pwsid_raw)
}

#' Pull new MA BWN data and reconcile with previous run before saving.
#' @param config Main config
#' @param dataset_id "raw_ma_bwn"
#' @return Reconciled raw BWN data frame
update_raw_ma_bwn <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link
  pwsid_names_link <- sub_config$input_links$pwsid_names_link

  message("Initializing headless Chrome session...")
  chrome_object <- chromote::Chromote$new(
    browser = chromote::Chrome$new(args = c(
      chromote::get_chrome_args(), "--no-sandbox", "--disable-dev-shm-usage"
    ))
  )
  b <- chrome_object$new_session()
  on.exit(try(b$close(), silent = TRUE), add = TRUE)

  message("Navigating to the MassDEP Public Health Orders page...")
  b$Page$navigate(source_url, timeout_ = 60)
  Sys.sleep(15)

  message("Clicking on the Search button to load all orders for the past calendar year...")
  b$Runtime$evaluate(
    "document.getElementsByName('ctl00$ContentPlaceHolder1$mmbtnSearch')[0].click()"
  )
  Sys.sleep(10)

  message("Scraping page 1...")
  ma_bwn_raw <- scrape_ma_bwn_grid_page(b)

  message("Paging through and scraping remaining results...")
  page_num <- 1L
  repeat {
    next_page_link <- sprintf(
      "a[href*=\"__doPostBack('ctl00$ContentPlaceHolder1$GridView1','Page$%d')\"]",
      page_num + 1L
    )
    has_next <- b$Runtime$evaluate(sprintf(
      "!!document.querySelector('%s')", gsub("'", "\\\\'", next_page_link)
    ))$result$value

    if (!isTRUE(has_next)) {
      message("No more pages.")
      break
    }

    page_num <- page_num + 1L
    message(sprintf("On page: %d", page_num))
    b$Runtime$evaluate(sprintf(
      "document.querySelector('%s').click()", gsub("'", "\\\\'", next_page_link)
    ))
    Sys.sleep(10)

    ma_bwn_raw <- dplyr::bind_rows(ma_bwn_raw, scrape_ma_bwn_grid_page(b))
  }

  message("Tidying scraped data...")
  ma_bwn_tidy <- ma_bwn_raw %>%
    dplyr::distinct() %>%
    tidyr::separate(pwsid_raw, c("pws_name", "pwsid_suffix"), sep = "PWS ID#: ") %>%
    dplyr::mutate(
      pws_name = stringr::str_squish(stringr::str_sub(pws_name, end = -2)),
      # Assumes every pwsid starts with "MA"
      pwsid = paste0("MA", stringr::str_squish(stringr::str_sub(pwsid_suffix, end = -2)))
    ) %>%
    dplyr::select(-pwsid_suffix) %>%
    dplyr::relocate(pwsid, .after = pws_name) %>%
    dplyr::mutate(last_epic_run_date = as.character(Sys.Date()))

  message("Filtering to community water systems...")
  epa_sabs_pwsids <- s3_read_csv(pwsid_names_link)
  ma_bwn_tidy <- ma_bwn_tidy %>%
    dplyr::filter(pwsid %in% epa_sabs_pwsids$pwsid)

  message("Reading the previous run...")
  ma_bwn_old <- read_prior_bwn(link)

  ma_bwn_reconciled <- reconcile_ma_bwn(ma_bwn_tidy, ma_bwn_old)

  message("Validating raw_ma_bwn...")
  validate_raw_bwn(config, ma_bwn_reconciled, ma_bwn_old, dataset_id,
                   label = "MA Boil Water Notice Validation")

  message(sprintf("Writing raw_ma_bwn to S3 to %s...", link))
  s3_write_csv(ma_bwn_reconciled, link)

  return(ma_bwn_reconciled)
}

#' Reconcile a fresh Massachusetts pull against the previous run. Keys on
#' pwsid + date_order_issued + order_type, plus date_order_terminated to catch
#' a lift date being reported later for an advisory already on file.
#' @param ma_bwn_tidy Fresh tidied pull
#' @param ma_bwn_old Previous raw pull, or NULL on a first run
#' @return Combined data frame
reconcile_ma_bwn <- function(ma_bwn_tidy, ma_bwn_old) {
  reconcile_bwn_rolling_window(
    ma_bwn_tidy, ma_bwn_old,
    key_cols        = c("pwsid", "date_order_issued", "date_order_terminated", "order_type"),
    update_key_cols = c("pwsid", "date_order_issued", "order_type")
  )
}

#' Standardize MA BWN into the shared cross-state BWN schema
#' @param config Main config
#' @param dataset_id "clean_ma_bwn"
#' @param bwn_raw Optional pre-loaded raw BWN data. If NULL, downloaded from S3.
run_clean_ma_bwn_pipeline <- function(config, dataset_id = "clean_ma_bwn",
                                      bwn_raw = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  link <- sub_config$link

  if (is.null(bwn_raw)) {
    message("Downloading raw MA BWN from S3...")
    bwn_raw <- s3_read_csv(raw_link)
  }

  message("Standardizing to the shared BWN schema...")
  ma_bwn_clean <- bwn_raw %>%
    mutate(date_issued = as.Date(date_order_issued, tryFormats = c("%m/%d/%Y")),
           date_lifted = as.Date(date_order_terminated, tryFormats = c("%m/%d/%Y")),
           last_epic_run_date = as.Date(last_epic_run_date,
                                        tryFormats = c("%Y-%m-%d")),
           type = order_type,
           epic_date_lifted_flag = "Reported",
           state = "Massachusetts") %>%
    finalize_bwn_clean()

  message("Validating clean_ma_bwn...")
  validate_clean_bwn(config, ma_bwn_clean, dataset_id,
                     label = "MA BWN Clean Validation")

  message(sprintf("Writing clean_ma_bwn to S3 to %s...", link))
  s3_write_csv(ma_bwn_clean, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(ma_bwn_clean)
}
