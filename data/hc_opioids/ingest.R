# =============================================================================
# Health Canada Opioid and Stimulant Harms Data Ingestion
# Source: Health Infobase Canada API
# https://health-infobase.canada.ca/substance-related-harms/opioids-stimulants/
# Prefix: hc_op_ (Health Canada Opioids)
# =============================================================================

library(dplyr)
library(tidyr)

# Initialize process record
process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Download raw data from Health Infobase API
# -----------------------------------------------------------------------------
api_base <- "https://health-infobase.canada.ca/api/opioids"

# Download the full opioids table
url_opioids <- paste0(api_base, "/table/opioids")
raw_data <- jsonlite::fromJSON(url_opioids)

# Save raw data
raw_path <- "raw/opioids.csv.gz"
con <- gzfile(raw_path, "w")
write.csv(raw_data, con, row.names = FALSE)
close(con)

# Check if data has changed
current_hash <- tools::md5sum(raw_path)
raw_state <- list(hash = unname(current_hash))

if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 2. Load geography lookup
  # ---------------------------------------------------------------------------
  all_geo <- read.csv(
    gzfile("../../resources/all_geo.csv.gz"),
    colClasses = "character"
  )

  province_lookup <- all_geo %>%
    filter(geography != "00") %>%
    select(geography, geography_name)

  # ---------------------------------------------------------------------------
  # 3. Filter to quarterly opioid counts by source
  # ---------------------------------------------------------------------------
  # Keep: Substance=Opioids, Specific_Measure=Overall numbers,
  #        Unit=Number, Time_Period=By quarter
  # Sources: Deaths, Hospitalizations, ED Visits, EMS
  data_quarterly <- raw_data %>%
    filter(
      Substance == "Opioids",
      Specific_Measure == "Overall numbers",
      Unit == "Number",
      Time_Period == "By quarter"
    )

  # ---------------------------------------------------------------------------
  # 4. Map geography
  # ---------------------------------------------------------------------------
  # PRUID "1" = Canada (national), others are province/territory codes
  # Filter out sub-provincial regions (Winnipeg, Northern/rural Manitoba,
  # Whitehorse, Yellowknife) and the combined "Territories" (PRUID 63)
  data_geo <- data_quarterly %>%
    left_join(
      province_lookup,
      by = c("Region" = "geography_name")
    ) %>%
    mutate(
      geography = case_when(
        PRUID == "1" ~ "00",
        !is.na(geography) ~ geography,
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(geography))

  # ---------------------------------------------------------------------------
  # 5. Parse time from xAxisLabels (e.g., "2016 Q1" -> end-of-quarter date)
  # ---------------------------------------------------------------------------
  quarter_to_date <- function(label) {
    parts <- regmatches(label, regexec("^(\\d{4}) Q(\\d)$", label))
    year <- as.integer(sapply(parts, `[`, 2))
    quarter <- as.integer(sapply(parts, `[`, 3))
    # End-of-quarter month: Q1=3, Q2=6, Q3=9, Q4=12
    month <- quarter * 3
    # Last day of the end-of-quarter month
    # Use first day of next month minus 1
    next_month <- ifelse(month == 12, 1, month + 1)
    next_year <- ifelse(month == 12, year + 1, year)
    end_date <- as.Date(
      paste(next_year, next_month, "01", sep = "-")
    ) - 1
    format(end_date, "%Y-%m-%d")
  }

  data_time <- data_geo %>%
    mutate(
      time = quarter_to_date(xAxisLabels),
      Value = as.numeric(Value)
    ) %>%
    filter(!is.na(time))

  # ---------------------------------------------------------------------------
  # 6. Create short source codes for column names
  # ---------------------------------------------------------------------------
  data_coded <- data_time %>%
    mutate(
      source_code = case_when(
        Source == "Deaths" ~ "deaths",
        Source == "Hospitalizations" ~ "hosp",
        Source == "Emergency Department (ED) Visits" ~ "ed",
        Source == "Emergency Medical Services (EMS)" ~ "ems",
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(source_code))

  # ---------------------------------------------------------------------------
  # 7. Pivot to wide format (one column per source with hc_op_ prefix)
  # ---------------------------------------------------------------------------
  data_standard <- data_coded %>%
    select(geography, time, source_code, Value) %>%
    pivot_wider(
      names_from = source_code,
      values_from = Value,
      names_prefix = "hc_op_"
    ) %>%
    arrange(geography, time)

  # ---------------------------------------------------------------------------
  # 8. Handle suppressed / "n/a" values
  # ---------------------------------------------------------------------------
  # Values that were "n/a" in the source become NA after as.numeric()
  # No imputation needed - leave as NA

  # ---------------------------------------------------------------------------
  # 9. Write standardized output
  # ---------------------------------------------------------------------------
  out_path <- "standard/data.csv.gz"
  con <- gzfile(out_path, "w")
  write.csv(data_standard, con, row.names = FALSE)
  close(con)

  cat(
    "Wrote", nrow(data_standard), "rows to", out_path, "\n"
  )

  # ---------------------------------------------------------------------------
  # 10. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
