# =============================================================================
# Canada Wastewater Surveillance Data Ingestion
# Source: Health Infobase Canada API
# https://health-infobase.canada.ca/api/
# =============================================================================

library(dplyr)
library(tidyr)

# Initialize process record
process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Download raw data from Health Infobase API
# -----------------------------------------------------------------------------
api_base <- "https://health-infobase.canada.ca/api/wastewater"

# Download the aggregate (weekly) table
url_aggregate <- paste0(api_base, "/table/wastewater_aggregate")
raw_aggregate <- jsonlite::fromJSON(url_aggregate)

# Save raw data
raw_path <- "raw/wastewater_aggregate.csv.gz"
con <- gzfile(raw_path, "w")
write.csv(raw_aggregate, con, row.names = FALSE)
close(con)

# Check if data has changed
current_hash <- tools::md5sum(raw_path)
raw_state <- list(hash = unname(current_hash))

if (!identical(process$raw_state, raw_state)) {

  # -------------------------------------------------------------------
  # 2. Load geography lookup
  # -------------------------------------------------------------------
  all_geo <- read.csv(
    gzfile("../../resources/all_geo.csv.gz"),
    colClasses = "character"
  )

  province_lookup <- all_geo %>%
    filter(geography != "00") %>%
    select(geography, geography_name)

  # -------------------------------------------------------------------
  # 3. Transform data
  # -------------------------------------------------------------------
  data_standard <- raw_aggregate %>%
    # Map geography: use province name to merge with lookup
    left_join(
      province_lookup,
      by = c("province" = "geography_name")
    ) %>%
    mutate(
      # National rows have Location == "Canada"
      geography = case_when(
        Location == "Canada" ~ "00",
        !is.na(geography) ~ geography,
        TRUE ~ NA_character_
      )
    ) %>%
    # Keep only province and national level rows
    # (site-level rows have empty province and are not Canada)
    filter(!is.na(geography)) %>%
    # Map virus names to short codes for column naming
    mutate(
      virus = case_when(
        measureid == "covN2" ~ "covid",
        measureid == "fluA" ~ "flu_a",
        measureid == "fluB" ~ "flu_b",
        measureid == "rsv" ~ "rsv",
        TRUE ~ measureid
      )
    ) %>%
    # Convert weekstart (Sunday) to week-ending Saturday
    mutate(
      time = format(
        as.Date(weekstart) + 6,
        "%Y-%m-%d"
      )
    ) %>%
    # Keep only needed columns before pivot
    select(
      geography,
      time,
      virus,
      value = w_avg,
      value_min = min,
      value_max = max
    ) %>%
    # Ensure values are numeric
    mutate(
      value = as.numeric(value),
      value_min = as.numeric(value_min),
      value_max = as.numeric(value_max)
    ) %>%
    # Pivot wider: one column per virus with hc_ww_ prefix
    pivot_wider(
      names_from = virus,
      values_from = c(value, value_min, value_max),
      names_glue = "hc_ww_{virus}_{.value}"
    ) %>%
    # Clean up column names: move value type to end
    rename_with(
      ~ sub("^hc_ww_(.+)_value$", "hc_ww_\\1", .),
      starts_with("hc_ww_")
    ) %>%
    rename_with(
      ~ sub("^hc_ww_(.+)_value_min$", "hc_ww_\\1_min", .),
      starts_with("hc_ww_")
    ) %>%
    rename_with(
      ~ sub("^hc_ww_(.+)_value_max$", "hc_ww_\\1_max", .),
      starts_with("hc_ww_")
    ) %>%
    arrange(geography, time)

  # -------------------------------------------------------------------
  # 4. Write standardized output
  # -------------------------------------------------------------------
  out_path <- "standard/data.csv.gz"
  con <- gzfile(out_path, "w")
  write.csv(data_standard, con, row.names = FALSE)
  close(con)

  cat(
    "Wrote", nrow(data_standard), "rows to", out_path, "\n"
  )

  # -------------------------------------------------------------------
  # 5. Record processed state
  # -------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
