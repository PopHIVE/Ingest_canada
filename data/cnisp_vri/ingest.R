# =============================================================================
# CNISP-VRI Data Ingestion
# Canadian Nosocomial Infection Surveillance Program - Viral Respiratory Infections
# Source: Health Infobase Canada API
# https://health-infobase.canada.ca/cnisp/viral-respiratory-infections.html
# Prefix: hc_vri_ (Health Canada VRI)
#
# Tables ingested:
#   vri_rates       - Hospitalization rates per 1,000 admissions by virus & age
#   covid_rates     - COVID-19 outcomes (hosp, ICU, deaths) by age
#   outbreaks       - Weekly new VRI outbreak counts by virus
# =============================================================================

library(dplyr)
library(tidyr)

# Initialize process record
process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Download raw data from Health Infobase API
# -----------------------------------------------------------------------------
api_base <- "https://health-infobase.canada.ca/api/cnisp-vri/table/"

raw_vri <- jsonlite::fromJSON(paste0(api_base, "vri_rates"))
raw_covid <- jsonlite::fromJSON(paste0(api_base, "covid_rates"))
raw_outbreaks <- jsonlite::fromJSON(paste0(api_base, "outbreaks"))

# Save raw data
raw_paths <- c(
  vri = "raw/vri_rates.csv.gz",
  covid = "raw/covid_rates.csv.gz",
  outbreaks = "raw/outbreaks.csv.gz"
)

raw_list <- list(vri = raw_vri, covid = raw_covid, outbreaks = raw_outbreaks)

for (nm in names(raw_paths)) {
  con <- gzfile(raw_paths[[nm]], "w")
  write.csv(raw_list[[nm]], con, row.names = FALSE)
  close(con)
}

# Check if data has changed (hash all raw files together)
current_hashes <- sapply(raw_paths, function(p) unname(tools::md5sum(p)))
raw_state <- list(hashes = current_hashes)

if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 2. Process VRI rates (hospitalization rates by virus and age group)
  #    Rates are per 1,000 patient admissions across ~70 CNISP hospitals
  # ---------------------------------------------------------------------------
  vri <- raw_vri %>%
    mutate(
      rate = as.numeric(rate),
      # Week field is Sunday (start of epiweek); convert to Saturday (end)
      time = format(as.Date(Week) + 6, "%Y-%m-%d"),
      age = case_when(
        age_group == "All" ~ "Overall",
        TRUE ~ age_group
      ),
      virus_code = case_when(
        virus == "COVID-19" ~ "hc_vri_covid",
        virus == "Influenza A" ~ "hc_vri_flu_a",
        virus == "Influenza B" ~ "hc_vri_flu_b",
        virus == "RSV" ~ "hc_vri_rsv"
      )
    ) %>%
    filter(!is.na(virus_code)) %>%
    select(time, age, virus_code, rate) %>%
    pivot_wider(names_from = virus_code, values_from = rate)

  # ---------------------------------------------------------------------------
  # 3. Process COVID rates (COVID-specific outcomes by age group)
  #    "Hospitalizations" duplicates vri_rates COVID-19, so only keep ICU/Deaths
  # ---------------------------------------------------------------------------
  covid <- raw_covid %>%
    mutate(
      rate = as.numeric(rate),
      time = format(as.Date(Week) + 6, "%Y-%m-%d"),
      age = case_when(
        age_group == "All" ~ "Overall",
        TRUE ~ age_group
      ),
      type_code = case_when(
        type == "ICU admissions" ~ "hc_vri_covid_icu",
        type == "Deaths" ~ "hc_vri_covid_deaths",
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(type_code)) %>%
    select(time, age, type_code, rate) %>%
    pivot_wider(names_from = type_code, values_from = rate)

  # ---------------------------------------------------------------------------
  # 4. Combine VRI rates + COVID outcomes (shared time x age dimensions)
  # ---------------------------------------------------------------------------
  data_main <- vri %>%
    left_join(covid, by = c("time", "age")) %>%
    mutate(geography = "00") %>%
    select(geography, time, age, starts_with("hc_vri_")) %>%
    arrange(time, age)

  # Write main standard output
  out_main <- "standard/data.csv.gz"
  con <- gzfile(out_main, "w")
  write.csv(data_main, con, row.names = FALSE)
  close(con)

  cat("Wrote", nrow(data_main), "rows to", out_main, "\n")

  # ---------------------------------------------------------------------------
  # 5. Process outbreak counts (by virus, no age dimension)
  # ---------------------------------------------------------------------------
  outbreaks <- raw_outbreaks %>%
    mutate(
      outbreak_count = as.numeric(outbreak_count),
      time = format(as.Date(Week) + 6, "%Y-%m-%d"),
      virus_code = case_when(
        virus == "COVID-19" ~ "hc_vri_outbreaks_covid",
        virus == "Influenza A" ~ "hc_vri_outbreaks_flu_a",
        virus == "Influenza B" ~ "hc_vri_outbreaks_flu_b",
        virus == "RSV" ~ "hc_vri_outbreaks_rsv"
      )
    ) %>%
    filter(!is.na(virus_code)) %>%
    select(time, virus_code, outbreak_count) %>%
    pivot_wider(names_from = virus_code, values_from = outbreak_count) %>%
    mutate(geography = "00") %>%
    select(geography, time, starts_with("hc_vri_outbreaks_")) %>%
    arrange(time)

  # Write outbreaks standard output
  out_outbreaks <- "standard/data_outbreaks.csv.gz"
  con <- gzfile(out_outbreaks, "w")
  write.csv(outbreaks, con, row.names = FALSE)
  close(con)

  cat("Wrote", nrow(outbreaks), "rows to", out_outbreaks, "\n")

  # ---------------------------------------------------------------------------
  # 6. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
