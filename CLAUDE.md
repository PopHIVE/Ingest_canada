# PopHIVE/Ingest - Claude Code Configuration

## Project Overview

This repository standardizes public health surveillance data for the PopHIVE platform (pophive.org). Data sources are transformed into a consistent format and combined into bundles for visualization. The project uses the `dcf` R package from Yale's Data-Intensive Social Science Center (DISSC) for workflow management.

**Repository**: https://github.com/PopHIVE/Ingest  
**Documentation**: https://pophive.github.io/processing-documentation/  
**Data Status**: https://dissc-yale.github.io/dcf/report/?repo=PopHIVE/Ingest

---

## Standard Data Format Specification

All standardized output files must conform to these column specifications:

### Required Columns

| Column | Description | Format | Examples |
|--------|-------------|--------|----------|
| `geography` | Geographic identifier | SGC code string | `"00"` (national/Canada), `"35"` (Ontario), `"24"` (Quebec) |
| `time` | Time period end date | `YYYY-mm-dd` | `"2025-01-04"` (Saturday for weekly data) |

### Common Optional Columns

| Column | Description | Values |
|--------|-------------|--------|
| `age` | Age group | `"0-4"`, `"5-17"`, `"18-49"`, `"50-64"`, `"65+"`, `"Overall"` |
| `sex` | Sex category | `"Male"`, `"Female"`, `"Overall"` |

**Note**: Categorical dimensions like virus type should NOT be stored as a column. Instead, pivot to wide format with one value column per category (see "Wide Format" section below).

### Value Column Naming Convention: Data-Specific Prefixes

**CRITICAL**: All value/measure columns in standardized output files MUST use a data-source-specific prefix to avoid naming collisions when datasets are combined in bundles. The prefix should be a short, descriptive abbreviation of the data source.

| Prefix | Source | Example Columns |
|--------|--------|----------------|
| `hc_ww_` | Health Canada Wastewater | `hc_ww_covid`, `hc_ww_flu_a`, `hc_ww_rsv`, `hc_ww_covid_min` |
| `hc_op_` | Health Canada Opioids | `hc_op_value`, `hc_op_rate` |

**Rules**:
- Standard shared columns (`geography`, `time`, `age`, `sex`, etc.) do NOT get prefixed
- Only value/measure columns and source-specific metadata columns get the prefix
- The prefix format is: `{org}_{topic}_` (e.g., `hc_ww_` = Health Canada Wastewater)
- All prefixed columns must be documented in `measure_info.json` with the prefixed name as the variable ID

### Wide Format: Categorical Dimensions as Columns

**CRITICAL**: When source data has a categorical dimension (e.g., virus type, age group) with associated values, the standard file MUST be pivoted to **wide format** with one column per category, NOT stored as a long-format `virus`/`age` column with a generic `value` column.

**Why**: Wide format makes each measure a distinct, self-documenting column that maps directly to a `measure_info.json` entry. It avoids ambiguity when bundling datasets and makes downstream joins simpler.

**Example**: Wastewater data with 4 viruses becomes:
```
geography, time, hc_ww_covid, hc_ww_flu_a, hc_ww_flu_b, hc_ww_rsv, hc_ww_covid_min, ...
```
NOT:
```
geography, time, virus, hc_ww_value, hc_ww_value_min, ...
```

Use `tidyr::pivot_wider()` in the ingest script to reshape, and create one `measure_info.json` entry per resulting column.

### Value Columns

| Column | Description |
|--------|-------------|
| `{prefix}{measure}` | Primary measure (closest to source data) |
| `{prefix}{measure}_smooth` | 3-week moving average |
| `{prefix}{measure}_smooth_scale` | Smoothed value scaled 0-100 |
| `{prefix}{measure}_min` | Minimum value (e.g., across sites) |
| `{prefix}{measure}_max` | Maximum value (e.g., across sites) |
| `suppressed_flag` | `1` if value was suppressed and imputed, `0` otherwise |

### Geography Standards (Canada)

This repository uses Statistics Canada's Standard Geographical Classification (SGC) codes:

- **National**: Use `"00"` (not `"CA"` or `"0"`)
- **Province/Territory**: 2-digit SGC code as string (`"35"` for Ontario, not `35`)
- **PREFERRED**: Convert province names to SGC codes using merge with `resources/all_geo.csv.gz`

#### all_geo.csv.gz Structure

The file contains three columns:
| Column | Description | Examples |
|--------|-------------|----------|
| `geography` | SGC code | `"35"` (Ontario), `"24"` (Quebec), `"00"` (national) |
| `geography_name` | Full name | `"Ontario"`, `"Quebec"`, `"Canada"` |
| `province_abbr` | Province abbreviation | `"ON"`, `"QC"`, `"CA"` |

#### Complete SGC Province/Territory Codes

| Code | Province/Territory | Abbr |
|------|-------------------|------|
| `00` | Canada (national) | CA |
| `10` | Newfoundland and Labrador | NL |
| `11` | Prince Edward Island | PE |
| `12` | Nova Scotia | NS |
| `13` | New Brunswick | NB |
| `24` | Quebec | QC |
| `35` | Ontario | ON |
| `46` | Manitoba | MB |
| `47` | Saskatchewan | SK |
| `48` | Alberta | AB |
| `59` | British Columbia | BC |
| `60` | Yukon | YT |
| `61` | Northwest Territories | NT |
| `62` | Nunavut | NU |

#### Province-level geography lookup
  ```r
  # Load geography crosswalk (do this once per script)
  all_geo <- read.csv(gzfile("../../resources/all_geo.csv.gz"), colClasses = "character")

  # For province abbreviations (e.g., "ON", "QC"):
  province_lookup <- all_geo %>%
    filter(geography != "00") %>%
    select(geography, province_abbr)

  # For full province names (e.g., "Ontario", "Quebec"):
  province_lookup <- all_geo %>%
    filter(geography != "00") %>%
    select(geography, geography_name)

  # Merge to get SGC codes
  data <- data %>%
    left_join(province_lookup, by = c("province" = "geography_name"))
  ```

### Time Standards

- **Format**: `YYYY-mm-dd` (ISO 8601 format with leading zeros)
- **Weekly data**: Use Saturday at end of week (epiweek convention)
- **Monthly data**: Use last day of month
- **Annual data**: Use `YYYY-12-31`

---

## Directory Structure

```
PopHIVE/ingest_canada/
├── data/
│   ├── {source_name}/           # Individual data source
│   │   ├── raw/                 # Downloaded source files (compressed)
│   │   ├── standard/            # Standardized output files
│   │   │   └── data.csv.gz      # Main standardized file
│   │   ├── ingest.R             # Transformation script (SINGLE FILE PER SOURCE)
│   │   ├── measure_info.json    # Variable metadata
│   │   └── process.json         # Processing state (auto-generated)
│   │
│   ├── bundle_{category}/       # Combined datasets
│   │   ├── build.R              # Bundle assembly script
│   │   ├── process.json         # Lists source files used
│   │   └── dist/                # Final outputs for visualization
│   │       └── *.parquet        # Parquet format only (no CSV)
│   │
│   ├── wastewater/              # Health Canada wastewater surveillance (hc_ww_)
│   └── hc_opioids/              # Health Canada opioid data (hc_op_)
│
├── scripts/                     # Utility scripts
├── resources/                   # Reference files (SGC codes, etc.)
│   └── all_geo.csv.gz           # Province/territory SGC code crosswalk
├── settings.json               # Project configuration
├── file_log.json               # File tracking
└── renv.lock                   # R package versions
```

---

## Key dcf Package Functions

```r
# Create new data source folder structure
### Important!! When adding a new data source, you MUST run this function. Otherwise the process.json files will not be initialized correctly, causing the pieplein to fail
dcf::dcf_add_source("source_name")

# Initialize processing record for tracking changes
process <- dcf::dcf_process_record()

# Download data from CDC data.gov (Socrata API)
raw_state <- dcf::dcf_download_cdc(
  "dataset-id",      # e.g., "kvib-3txy"
  "raw",             # output directory
  process$raw_state  # previous state for change detection
)

# Update processing record after changes
process$raw_state <- raw_state
dcf::dcf_process_record(updated = process)

# Create or update a bundle
dcf::dcf_process("bundle_respiratory", ".")

# Build all sources (run from project root)
dcf::dcf_build()
```

---

## Important Convention: Single ingest.R Per Data Source

**CRITICAL**: Each data source directory must contain exactly **ONE** `ingest.R` script.

### Multiple Data Sources in One Directory

When a single data source directory needs to process multiple related datasets (e.g., data from different URLs or APIs), **integrate all processing into the single `ingest.R` file** rather than creating separate scripts.

### Example: SchoolVaxView

The `schoolvaxview` directory processes data from two sources:
1. CDC SchoolVaxView (via Socrata API)
2. Washington Post School Vaccination Rates (via GitHub)

Both are integrated into a single `ingest.R` file:

```r
# ingest.R structure for multiple sources
process <- dcf::dcf_process_record()

# Source 1: CDC SchoolVaxView
raw_state <- dcf::dcf_download_cdc("ijqb-a7ye", "raw", process$raw_state)
if (!identical(process$raw_state, raw_state)) {
  # Process CDC data...
  # Write to standard/data.csv.gz and standard/data_exemptions.csv.gz
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}

# Source 2: Washington Post
download.file(wapo_url, "raw/wapo_file.csv")
current_wapo_state <- list(hash = tools::md5sum("raw/wapo_file.csv"))
if (!identical(process$wapo_state, current_wapo_state)) {
  # Process WaPo data...
  # Write to standard/data_wapo_counties.csv.gz and standard/data_wapo_schools.csv.gz
  process$wapo_state <- current_wapo_state
  dcf::dcf_process_record(updated = process)
}
```

### Key Points

- Use the `process` object to track multiple raw data states (e.g., `process$raw_state`, `process$wapo_state`)
- Each source can have its own change detection logic
- Output multiple standardized files with descriptive names (e.g., `data_wapo_counties.csv.gz`)
- All variables from secondary sources should use prefixes to avoid naming conflicts (e.g., `wapo_`)

### Why This Matters

- The `dcf` package expects one `ingest.R` per source directory
- Running `dcf::dcf_process("source_name", "..")` executes the single `ingest.R`
- Multiple scripts would require manual orchestration outside of `dcf`

---

## ingest.R Template

```r
# =============================================================================
# {SOURCE_NAME} Data Ingestion
# Source: {URL or description}
# Prefix: {prefix}_ (e.g., hc_ww_ for Health Canada Wastewater)
# =============================================================================

library(dplyr)

# Initialize process record (creates process.json if it doesn't exist)
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# -----------------------------------------------------------------------------
# 1. Download raw data
# -----------------------------------------------------------------------------
# For Health Infobase API:
api_url <- "https://health-infobase.canada.ca/api/{endpoint}"
raw_data <- jsonlite::fromJSON(api_url)
raw_path <- "raw/{source}.csv.gz"
con <- gzfile(raw_path, "w")
write.csv(raw_data, con, row.names = FALSE)
close(con)

current_hash <- tools::md5sum(raw_path)
raw_state <- list(hash = unname(current_hash))

# Only process if data has changed
if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 2. Load geography lookup and read raw data
  # ---------------------------------------------------------------------------
  # Load Canada geography crosswalk
  all_geo <- read.csv(gzfile("../../resources/all_geo.csv.gz"), colClasses = "character")

  # Province-level lookup (for full province names like "Ontario")
  province_lookup <- all_geo %>%
    filter(geography != "00") %>%
    select(geography, geography_name)

  data_raw <- read.csv(gzfile(raw_path))

  # ---------------------------------------------------------------------------
  # 3. Transform data
  # ---------------------------------------------------------------------------
  data_standard <- data_raw %>%
    # Map geography using province lookup
    left_join(province_lookup, by = c("province" = "geography_name")) %>%
    mutate(
      geography = case_when(
        location == "Canada" ~ "00",
        !is.na(geography) ~ geography,
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(geography)) %>%
    # Format time (adjust to Saturday end-of-week if needed)
    mutate(
      time = format(as.Date(date_col), "%Y-%m-%d")
    ) %>%
    # Rename value columns WITH DATA-SPECIFIC PREFIX
    rename(
      {prefix}_value = raw_value_col
    ) %>%
    # Select standard + prefixed columns
    select(geography, time, {prefix}_value)

  # ---------------------------------------------------------------------------
  # 4. Write standardized output
  # ---------------------------------------------------------------------------
  out_path <- "standard/data.csv.gz"
  con <- gzfile(out_path, "w")
  write.csv(data_standard, con, row.names = FALSE)
  close(con)

  # ---------------------------------------------------------------------------
  # 5. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
```

---

## measure_info.json Template

Each `measure_info.json` file should include variable definitions and a centralized `_sources` object. Variables reference sources by ID.

```json
{
  "{prefix}_variable_name": {
    "id": "{prefix}_variable_name",
    "short_name": "Brief description (< 100 chars)",
    "long_name": "Full descriptive name",
    "category": "respiratory|immunization|chronic|injury",
    "short_description": "One sentence description",
    "long_description": "Detailed description with methodology notes",
    "statement": "Template for narrative: 'In {location}, {value} cases were reported'",
    "measure_type": "Incidence|Prevalence|Rate|Percent|Count",
    "unit": "Cases per 100,000|Percent|Count",
    "time_resolution": "Week|Month|Year",
    "sources": [{ "id": "source_id" }],
    "citations": [
      {
        "title": "Publication title",
        "url": "https://doi.org/..."
      }
    ]
  },

  "_sources": {
    "source_id": {
      "name": "Full source name",
      "url": "https://data.source.url",
      "organization": "Organization name",
      "organization_url": "https://organization.url",
      "location": "Specific dataset location (optional)",
      "location_url": "https://specific.dataset.url (optional)",
      "description": "Detailed narrative description of the data source, including methodology, coverage, limitations, and any important caveats for users.",
      "restrictions": "License and usage restrictions. Examples: 'Public domain. CDC data is generally not subject to copyright restrictions.' or 'CC BY 4.0. Attribution required for reuse.' or 'Attribution required. Cite [citation].'",
      "date_accessed": 2025
    }
  }
}
```

### _sources Field Requirements

Every `_sources` entry MUST include:
- **name**: Full name of the data source
- **url**: Primary URL for the data source
- **organization**: Name of the organization providing the data
- **organization_url**: URL for the organization
- **description**: Narrative description of the source (methodology, coverage, limitations)
- **restrictions**: License and usage restrictions

Special restriction wording:
- **Health Infobase Canada / PHAC**: "Open Government Licence - Canada. Data may be freely reused with attribution to the Public Health Agency of Canada."
- **Statistics Canada**: "Open Government Licence - Canada. Data may be freely reused with attribution to Statistics Canada."
- **Academic publications**: "Attribution required. Cite [full citation]."

---

## Common Data Source Patterns

### Health Infobase Canada API

```r
# Health Infobase provides JSON APIs for various datasets
api_base <- "https://health-infobase.canada.ca/api/"
url <- paste0(api_base, "{endpoint}/table/{table_name}")
raw_data <- jsonlite::fromJSON(url)
```

### Suppressed Data Handling

Some Canadian data sources suppress small counts for privacy:
```r
data <- data %>%
  mutate(
    suppressed_flag = if_else(count < 10, 1, 0),
    # Impute suppressed values as halfway between 0 and minimum
    {prefix}_value = if_else(
      suppressed_flag == 1,
      min({prefix}_value[suppressed_flag == 0], na.rm = TRUE) / 2,
      {prefix}_value
    )
  )
```

### National Averages from Provincial Data

When national totals aren't provided, calculate population-weighted average:
```r
# Load province populations
prov_pop <- read.csv("resources/province_populations.csv")

data_national <- data %>%
  left_join(prov_pop, by = "geography") %>%
  group_by(time, age) %>%
  summarize(
    {prefix}_value = weighted.mean({prefix}_value, population, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(geography = "00")

data_final <- bind_rows(data, data_national)
```

---

## Bundle build.R Template

```r
# =============================================================================
# Bundle: {bundle_name}
# Combines: {list of sources}
# =============================================================================

library(dplyr)
library(arrow)

# -----------------------------------------------------------------------------
# 1. Load standardized source files
# (Each source uses its own prefixed value columns, e.g., hc_ww_value)
# -----------------------------------------------------------------------------
wastewater <- vroom::vroom("../wastewater/standard/data.csv.gz")

# -----------------------------------------------------------------------------
# 2. Combine (prefixed columns avoid naming collisions)
# -----------------------------------------------------------------------------
combined <- bind_rows(
  wastewater %>% mutate(source = "hc_wastewater")
)

# -----------------------------------------------------------------------------
# 3. Write outputs (parquet only, no CSV)
# -----------------------------------------------------------------------------
arrow::write_parquet(
  combined,
  "dist/overall_trends.parquet",
  compression = "snappy"  # Use "gzip" for smaller files
)
```

---

## Validation Checklist

When creating or reviewing ingestion scripts, verify:

- [ ] **Geography**: All values are valid SGC codes; national = `"00"`
- [ ] **Time**: Format is `YYYY-mm-dd`; weekly data uses Saturday
- [ ] **Column names**: Use standard names (lowercase, underscores) with data-specific prefix for value columns
- [ ] **Missing data**: Handled appropriately (NA, not empty strings)
- [ ] **Suppression**: Flagged with `suppressed_flag` column if imputed
- [ ] **measure_info.json**: Entry exists for each variable
- [ ] **Compression**: Standard output files are gzip compressed (`.csv.gz`)
- [ ] **Bundle outputs**: Dist files are parquet only (`.parquet`), no CSV
- [ ] **process.json**: Updated in bundle with source file paths

---

## Common Issues and Solutions

### Issue: Province names instead of SGC codes

See the **Geography Standards (Canada)** section above for the preferred approach using `all_geo.csv.gz`.

```r
# Merge province names to SGC codes
all_geo <- read.csv(gzfile("../../resources/all_geo.csv.gz"), colClasses = "character")
province_lookup <- all_geo %>%
  filter(geography != "00") %>%
  select(geography, geography_name)

data <- data %>%
  left_join(province_lookup, by = c("province" = "geography_name"))
```

### Issue: Date in wrong format
```r
# Solution: Parse and reformat to ISO 8601
mutate(time = format(as.Date(time, "%m-%d-%Y"), "%Y-%m-%d"))
```

### Issue: National data missing
```r
# Solution: Calculate population-weighted average (see pattern above)
```

### Issue: Weekly dates not on Saturday
```r
# Solution: Adjust to end-of-week Saturday
mutate(time = ceiling_date(as.Date(time), "week", week_start = 7) - 1)
```

### Issue: Multiple records per geography/time
```r
# Solution: Check for duplicates, aggregate if needed
data %>%
  group_by(geography, time, age) %>%
  summarize(value = sum(value), .groups = "drop")
```

### Issue: Error "process file process.json does not exist"
This is caused by failure to initialize a new datasource with `dcf::dcf_add_source()`. If this is not done, the process.json file is not properly initialized.

**Preferred solution**: Run `dcf::dcf_add_source("source_name")` to create the folder structure properly.

**Manual fix**: If you need to create the process.json manually, use this structure (replace `source_name` with your data folder name):

```json
{
  "name": "source_name",
  "type": "source",
  "scripts": [
    {
      "path": "ingest.R",
      "manual": false,
      "frequency": 0,
      "last_run": "",
      "run_time": "",
      "last_status": {
        "log": "",
        "success": true
      }
    }
  ],
  "checked": "",
  "check_results": []
}
```

### Issue: Error "raw/file.csv.gz does not exist in current working directory"
```r
# Problem: ingest.R uses relative paths and must be run from source directory
# Solution: Change to source directory before running, then change back
setwd("data/source_name")
source("ingest.R")
setwd("../..")
```

### Issue: Error "missing process file: ../ingest/process.json" when using dcf_process
```r
# Problem: Wrong directory parameter or missing project files
# Solution: From project root, just use the source name (no directory parameter)
dcf::dcf_process("source_name")

# Also ensure project.Rproj and README.md exist in the source folder
```

### Issue: Error "vec_math.arrow_binary() not implemented" when running dcf_process()

This error occurs when a script works fine when run directly but fails via `dcf_process()`. The cause is vroom's Arrow ALTREP (lazy loading) backend:

- **When running directly**: Interactive sessions may materialize data earlier or have different environment state
- **When running via dcf_process()**: Scripts run in a cleaner context where Arrow ALTREP stays active, keeping columns as Arrow binary types until an operation forces materialization

```r
# Solution: Use geography lookup merge (avoids Arrow type issues)
all_geo <- read.csv(gzfile("../../resources/all_geo.csv.gz"), colClasses = "character")

data <- data %>%
  left_join(all_geo, by = c("province" = "geography_name")) %>%
  mutate(geography = if_else(location == 'Canada', "00", geography))

# Alternative: Disable Arrow ALTREP (less preferred)
data <- vroom::vroom("file.csv.xz", show_col_types = FALSE, altrep = FALSE)
```

---

## Quick Reference Commands

```r
# Check current data status
dcf::dcf_status()

# Rebuild single source (from project root)
dcf::dcf_process("source_name")

# Rebuild single bundle
dcf::dcf_process("bundle_measles")

# Full rebuild
dcf::dcf_build()

# Validate standard file format
source("scripts/validate_standard.R")
validate_standard_file("data/source_name/standard/data.csv.gz")

# Rebuild data source documentation (generates docs/index.html)
Rscript scripts/build_docs.R
```

---

## Adding a New Data Source: Step-by-Step

1. **Create folder structure**
   ```r
   dcf::dcf_add_source("new_source")
   ```

2. **Edit `ingest.R`**: Follow template above, adapting for source format

3. **Create `measure_info.json`**: Add entries for all output variables

4. **Test transformation**
   ```r
   source("data/new_source/ingest.R")
   ```

5. **Validate output**
   ```r
   validate_standard_file("data/new_source/standard/data.csv.gz")
   ```

6. **Add to bundle**: Update relevant `bundle_*/build.R` and `process.json`

7. **Rebuild bundle**
   ```r
   dcf::dcf_process("bundle_category", ".")
   ```

8. **Update documentation**: The data source documentation is auto-generated from `measure_info.json` files
   ```r
   Rscript scripts/build_docs.R
   ```
   This generates `docs/index.html` with variable tables and source information. The GitHub Action will also rebuild docs automatically when `measure_info.json` files change.

9. **Commit changes**: Include raw data sample, ingest.R, measure_info.json, standard output, and updated docs/

---

## Contact and Resources

- **Processing Documentation**: https://pophive.github.io/processing-documentation/
- **dcf Package**: https://dissc-yale.github.io/dcf/
- **Data Status Report**: https://dissc-yale.github.io/dcf/report/?repo=PopHIVE/Ingest
- **Feedback Form**: https://docs.google.com/forms/d/e/1FAIpQLSchAasiq7ovCCNz9ussb7C2ntkZ-8Rjc7-tNSglkf5boS-A0w/viewform