
<!-- README.md is generated from README.Rmd. Please edit that file -->

# tidynpi

<!-- badges: start -->

[![License:
MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![R-CMD-check](https://img.shields.io/badge/R--CMD--check-passing-brightgreen.svg)](https://github.com/Lfirenzeg/tidynpi)
<!-- badges: end -->

**tidynpi** provides efficient access to NPPES (National Plan & Provider
Enumeration System) data via a hosted Parquet lake. Query millions of
provider records with minimal overhead using:

- **Manifest-driven discovery** of partitioned parquet files
- **DuckDB + HTTPFS** for selective columnar reads over HTTPS
- **Jaro-Winkler name matching** with configurable thresholds
- **Taxonomy code translation** to human-readable specialties
- **Reproducible pipelines** with version-pinned snapshots

No credentials required. No server setup. Just point and query.

## Installation

### From GitHub (Recommended)

``` r
# Install from GitHub
remotes::install_github("Lfirenzeg/tidynpi")
```

### From Source (Development)

``` r
# Clone the repository
# git clone https://github.com/Lfirenzeg/tidynpi.git
# cd tidynpi

# Build and install
devtools::build()
devtools::install()
```

## Quick Start

### High-Level Workflow: `npi_enrich()`

The main function enriches your input data by finding matching
providers:

``` r
library(tidynpi)
library(dplyr)

# 1. Prepare your inputs
inputs <- data.frame(
  full_name = c("Smith, John", "Anderson, Mary", "Garcia, Carlos"),
  state = c("NY", "CA", "TX"),
  city = c("New York", "Los Angeles", "Houston"),
  stringsAsFactors = FALSE
)

# 2. Connect to DuckDB
con <- tnp_duckdb(":memory:")

# 3. Load manifest (points to public snapshot)
man <- tnp_manifest()

# 4. Run enrichment
results <- npi_enrich(
  inputs = inputs,
  con = con,
  man = man,
  strategy = "strict",      # High precision (JW >= 0.90)
  city_mode = "prefer",     # Boost city matches
  max_candidates = 5,
  verbose = TRUE
)

# 5. View best matches
best_matches <- results |>
  group_by(input_id) |>
  slice(1) |>
  select(first_norm, last_norm, npi, jaro_winkler, rank_label)

print(best_matches)

# Clean up
DBI::dbDisconnect(con, shutdown = TRUE)
```

### Understanding Results

The enrichment returns scored candidates with:

- **`npi`**: Provider’s National Provider Identifier
- **`jaro_winkler`**: String similarity score (0-1)
- **`rank_label`**: Match quality tier (exact, strong, good, weak)
- **`exact_name`**: Boolean indicating perfect name match
- **`city_match`**: Boolean indicating city match
- **`tax_code_1..5`**: Provider taxonomy codes

## Key Features

### 1. Name Normalization

Automatically handles: - Comma-separated formats (“Last, First”) -
Space-separated formats (“First Last”) - Apostrophes and special
characters - Name suffixes (Jr, Sr, MD, etc.)

``` r
normalized <- npi_normalize(
  inputs = data.frame(
    full_name = c("O'Brien, Patrick J.", "de la Cruz, Maria"),
    state = c("MA", "FL"),
    city = c("Boston", "Miami")
  )
)

# Creates blocking keys: state_part + lname_initial
print(normalized[, c("first_norm", "last_norm", "state_part", "lname_initial")])
```

### 2. Taxonomy Translation

Convert cryptic codes to human-readable specialties:

``` r
# Download taxonomy dictionary (cached after first call)
tax_dict <- tnp_taxonomy_dict()

# Translate codes in your results
results_with_taxonomy <- tnp_taxonomy_translate(results)

# Now you have readable columns like:
# - tax_tax_code_1_display_name
# - tax_tax_code_1_classification
# - tax_tax_code_1_specialization
```

### 3. Selective Parquet Reads

Only read relevant data shards via partitioning:

``` r
# Read specific state + initial combination
con <- tnp_duckdb(":memory:")
man <- tnp_manifest()

# Get all providers in NY with last name starting with 'S'
ny_smiths <- tnp_lake_read_https(
  con = con,
  man = man,
  state = "NY",
  initials = "S",
  columns = c("npi", "first_name", "last_name", "city", "tax_code_1"),
  max_urls_per_query = 3,
  tries = 2
)

DBI::dbDisconnect(con, shutdown = TRUE)
```

## Matching Strategies

### Strategy Comparison

| Strategy   | Threshold | Use Case                                 |
|------------|-----------|------------------------------------------|
| `"strict"` | JW ≥ 0.90 | High precision, minimize false positives |
| `"loose"`  | JW ≥ 0.85 | Higher recall, catch variant spellings   |

### City Mode Options

| Mode        | Behavior                        |
|-------------|---------------------------------|
| `"ignore"`  | Match on name + state only      |
| `"prefer"`  | Boost ranking when city matches |
| `"require"` | Only return exact city matches  |

## Data Partitioning

The Parquet lake is partitioned by:

- **`state_part`**: 50 US states + DC + territories + `INTL`
- **`lname_initial`**: A-Z, 0-9, URL-encoded specials (`%23`, `%2F`),
  `_`

This creates ~1,500 shards, enabling **selective reads** of only
relevant data.

## Configuration

### Custom Snapshots

Point to a specific snapshot version:

``` r
Sys.setenv(TNP_MANIFEST_URL = "https://rnppes.org/nppes/2025-10-13/manifest_state_files.json")

# Or use the latest pointer
Sys.setenv(TNP_LATEST_URL = "https://rnppes.org/nppes/latest.txt")
```

### Retry Policy

Adjust backoff for rate-limited endpoints:

``` r
results <- npi_enrich(
  inputs = inputs,
  con = con,
  man = man,
  tries = 6,                 # More retry attempts
  initial_wait = 0.8,        # Longer initial wait
  max_wait = 8,              # Higher max backoff
  sleep_between_batches = 0.5
)
```

## Performance Tips

1.  **Batch inputs**: Process 10-50 records at a time during testing
2.  **Increase `max_urls_per_query`**: Set to 3-5 for faster processing
    (if no rate limits)
3.  **Use `city_mode = "ignore"`**: Simplifies matching when city data
    is unreliable
4.  **Limit `max_candidates`**: Set to 1 if you only need the best match
5.  **Pin snapshot versions**: Use specific dates for reproducible
    analyses

## Example Workflow

See [`demo_tidynpi.qmd`](demo_tidynpi.qmd) for a complete example
including:

- Loading demo data from GitHub
- Running enrichment with validation
- Taxonomy translation and visualization
- Accuracy metrics against ground truth NPIs

## FAQ

**Q: Why DuckDB + Parquet over HTTPS?**  
A: Zero server-side setup, vectorized columnar reads, efficient column
selection, and pushdown filters.

**Q: How big is a shard?**  
A: Shards are partitioned by state × initial. A single shard (e.g., NY ×
S) is typically manageable for selective queries.

**Q: Can I use this offline?**  
A: You can download the manifest and parquet files locally. Just point
`TNP_MANIFEST_URL` to your local path.

**Q: Do I need credentials?**  
A: No! The public snapshot is served over HTTPS with no authentication
required.

**Q: How often is the data updated?**  
A: Check the hosted snapshots at
[rnppes.org](https://rnppes.org/nppes/). Each snapshot is immutable with
a dated path.

## Development

``` r
# Run tests
devtools::test()

# Check package
devtools::check()

# Update documentation
devtools::document()
```

## Related Work

This package is designed for **batch analytics** and **reproducible
research**. For interactive single-record lookups, consider the official
[NPI Registry API](https://npiregistry.cms.hhs.gov/api/).

## License

MIT © 2025 Luis Muñoz Grass

## Citation

If you use tidynpi in your research, please cite:

    Muñoz Grass, L. (2025). tidynpi: Efficient NPPES Provider Lookup 
    via Parquet Lake and DuckDB. R package version 0.0.0.9000.
    https://github.com/Lfirenzeg/tidynpi
