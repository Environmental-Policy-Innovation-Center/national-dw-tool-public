![](www/epic-logo-transparent.png)

# EPIC's Drinking Water Explorer Tool

This repository maintains the backend data pipeline powering the National Drinking Water Tool. Every dataset the tool uses is defined as an entry in [`main_config.json`](main_config.json) and processed by [`main_runner.R`](main_runner.R), which pulls, cleans, registers, and stages data on a per-dataset basis.

## How the pipeline works

Each dataset has a unique `dataset_id` in `main_config.json` (e.g. `raw_sdwa`, `clean_sdwis_viols`, `merged_national_bwn_summary`) with metadata like its S3 output path (`link`), `staged_link`, `input_links`, and downstream `triggers`. Dataset ids contain one of three prefixes:

-   **`raw_*`** - pulls a dataset from its original source (an API, a state website, a bulk download) and writes it to S3 with little to no transformation.
-   **`clean_*`** - reads one or more `raw_*` (or other `clean_*`) outputs, performs cleaning and filtering, and writes a tool-ready dataset. If it has a `staged_link`, it's a staged dataset for the tool.
-   **`merged_*`** - combines multiple `clean_*` datasets into a national or cross-dataset summary (e.g. `merged_national_bwn_summary` combines every state's Boil Water Notice data).

Running a `dataset_id` through `main_runner.R` does up to three things:

1. **Run the pipeline** - calls that dataset's pipeline function (see [Pipelines](#pipelines) below) to actually fetch, clean, and write its data.
2. **Sync registries and stage data** - updates the variable registry (schema + quality checks) and dataset registry (dataset level metadata), and copies the output to its `staged_link` if it passes checks. See [`functions/registry_updates.R`](functions/registry_updates.R).
3. **Run downstream dependencies (triggers)** - recursively repeats steps 1-2 for every `dataset_id` listed in that dataset's `triggers`, so a single command can refresh data for all downstream dependencies (e.g. running `raw_sdwa` can also rerun `clean_sdwis_viols`).

### Repo layout

-   **[`pipelines/`](pipelines)** - one `.R` file per dataset family, each defining the `run_*_pipeline()` functions that do the actual downloading/cleaning for its `raw_*`/`clean_*`/`merged_*` dataset ids.
-   **[`functions/`](functions)** - shared helpers sourced by `main_runner.R` and used across pipelines: S3 I/O ([`s3_client.R`](functions/s3_client.R)), registry updates and staging ([`registry_updates.R`](functions/registry_updates.R)), quality checks ([`checks.R`](functions/checks.R)), Boil Water Notice helpers ([`bwn_helpers.R`](functions/bwn_helpers.R)), census crosswalking ([`census_xwalk_helpers.R`](functions/census_xwalk_helpers.R), [`xwalk_census_geo_sabs.R`](functions/xwalk_census_geo_sabs.R)), spatial coverage ([`spatial_coverage.R`](functions/spatial_coverage.R)), and other miscellaneous pipeline utilities ([`pipeline_helpers.R`](functions/pipeline_helpers.R)).
-   **[`main_config.json`](main_config.json)** - the single source of truth for every dataset's metadata, S3 paths, and trigger chains.
-   **[`main_runner.R`](main_runner.R)** - the CLI entry point.
-   **[`scripts/`](scripts)** - one-off and maintenance scripts that aren't part of the per-dataset pipeline.posts.

## Running pipelines

`main_runner.R` is run from the repo root with `Rscript` and has the following CLI flags:

| Flag | Default | Description |
|---|---|---|
| `--run-pipeline` | (none) | Required. The `dataset_id` to run, e.g. `raw_sdwa`. |
| `--update-registries` | `TRUE` | Optional. Whether to sync the variable/dataset registries and stage data after the pipeline runs. |
| `--skip-pipeline` | `FALSE` | Optional. Skip running the pipeline function itself. Registry updates (and triggers) still run normally - useful for refreshing registries without re-pulling data. |
| `--run-triggers` | `TRUE` | Optional. Whether to cascade into the dataset's downstream `triggers` after it succeeds. |
| `--dev` | `FALSE` | Optional. Route all S3 reads/writes to the development bucket instead of production. |

### Example commands

Run a single dataset's full pipeline, sync its registries, and cascade to its triggers:

```bash
Rscript main_runner.R --run-pipeline raw_sdwa
```

Same as above, but run with dev bucket instead of prod:

```bash
Rscript main_runner.R --run-pipeline raw_sdwa --dev TRUE
```

Refresh a dataset's registries/staging without re-running its pipeline or its triggers:

```bash
Rscript main_runner.R --run-pipeline clean_sdwis_viols --skip-pipeline TRUE --run-triggers FALSE
```

Run a pipeline but skip registry/staging updates entirely (data-only run):

```bash
Rscript main_runner.R --run-pipeline raw_ma_bwn --update-registries FALSE
```

## Maintenance scripts

[`scripts/`](scripts) holds standalone scripts for tasks that fall outside the dataset pipeline `main_runner.R` flow:

-   [`sync_main_config.R`](scripts/sync_main_config.R) - merges local edits to `main_config.json` with the version in S3. This helps with handling merge conflicts if multiple people are editing the file locally.
-   [`generate_national_rdata_lists.R`](scripts/generate_national_rdata_lists.R) - archives and generates the `national_*.RData` bundles (`national_water_system`, `national_bwn`, `national_environmental`, `national_socioeconomic`) from current pipeline outputs.
-   [`staged_prod_check.R`](scripts/staged_prod_check.R) - compares every staged dataset against its production baseline and writes a pass/fail report before promoting staging to prod.
-   [`generate_public_data_downloads.R`](scripts/generate_public_data_downloads.R) - builds the internal and public methodology docs and bundles staged datasets into national/state zip files for the tool's public data downloads.

Each script can be run from the repo root. For example:

```bash
Rscript scripts/staged_prod_check.R --dry-run
```

## AWS Automated Tasks

The [`Dockerfile`](Dockerfile) builds an image whose entrypoint is `Rscript main_runner.R`. AWS ECS task definitions provide a container command override to run specific scheduled pipelines. For example:

```json
["--run-pipeline", "raw_huc12", "--update-registries", "TRUE", "--dev", "TRUE"]
```
