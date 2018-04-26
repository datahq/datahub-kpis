# DataHub KPIs

[![Build Status](https://travis-ci.org/datahq/datahub-kpis.svg?branch=master)](https://travis-ci.org/datahq/datahub-kpis)

Key performance indicators for the DataHub project.

## What does it do?

The script:
- fetches reports from the Google Analytics;
- fetches stats from NPM for `data-cli` package;
- processes data and prints out KPIs.

## Travis-CI and the schedule

The script is run on weekly basis on Travis-CI. For more information see `.travis.yml` file or visit current build page - https://travis-ci.org/datahq/datahub-kpis.

## Output

Below is the sample result:

```
1554 - 00 - Total new users in the site
  30 - 01 - Clicks on "download" from anywhere
   6 - 02 - CLI downloads (from web)
   1 - 03 - First run of the CLI
   2 - 07 - Visit the showcase after push - method 1
 186 - 99 - NPM installs
```
