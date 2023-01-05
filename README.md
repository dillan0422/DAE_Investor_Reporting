# EQUITY INVESTORS REPORT REPOSITORY

This directory contains the projects to create the tables and files needed for the `Equity Investors Report`.

The two projects are
* 1) equity_investors_report (dbt project to populate Redshift tables)
* 2) investors_report_desing (Python scripts to populate the report/data pack)

First, it is need to populate the tables in the `portfolio_report` schema. To do so, the bash script `investors_report_dbt.sh` needs to be run.

Then, to create the necessary spreadsheets for the report/data pack, the bash script `investors_report_sheets.sh` needs to be run.

For further details and instructions for each project, please refer to:
* 1) Equity Investors Report Instructions
* 2) Investors Report Design Instructions

For any questions, please contact:
- Dillan Aguirre (dillan.aguirre@storicard.com)