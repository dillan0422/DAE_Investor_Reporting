# EQUITY INVESTORS REPORT DBT PROJECT

This directory contains the necessary code to create the following tables in the `portfolio_report` schema:
- `revenue_cohort`
- `revenue_sumry`
- `stmt_dq_accts`
- `stmt_dq_sumry`
- `vintage_sumry`

These tables are used to populate:
* 1) Investors Report (Data Pack)
* 2) Financial Model (Quicksight Dashboard)

The tables are created compiling all the SQL queries in the dbt project. To do so, the bash script `investors_report_dbt.sh` needs to be run.

Once the bash script ran successfully, it is necessary to review the created tables in the `portfolio_report` schema and to run data validation process.

If the data validation process is successful:
* 1) Refresh the Quicksight Dashboard of the Financial Model and let the Finance Team know.
* 2) Follow the instructions in the `investors_report_design README.md file` to populate the Investors Report (Data Pack)