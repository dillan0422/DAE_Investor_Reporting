#!/bin/bash

# Script to run the python script to get the applicant geohash

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

. ~/.cred

# Change directory
cd ~/DAE_Investor_Reporting/equity_investors_report

# Run dbt project
dbt run

# Final message
echo "Tables in portfolio_report schema created successfully"