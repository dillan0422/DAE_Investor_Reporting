#!/bin/bash

# Script to run the python script to get the applicant geohash

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

. ~/.cred

# Change directory
cd ~/DAE_Investor_Reporting/investors_report_design

# Check if datapack_spreadsheets directory exists
DIR=./datapack_spreadsheets

if [ -d "$DIR" ]; then
    echo "Running python scripts to create data pack spreadsheets"
else
	mkdir datapack_spreadsheets
    echo "$DIR created.\nRunning python scripts to create data pack spreadsheets"
fi

# Run python scripts
python3 datapack_summary.py
python3 datapack_usage_pivot.py
python3 datapack_dq_pivot.py
python3 datapack_revenue_pivot.py

# Final message
echo "Datapack spreadsheets successfully created in investors_report_desing/datapack_spreadsheets"