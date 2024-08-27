#!/bin/bash

read -p "Enter 1 to run the script every 30 seconds or enter 2 to run it every week: " selection

script_path="/path-to-rust-executable-file" # path to the rust executable file
logfile="/path-to-log-file" #path to the log file

case $selection in
    1)
        cron_expr="* * * * *"
        # Write the cron job to a temporary file
        temp_file=$(mktemp)
        echo "$cron_expr (sleep 30 ; $script_path > $logfile 2>&1)" > $temp_file
        ;;
    2)
        cron_expr="0 0 * * 0"
        # Write the cron job to a temporary file
        temp_file=$(mktemp)
        echo "$cron_expr $script_path > $logfile 2>&1" > $temp_file
        ;;
        # To run the file every week
    *)
        echo "Invalid selection"
        exit 1
        ;;
esac

# Install the cron job
crontab $temp_file
echo "Cron job installed successfully"

# Clean up
rm $temp_file
