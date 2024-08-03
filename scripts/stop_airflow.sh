#!/bin/bash

# Find and kill the Airflow web server and scheduler processes
pgrep -f "airflow webserver" | xargs kill
pgrep -f "airflow scheduler" | xargs kill


