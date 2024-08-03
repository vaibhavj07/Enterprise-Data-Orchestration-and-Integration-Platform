#!/bin/bash


source airflow-venv-main/bin/activate

airflow webserver -p 8080 &

airflow scheduler &


