#start airflow
docker run -it -p 8080:8080 <image_name> start_airflow

#stop airflow
docker run -it <image_name> stop_airflow


#stop airflow
docker run -it <image_name> spark

#run shell script
docker run -it <image_name> shell
