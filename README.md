# Airflow_Mini_Project
In this project I used a pipeline to extract data from yahoo finance and scheduled the process using apache airflow. 

### Prerequisites:
1) download the folder with all the relevant files for docker set up
2) Copy your .py file (dag) to mnt/airflow/dags directory
3) Then execute ./start.sh script. This should build and start all the services.
4) Execute docker-compose ps
5) go to localhost:8080 - you will be presented with the web ui
6) use username/pwd as airflow/airflow
7) After completing you can use ./stop.sh to stop the services
8) ./reset.sh to completely wipe out all the images 
