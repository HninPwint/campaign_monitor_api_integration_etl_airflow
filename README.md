
#  Campaign Monitor API Integration ETL via ![image](https://user-images.githubusercontent.com/14988972/126754341-95bfec58-3578-4472-b0f1-a10ef6946bed.png)


### Project Overview
This project aims to orchestrate schedued ETL process of Data extraction from the marketing thrid party's API ( Campaign Monitor), loaded in to the on-premise PostgreSQL data warehouse
with Airflow.

### Project Set Up
* to launch the docker container, ``` docker-compose up -d ```
* Once the docker is running, in the web browser http://localhost:8080/ 
* Username/password: airflow/airflow

### Airflow Dashbord
![image](https://user-images.githubusercontent.com/14988972/128623091-62013a68-8943-4dad-adb2-4826dc2cf3b5.png)


### DAG (1)
![image](https://user-images.githubusercontent.com/14988972/126753027-8506eb68-9fa4-4532-96f2-bb323f583040.png)

### DAG (2)
![image](https://user-images.githubusercontent.com/14988972/126758283-089b77fb-9a90-459d-9a18-d6338120703e.png)

![image](https://user-images.githubusercontent.com/14988972/128624024-8ff67f27-b7ca-426e-b607-0f6e53bec731.png)



### List View of DAG (2)
![image](https://user-images.githubusercontent.com/14988972/128623205-a6578cf1-945c-4fc6-9f4b-1b263b8fa7b3.png)
