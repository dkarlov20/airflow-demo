# GridU Airflow  
  
## Run  

### Run  Airflow in Docker

#### First time:

 1. Execute following commands in **_docker_compose_** folder:
	```
	docker-compose up postgres
	docker-compose up initdb
	docker-compose up --scale worker=3 webserver scheduler redis flower
	```
 
 2. Create a directory **_shared/_**:
	```
	mkdir <project_directory>/shared
    ```
		
 3. Update **_postgres_default_** connection in Airflow admin:
      * go to the [Airflow connection panel](http://localhost:8080/admin/connection/)
      * edit **_postgres_default_**
      * set _login_=**airflow** and _port_=**5432**
     
#### Next times:
 4. Execute following command in **_docker_compose_** folder:
	```
	 docker-compose up -d --scale worker=3 postgres webserver scheduler redis flower
	```
	
### Run  pipeline
 1. Enable **_dag_1_** and **_trigger_dag_** in [Airflow admin panel](http://localhost:8080/admin/)
 2. Trigger **_trigger_dag_**
 3. Put trigger file in **_<project_directory>/shared_**

## Variables  

| Key        	| Value                      | Description                                        |
| ------------- |----------------------------|----------------------------------------------------|
| file_path     | /usr/local/airflow/shared/ | Path to directory where trigger files will be put  |
| triggered_dag | dag_1                      | Dag_id of the dag that trigger_dag will be trigger |