**Hello people =)**

## **Solution outline**

This app is designed to export data from [Punk API](https://punkapi.com/documentation/v2), transform it and load into 
DWH for further analysis. 

It runs in containers using Docker Compose. In order to launch it on your machine, 
please install the following soft:
* [Docker](https://docs.docker.com/get-docker/)
* [Docker Compose](https://docs.docker.com/compose/install/)


### **Pipeline**

1. *landing layer*: data from the API is exported into `/object_storage` folder as JSONL file
2. *raw data layer*: json data is parsed and inserted into DWH. it's now available for querying
3. *core layer*: raw data is organized into simple model
4. *data mart layer*: aggregated views are created on top of core data

### **Initialization**

1. open CMD/Terminal inside the root folder `/BEER_FFS`
2. run `docker-compose up airflow-init` to initialize Airflow
3. after init finished run `docker-compose up -d`
4. PROFIT (wait 1-3 mins before everything is ready. run `docker ps` for more info)
5. open airflow-webserver page [localhost](localhost:8080)
6. login using username: *airflow*  password: *airflow*
7. open the only dag and turn it on (also check *Graph* section for the pipeline lineage)
8. that's it. the data should already be in the DWH
9. using DB viewer([Dbeaver](https://dbeaver.io/download/) for example) of your choice connect to postgres:
   ```
    host: localhost
    port: 54320
    database: BEER
    username: hotdog
    password: ketchup
10. query freshly created views