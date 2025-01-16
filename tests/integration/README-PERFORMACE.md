## Steps to run Integration in local using docker

1. Run the following commands
    - `cd test/integration`
    - `docker compose -f docker-compose-performance.yml build --no-cache`
    - `docker compose -f docker-compose-performance.yml up`
    - once all the containers are up and running procced to next steps. (verify by checking the last log of the mysql containers it should be `finished executing blocking session queries`)
    - `chmod +x mysql-performance-config/block.sh`
    - `./block.sh` executing this file will create blocking sessions in `mysql_8-0-40` server
2. In the integration_nri-mysql_perf_1 docker container shell execute the integration using the following command
    - `./nri-mysql -username=root -password=DBpwd1234 -hostname=mysql_8-0-40 -port=3306 -verbose=true -enable_query_performance=true -slow_query_fetch_interval=300`
3. Change the hostname, enable_query_performance, slow_query_fetch_interval flags to see the integrations stdout for different scenarios


## Performance Integration test setup

1. A custom image is built for mysql server to enable performance extensions/flags [Dockerfile](./mysql-performance-config/versions/8.0.40/Dockerfile)
2. The entrypoint of the custom image is modified to populate sample data in the mysql server, execute slow queries and blocking sessions queries
3. These custom Dockerfiles are used in [Docker Compose Performance](./docker-compose-performance.yml)
4. Once the mysql containers are up and running.
5. [Performance Integration tests](./performance_integration_test.go) executes the binary of the nri-mysql integration with the above mysql container details and validate if the six output jsons mach the [defined schemas](./json-schema-performance-files/).
