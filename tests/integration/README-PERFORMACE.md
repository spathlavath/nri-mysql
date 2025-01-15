## Steps to run Integration in local using docker

1. Run the following commands
    - `cd test/integration`
    - `docker compose -f docker-compose-performance.yml build --no-cache`
    - `docker compose -f docker-compose-performance.yml up`
    - `chmod +x mysql-performance-config/block.sh`
    - `./block.sh` executing this file will create blocking sessions in `mysql_8-0-40` server
2. In the integration_nri-mysql_1 docker container shell execute the integration using the following command
    - `./nri-mysql -username=root -password=DBpwd1234 -hostname=mysql_8-0-40 -port=3306 -verbose=true -enable_query_performance=true -slow_query_fetch_interval=300`
3. Change the hostname, enable_query_performance, slow_query_fetch_interval flags to see the integrations stdout for different scenarios
