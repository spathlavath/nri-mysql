FROM mysql:9.1
# Create and set permissions for log directory
RUN mkdir -p /var/log/mysql && \
    chown -R mysql:mysql /var/log/mysql
COPY tests/integration/mysql-performance-config/versions/9.1.0/mysql.conf.cnf /etc/mysql/conf.d/mysql.conf.cnf
COPY tests/integration/mysql-performance-config/init.sql /etc/mysql/init.sql


# Update package lists and install necessary tools
RUN microdnf install -y unzip

# Set the working directory to /tmp
WORKDIR /tmp

# Download and unzip the test_db ZIP archive
RUN curl -L https://github.com/datacharmer/test_db/archive/refs/heads/master.zip -o test_db-master.zip \
    && unzip -o test_db-master.zip \
    && mv test_db-master test_db

# Set the working directory
WORKDIR /tmp/test_db

# Copy the custom entrypoint script to the image
COPY tests/integration/mysql-performance-config/custom-entrypoint.sh /usr/local/bin/custom-entrypoint.sh
RUN chmod +x /usr/local/bin/custom-entrypoint.sh
RUN ls /usr/local/bin/

ENV MYSQL_ROOT_PASSWORD=DBpwd1234
# Replace the default entrypoint with the custom script
ENTRYPOINT ["/usr/local/bin/custom-entrypoint.sh"]