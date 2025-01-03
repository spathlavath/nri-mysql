#!/bin/bash
set -e

# Run the MySQL default entrypoint setup
/usr/local/bin/docker-entrypoint.sh mysqld &

# Wait for MySQL to be fully initialized and ready
until mysqladmin ping --silent; do
    echo "Waiting for MySQL server..."
    sleep 2
done

# Populated Employees Sample Data
echo "started copying sample employees data"

mysql -u root -p${MYSQL_ROOT_PASSWORD} -t < employees.sql

echo "completed copying sample employees data"

# Execute slow queries
echo "started executing slow queries"
mysql -u root -p${MYSQL_ROOT_PASSWORD} -e "
USE employees;
SELECT e.emp_no, e.first_name, e.last_name
FROM employees e
WHERE EXISTS (
    SELECT 1
    FROM salaries s
    WHERE s.emp_no = e.emp_no
    AND s.salary > (
        SELECT AVG(salary)
        FROM salaries
        WHERE to_date = '9999-01-01'
    )
);
"

mysql -u root -p${MYSQL_ROOT_PASSWORD} -e "
USE employees;
SELECT e.emp_no, e.first_name, e.last_name, 
       (SELECT COUNT(*) FROM dept_emp de WHERE de.emp_no = e.emp_no) AS dept_count,
       (SELECT AVG(salary) FROM salaries s WHERE s.emp_no = e.emp_no AND s.to_date = '9999-01-01') AS avg_salary
FROM employees e
ORDER BY avg_salary DESC
LIMIT 100;
"
echo "finshed executing slow queries"

# Handle foreground execution to keep the container running
wait