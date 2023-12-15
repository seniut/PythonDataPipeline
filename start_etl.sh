#!/bin/bash

# Start dwh container
docker-compose up -d dwh

# Optional: Wait for dwh to become healthy
echo "Waiting for dwh service to become healthy..."
docker-compose wait dwh

# Start etl_test container
docker-compose up --build -d etl_test

# Wait for the etl_test container to finish and get its exit code
ETL_TEST_EXIT_CODE=$(docker wait etl_test)

# Display logs for etl_test
echo "Displaying logs for etl_test:"
docker-compose logs etl_test

# Check if etl_test was successful
if [ "$ETL_TEST_EXIT_CODE" -eq 0 ]; then
    echo "ETL tests passed, starting ETL process."
    docker-compose up --build -d etl

    echo "Waiting where etl will be finished"
    docker wait etl

    # Display logs for etl
    echo "Displaying logs for etl:"
    docker-compose logs etl
else
    echo "ETL tests failed, aborting ETL process."
    exit 1
fi
