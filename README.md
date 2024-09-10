# To run the project make sure you have python3.10, docker & pip installed
# then run the following steps.


1. clone the repository ```git clone <link to repository> .```
2. cd vitamojo/
3. Run ```pip install -r requirements.txt```
4. Run ```docker run --rm -P -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD="1234" --name pg postgres:alpine```
5. In a separate terminal run ```python etl.py```


## You can find the queries separately in the queries.sql file. To run the queries.
1. In a terminal run ```docker ps```
2. Use the container_id to run ```docker exec -it <container_id> bash```
3. Log into the postgres inside the container using ```psql -U postgres```
4. Copy the queries one by one, paste and run on the postgres server to see results
