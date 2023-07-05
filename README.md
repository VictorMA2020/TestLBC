# TestLBC
Installation steps : 
- Clone this repo
- Install docker on your machine (I personnally used Windows 10 for this exercise so here's the link that I used for my installation : https://docs.docker.com/desktop/install/windows-install/)
- Open a terminal and go the root directory of the repo, where the docker-compose file is located
- Run : docker-compose build
- Run : docker-compose up
- Open a browser to localhost:8080 to access the webserver -> Default login is airflow/airflow
- To run a command in the container, use the docker exec command <br />
    ex : docker exec testlbc-airflow-webserver-1 airflow dags backfill some_dag -s YYYY-MM-JJ

The BQ project where the tables are stored : 
https://console.cloud.google.com/bigquery?hl=fr&organizationId=0&project=test-lbc-391414

I will need to manually add your account to the project for you to access it, send me the email address that you wish to access BQ with.
