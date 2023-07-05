# TestLBC

## Installation steps

- Clone this repo
- Install docker on your machine (I personnally used Windows 10 for this exercise so here's the link that I used for my installation : https://docs.docker.com/desktop/install/windows-install/)
- Open a terminal and go the root directory of the repo, where the docker-compose file is located
- Run : docker-compose build
- Run : docker-compose up
- Open a browser to localhost:8080 to access the webserver -> Default login is airflow/airflow
- Create a Google Cloud connection in Admin > Connections as : <br />
        name: "gcp" <br />
        keyfile_path: "GCP_service_account_key.json"

## Execution
To run a command in the container, use the docker exec command <br />
- For this exercise I created two dags, one for the crawling, and one for the loading into BigQuery
- Both dags are scheduled only once so they can executed directly in the Airflow UI, but if you want to run them manually you can use the following command : <br />
      $docker exec airflow dags backfill {articles_crawling or articles_loading} -s 2023-07-01 

The BQ project where the tables are stored : 
https://console.cloud.google.com/bigquery?hl=fr&organizationId=0&project=test-lbc-391414

I will need to manually add your account to the project for you to access it, send me the email address that you wish to access BQ with.

As for the question asking to find the top 5 movies by genre, just run this query in BQ : 
```
SELECT 
    genre, 
    headline, 
    ROW_NUMBER() OVER(PARTITION BY genre ORDER BY rating DESC) AS rank 
FROM dwh_tmp.articles_cleaned_20230701 
WHERE type = 'Review' 
QUALIFY rank <= 5
```

## Improvements
Now to answer the questions in the instructions :

### How you can setup this kind of pipeline in production?
-> I think I sort of did. This is just a dev environment, but with the correct infrastructure this solution can be deployed in production, using Airflow to schedule different pipelines and GCP as a cloud storage and database solution.

### What are the limitation of this solution?
-> I honestly don't know. I basically reproduced the solution and environement that I've been using profesionnally for 3 years, so I'd like to think that it is viable. Airflow offers a lot of flexibility in terms of scheduling, dependencies, and a lot of versatility through custom hooks and operators, we can basically orchestrate anything with it.

### How you can improve it, using an other technology or an other pattern?
-> Due to time limitations I developed a very basic solution, but it can be improved quite a lot if we start to think about scalability. First I would use a cleaner method of loading to BQ, using GCS as a cold storage intermediary, so that we can keep historical data and replay the pipelines if we ever need to (to apply some new transformations retroactively for example). I would also add some cleaning SQL scripts after the loading into BQ. And if these dags were to used for other cases I could create a templated dag and/or some templated scripts.

### What are the advantages and disadvantages of using a distributed data processing framework in this situation?
-> I think that theoritically, centralized data processing is more cost-efficient, as it requires less hardware resources, whereas distributed data processing is less time consuming. In this exercise, the amount of data to ingest is pretty low anyway so I don't think it matters much. Infrastructure questions are not my strong suit and I've only worked with distributed data processing so I can't really tell. 



