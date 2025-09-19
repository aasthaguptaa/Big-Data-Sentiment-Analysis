# Sentiment Analysis

## Table of Contents

-   [Overview](#overview)
-   [Architecture](#architecture)
-   [Prerequisites](#prerequisites)
-   [Quick Start](#quick-start)
-   [Accessing Services](#accessing-services)
-   [Common errors & quick fixes](#common-errors--quick-fixes)

## Overview

– This project implements a sentiment analysis pipeline using Kafka, Flink, Elasticsearch, and Kibana. The pipeline processes Reddit comments, analyzes their sentiment, and visualizes the results in a Kibana dashboard. The user can type two keywords to see the sentiment change over time in the graph.

## Architecture

– Reddit Comments Dataset -> Kafka -> Flink -> Elasticsearch ->Kibana

1. **Kafka**: Acts as the message broker for sending Reddit comments data to Flink.
2. **Flink**: Preprocesses the comments and performs model inference.
3. **Elasticsearch**: Stores the processed data for querying to be pushed to Kibana
4. **Kibana**: Visualizes the sentiment analysis results(One line for each keyword).

## Prerequisites

1. For Cloud Run:
    - Web browser and Internet connection.
2. For Local Run:
    - Docker Desktop App and VsCode

## Quick Start
1. For Cloud Run:
    - Kibana Dashboard: http://34.32.114.98:5601
    - Flink Dashboard: http://34.32.114.98:8081

    - Backup Access to Dashboards (If the above IP addresses are not working):
        - Kibana: http://34.63.30.113:5601
        - Flink: http://34.63.30.113:8081
2. For Local Run:
    - git clone "https://collaborating.tuhh.de/e-19/teaching/bd25_project_a11_a"
    - docker-compose up --build -d
    - Before starting the job, make sure the mapping on Elasticsearch is set. For this you can use the following command:
  ```bash
curl -X PUT "http://localhost:9200/sentiment_analysis" \
     -H 'Content-Type: application/json' \
     -d '{
  "mappings": {
    "properties": {
      "author":      { "type": "keyword" },
      "body":        { "type": "text"    },
      "controversiality": { "type": "integer" },
      "created_utc": { "type": "date", "format": "epoch_second" },
      "id":          { "type": "keyword" },
      "ingest_time": { "type": "date"    },
      "score":       { "type": "integer" },
      "sentiment":   { "type": "float"   },
      "subreddit":   { "type": "keyword" }
    }
  }
}'
  ```

2. Start the Flink job:
    ```bash
    docker-compose exec jobmanager flink run -py /project/jobs/flink_sentiment_job_es.py --detached
    ```
    - Go to Flink Dashboard: http://localhost:8081(To check the job is running)
    - Go to Kibana Dashboard: http://localhost:5601
        - Make sure Stack Management->Index Patterns is set to sentiment_analysis
        - Youtube Video: https://youtu.be/9JsPqcRjmWo
## Accessing Services

– Flink Dashboard: http://34.32.114.98:8081 (port 8081)
– Kibana :http://34.32.114.98:5601 (port 5601)
- Docker Hub : https://hub.docker.com/u/ozanermis

## Common errors & quick fixes  
– Elasticsearch: If you can't see the data on Kibana Dashboard;
    - Go to Stack Management -> Index Management -> Sentiment_Analysis -> Mapping check the data 
    types of "created_utc" and "sentiment"
- Docker : If the producer container crashes: Make sure The value of ```sh
env.set_parallelism(10)  
taskmanager.numberOfTaskSlots: 10 
!Depending on your system, you can increase or decrease the parallelism and task slots.

