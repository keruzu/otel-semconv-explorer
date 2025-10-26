# otel-semconv-explorer

## Usage

1. Clone this repo.
2. Install pre-requisites: eg `python -m venv venv; source venv/bin/activate; pip install -r requirements.txt`
2. Run `python explore.py` on the root level.
3. Open your browser to `http://127.0.0.1:5000/graphql`.

## Kuzu DB References
[Deep Dive Into Kuzu Explorer](https://youtu.be/yKcVV_bhBTo?si=OG6MnSJ3L-LqMlb7)

Blog: https://betterprogramming.pub/coding-a-graphql-api-with-python-af74919e7f90

## Show tables

Here's the command to show all of the tables:

    CALL show_tables() RETURN *;

## Import the Data

Here's how to import the data

    COPY Metric from 'metrics.json'

# Show All Rows in Table

    MATCH (n: Metric)
    RETURN n;
