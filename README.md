# sample-graphql

# Usage

1. Clone this repo.
2. Run `python main.py` on the root level. If you have `pipenv` feel free to run that way with the included `Pipfile`.
3. Open your browser to `http://127.0.0.1:5000/graphql`.

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