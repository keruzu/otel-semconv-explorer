import marimo

__generated_with = "0.16.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        r"""
    # OpenTelemetry Semantic Conventions
    The [Open Telemetry Semantic Conventions](https://opentelemetry.io/docs/concepts/semantic-conventions/)
    [define](https://github.com/open-telemetry/semantic-conventions/tree/main/model)
    the various metrics and spans, and provide common attributes (aka "dimensions").

    This means that any backend observability tool can consume the standard these observability signals (ie metrics, events, logs, traces, profiles) and know that
    everyone else uses the same units of measure, the same dimensions and the same documentation in dashboards and traces.

    ![Graph schema](graph_schema.png "Graph schema")

    """
    )
    return


@app.cell
def _():
    from polars import DataFrame
    import json
    import logging
    from pathlib import Path
    import marimo as mo

    import yaml
    from pythonjsonlogger.json import JsonFormatter
    import kuzu

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])

    log = logging.getLogger()
    return DataFrame, log, mo


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Node Exploration
    We parse the OTEL semantic conventions and split them out into 'nodes' and 'relations' (ie edges)

    ## Import the Data

    Here's how to import the data
    ```
        COPY Metric from 'metrics.json'
    ```
    """
    )
    return


@app.cell
def _(log):
    from build_semconv_db import PersistenceKuzu
    START_PATH = '../semantic-conventions/model'
    conventions = PersistenceKuzu(log)
    conventions.import_conventions_from_dir(START_PATH)
    conventions.nodes.keys()
    return (conventions,)


@app.cell
def _(conventions, mo):
    data = {
        "nodes": list(conventions.nodes.keys()),
        "relations": list(conventions.relations.keys())
    }
    mo.tree(data, label="Graph Nodes")

    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Metrics
    The OTEL signal 'metrics' have names (generally the same as their id)
    """
    )
    return


@app.cell
def _(DataFrame, conventions):
    metrics = DataFrame(list(conventions.nodes['Metric'].values()))
    metrics = metrics.drop(['attributes', 'entity_associations'])
    metrics
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Attributes
    Attributes are dimensional data that allow us to express a specific metric time series. In most observability products, the number of dimensions (aka cardinality) directly impacts storage and costs.
    """
    )
    return


@app.cell
def _(conventions):

    attributes_json = list(conventions.nodes['Attribute'].values())
    attributes_json
    return


@app.cell
def _(DataFrame, conventions):
    attributes = DataFrame(list(conventions.nodes['Attribute'].values()), strict=False)
    attributes
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Graph Database View
    Here's an example of the possibilities using a graph database, such as 'Kuzu'
    """
    )
    return


@app.cell
def _(conventions):
    conventions.create_db()
    conventions.persist_nodes()
    conventions.persist_relations()

    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Kuzu Explorer
    From the [Kuzu website](https://docs.kuzudb.com/visualization/kuzu-explorer/)

    > Kuzu Explorer is a browser-based frontend to visualize and explore Kuzu database schemas
    > and query results in the form of a graph, table, or JSON. This is a useful tool for exploring 
    > your graph data and debugging your data model during the prototyping phase.

    You can point the Kuzu explorer on the graph datbase with the following command🥇

    ```bash
    podman run -it -p 8000:8000 -v `pwd`/db:/database -e KUZU_FILE=semantic_conventions.kuzu kuzudb/explorer:latest
    ```

    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Examples
    ### Show tables

    Here's the command to show all of the tables:
    ```
        CALL show_tables() RETURN *;
    ```


    ### Show All Rows in Table

    ```
        MATCH (n: Metric)
        RETURN n;
    ```
    ### Show Interesting Attributes
    Find all attributes related to `http`

    ```
    MATCH (a: Attribute)
    WHERE a.brief CONTAINS 'http' OR a.note CONTAINS 'http' OR a.id CONTAINS 'http'
    RETURN a LIMIT 5;
    ```
    """
    )
    return


if __name__ == "__main__":
    app.run()
