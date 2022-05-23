harvardx-bigquery-export
===
Remove PII from BigQuery tables and export them as CSV files

## Installation

Ensure you have Python 3.10.4 installed (as specified in `.python-version`), configure your [Poetry](https://python-poetry.org/) environment to use the correct Python interpreter, and install the dependencies:

```sh
pyenv install 3.10.4
poetry env use $(pyenv which python)
poetry install
```

## Environment
Download an appropriate Google Cloud service account key to your file system, then copy `.env.dist` as `.env` and fill in the values.

## Configuration

Copy `config.toml.dist` as `config.toml` and enter the course IDs and queries you want to perform using the template.

* Enter as many `course_ids` and `[[table]]` blocks as required.
* Select from `full_table_name` in the query. The fully-qualified table name will be interpolated at runtime.


## Running

```sh
poetry run python app.py
```

The requested CSV files will be written to `<date>/exported/<course_id>/<table_name>.csv`.
