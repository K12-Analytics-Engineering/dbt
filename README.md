# dbt

## Development Environment
After you clone this repo, poetry can be used to create a python virtual environment with all dependencies installed.

```bash

poetry env use 3.9.10;
poetry install;
# TODO copy and complete .env file
env $(cat .env) poetry shell;
code .
dbt deps;
dbt run-operation stage_external_sources --vars "ext_full_refresh: true";

```
