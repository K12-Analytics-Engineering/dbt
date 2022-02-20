# dbt

## Development Environment
After you clone this repo, poetry can be used to create a python virtual environment with all dependencies installed.

```bash

poetry env use 3.9;
poetry install;
env $(cat .env) poetry shell;
dbt deps;

```
