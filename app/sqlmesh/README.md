# SQLMesh in the data platform
 
This is a short guide for our local SQLMesh setup.
It starts with the deployment and the variables, and ends with useful commands.

## 1. Setup
 
### 1.1 Variables in `.env`
 
The `.env` file is next to `docker-compose.yml`. Never commit it to git.
 
Postgres variables (we already have them):
 
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
Extra variables (create them once, on Linux or WSL2):
 
```bash
echo "UID_GID=$(id -u):$(id -g)" >> .env
echo "DOCKER_GID=$(stat -c %g /var/run/docker.sock)" >> .env
echo "HOST_PROJECT_DIR=$(pwd)" >> .env
tail -n 3 .env
```
 
What they are for:
 
- `UID_GID`: your user id and group id. The container runs as you, so files it creates are yours, not root's.
- `DOCKER_GID`: the group id of the Docker socket. Airflow needs it to start containers.
- `HOST_PROJECT_DIR`: the absolute path of the project on the host. Airflow needs it for mounts.
Do not run these commands twice. The lines would be added twice.
 
### 1.2 Dockerfile
 
File: `app/sqlmesh/Dockerfile`
 
Why not `sqlmesh[postgres]`? The extra installs the plain `psycopg2`.
It builds from source and needs `pg_config`. The slim image does not have it, so the build fails.
`psycopg2-binary` has a ready wheel and works.
 
### 1.3 Compose service
 
```yaml
  sqlmesh:
    build:
      context: ./app/sqlmesh
      dockerfile: Dockerfile
    image: sqlmesh:local
    # profiles: ["tools"]
    user: "${UID_GID}"
    command: sleep infinity
    environment:
      POSTGRES_HOST: ${POSTGRES_HOST}
      POSTGRES_PORT: ${POSTGRES_PORT}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - ./app/sqlmesh:/opt/app/sqlmesh
```
 
Notes:
 
- `command: sleep infinity` keeps the container alive, so we can use `docker compose exec`.
- Without a command, the container starts and stops at once, because the image has no long process.
- To make it optional later: turn `profiles: ["tools"]` on and remove `command`.
  Then start it only with `docker compose run --rm sqlmesh ...`.
- The network `data-platform-net` is the default network of our compose file.
  The service joins it automatically.

### 1.4 Build and start
 
```bash
docker compose build sqlmesh
docker compose up -d sqlmesh
docker compose exec sqlmesh sqlmesh --version
```
 
After you change the Dockerfile, build again with `docker compose build sqlmesh`.
 
Quick test of the Postgres connection, without SQLMesh:
 
```bash
docker run --rm --network data-platform-net --env-file .env sqlmesh:local python -c "
import os, psycopg2
c = psycopg2.connect(host=os.environ['POSTGRES_HOST'], port=os.environ['POSTGRES_PORT'],
                     user=os.environ['POSTGRES_USER'], password=os.environ['POSTGRES_PASSWORD'],
                     dbname=os.environ['POSTGRES_DB'])
print('server version:', c.server_version)
"
```
 
### 1.5 Shortcut
 
```bash
alias sm='docker compose exec sqlmesh sqlmesh'
```
 
The alias lives only in the current terminal.
Add the line to `~/.bashrc` or `~/.zshrc` to keep it.
All examples below use `sm`.
 
### 1.6 Create the project (done once)
 
```bash
docker compose exec sqlmesh sqlmesh init postgres
```
 
- The argument `postgres` is the SQL engine of the project.
- `init` only writes files. It does not connect to the database.
- Choose the `DEFAULT` template.


## 2. Configuration: `config.yaml`
 
The connection reads the values from environment variables.
Keep the quotes, and keep the indentation (2 spaces).
 
```yaml
gateways:
  postgres:
    connection:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST') }}"
      port: "{{ env_var('POSTGRES_PORT') }}"
      user: "{{ env_var('POSTGRES_USER') }}"
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      database: "{{ env_var('POSTGRES_DB') }}"
 
default_gateway: postgres
 
model_defaults:
  dialect: postgres
  start: 2026-09-20
  cron: '@daily'
```
 
Main ideas:
 
- **Gateway**: a named connection. Ours is called `postgres`.
- **Dialect**: the SQL flavor of the models.
- `model_defaults` gives a default to every model. A model can override it (for example `start`).
- We have no `state_connection`, so SQLMesh keeps its state in the same database, in the schema `sqlmesh`.
- Environment variables have the highest priority.
  They can override settings from `config.yaml`.
- If SQLMesh complains about the type of `port`, write the number directly: `port: 5432`.
- The `linter` block turns on a few checks that run during `plan`.
Check the setup with `sm info`. It should say `Data warehouse connection succeeded`.
 

## 4. Key ideas
 
- **Model**: a SQL query plus its settings (name, kind, cron, grain, audits).
- **Kind**: how the data is stored and updated. Examples: `FULL`, `VIEW`, `SEED`, `INCREMENTAL_BY_TIME_RANGE`.
- **Grain**: the columns that make a row unique.
- **Snapshot**: one version of a model. SQLMesh makes a fingerprint (a hash) from the model and its parents.
  A new fingerprint means a new snapshot and a new physical table.
- **Interval**: a time slice of a model, size set by `cron`. SQLMesh remembers which intervals are done.
  An interval counts as ready only after it has ended.
- **Environment**: a set of views that point to physical tables.
  `prod` is the main one. Any other name is a dev environment.
  A new environment is cheap, because data is reused and not copied.
- **Plan**: SQLMesh compares your local code with an environment, shows the difference, and asks you to apply it.
- **Change types**: breaking (downstream models are rebuilt), non-breaking (only the changed model is rebuilt), forward-only (history is not rebuilt).
- **Restatement**: recompute data without any code change.
- **Audit**: a query that looks for bad rows. If it returns a row, the check fails. Audits are blocking by default.
- **Unit test**: a YAML file with fake input and expected output. Tests run on DuckDB, not on Postgres.
- **State**: SQLMesh metadata (snapshots, intervals, environments). It is stored in Postgres, schema `sqlmesh`.

## 5. What SQLMesh creates in Postgres
 
- `sqlmesh`: the state tables.
- `sqlmesh__<schema>`: physical tables. Names include a version number.
- `<schema>`: views for `prod`. Example: `sqlmesh_example.full_model`.
- `<schema>__<env>`: views for a dev environment. Example: `sqlmesh_example__dev.full_model`.
SQLMesh does not touch other schemas (`public` and others).
 
Look at the schemas:
 
```bash
sm fetchdf "select schema_name from information_schema.schemata order by 1"
sm fetchdf "select table_schema, table_name, table_type from information_schema.tables where table_schema like 'sqlmesh%' order by 1, 2"
```
 
## 6. Daily workflow
 
1. Edit a model file.
2. See the final SQL: `sm render <model>`.
3. Run the unit tests: `sm test`.
4. Make a dev plan: `sm plan dev`. Read the difference and apply it.
5. Check the data: `sm fetchdf "select * from <schema>__dev.<model>"`.
6. Compare with prod: `sm table_diff prod:dev <model>`.
7. If all is good, apply the change to prod: `sm plan`.
8. Scheduled runs (later from Airflow): `sm run`.
Important: `sm plan` without a name works on `prod`.
Use `sm plan dev` when you only want to test.
 

## 7. Command cheat sheet
 
Use `sm <command> --help` to see all options.
 
### Look and check
 
- `sm info`: project stats and connection test.
- `sm render <model>`: show the final SQL. Macro dates show `1970-01-01` unless you pass a range (see `sm render --help`).
- `sm fetchdf "<sql>"`: run a query and print the result.
- `sm table_name <model>`: show the physical table behind a model.
- `sm test`: run unit tests.
- `sm audit`: run audits.

### Plan
 
- `sm plan`: plan for `prod`.
- `sm plan dev`: plan for the `dev` environment (created if it does not exist).
- Answer the question with a capital `Y`. A small `y` is not accepted.
- `sm plan --auto-apply`: do not ask for confirmation.
- `sm plan --no-prompts`: turn off interactive questions.
- `sm plan --skip-tests`: skip unit tests.
- `sm plan --skip-backfill`: do not compute data.
- `sm plan --forward-only`: make a forward-only plan.
- `sm plan --start 2020-01-01 --end 2020-01-05`: set the date range.

### Choose models and recompute
 
- `sm plan --restate-model <model>`: recompute a model and all models below it. This is like a full refresh.
  Add `--start` and `--end` to limit the dates.
  Some model kinds cannot restate a part of the history. Then SQLMesh widens the range and prints a warning.
- Upstream models are not restated. To include them, name the highest model you need.
- `sm plan dev --select-model "<model>"`: only include selected changed models in the plan.
  It works on models with code changes. It does not force a recompute.
- Selector syntax:
  - `"+model"`: model and changed models above it.
  - `"model+"`: model and changed models below it.
  - `"schema.*_model"`: wildcard.
  - `"tag:name"`: by tag.
  - `"git:branch"`: models changed compared to a git branch.
  - `&`, `|`, `^`: and, or, not.
- `sm plan dev --backfill-model "<model>"`: limit which models are computed. Works only in dev environments.

### Compare
 
- `sm table_diff prod:dev <model>`: compare a model between two environments (not tested yet).
### Run
 
- `sm run`: compute the intervals that are ready, for models already applied in `prod`.
  If no new interval is ready, it does nothing. It ignores local code that is not applied.
- Run it as often as the smallest `cron` in the project.
### Maintenance (not tested yet)
 
- `sm environments`: list environments.
- `sm invalidate <env>`: mark a dev environment for deletion.
- `sm janitor`: clean up expired environments and old tables.
- `sm migrate`: update the state after a SQLMesh upgrade.
- `sm clean`: clean the local cache.
- `sm create_external_models`: write `external_models.yaml` for tables that SQLMesh does not manage (for example, tables written by Spark).
- `sm format`: format the model files.
- `sm destroy`: remove all project resources. Be careful.

## 8. How to read `sm plan`
 
Example output parts:
 
- `Successfully Ran N tests against duckdb`: unit tests passed.
- `prod environment will be initialized`: this is the first plan.
- `Models: Added / Directly Modified / Indirectly Modified`:
  - Added: new models.
  - Directly Modified: you changed the code.
  - Indirectly Modified: the model is downstream of a changed model.
- `Models needing backfill`: what will be computed.
  - `[full refresh]`: the whole table is rebuilt.
  - `[2020-01-01 - 2026-09-20]`: the interval range that will be processed.
- `Apply - Backfill Tables [y/n]`: type `Y` to apply.
- `[1/1] model [insert ...]`: one model finished. `audits ✔1` means one audit passed.
- `Updating virtual layer`: the views of the environment are switched to the new tables.
If there is nothing to do, you see `No changes to plan`.
 
## 10. Airflow (planned, not done yet)
 
The plan: Airflow starts a short-lived container from the image `sqlmesh:local` and runs `sqlmesh run`.
It uses `DockerOperator`.
 
What is already prepared:
 
- The Airflow image has `apache-airflow-providers-docker` (4.3.1) and the Python package `docker` (7.1.0).
- The compose service `airflow` mounts `/var/run/docker.sock`, has `group_add: "${DOCKER_GID}"` and the variable `HOST_PROJECT_DIR`.
What is still to do: write the DAG.
 
How it works:
 
- The Docker socket is the door to the Docker daemon on the host. Airflow asks the daemon to start a new container.
  The new container is a neighbor of Airflow, not a child.
- `group_add` gives the Airflow user the group that owns the socket. Without it: `Permission denied`.
- `HOST_PROJECT_DIR` is needed because the daemon reads mount paths on the host.
  Inside Airflow the project is at `/opt/app/sqlmesh`, and the host does not have this path.
- The compose service `sqlmesh` does not need to run. Airflow only needs the image to exist.
  After a Dockerfile change: `docker compose build sqlmesh`.
- The new container gets nothing from Airflow. Set these in the operator:
  `image`, `command`, `network_mode="data-platform-net"`, a bind mount with the host path,
  the `POSTGRES_*` variables, and the same `user` as in compose.
  Also set `mount_tmp_dir=False` and `auto_remove="success"`.
- Airflow runs only `sqlmesh run`. Changes are applied by hand with `sm plan`.
- Open-source SQLMesh has no built-in Airflow integration. The old one was deprecated.
  The current Airflow page in the docs is for the paid Tobiko Cloud.
Security notes:
 
- Access to the Docker socket is like root access to the host. Use it only on a personal machine.
- Ports of the services are bound to `127.0.0.1`, so they are not open to the network.
- `.env` must be in `.gitignore`.