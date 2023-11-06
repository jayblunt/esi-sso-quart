# esi-sso-quart

## Introduction

Oringally an SSO for ESI exampple with Quart.

Now multi-corp structure fuel / extraction tracking.

[Eve Online](https://www.eveonline.com/) ESI login example using [Quart](https://quart.palletsprojects.com/en/latest/) (asyncio implementation of the Flask api).

Requires a valid [application](https://developers.eveonline.com/applications) setup.

## System Requirements

- [Python](https://python.org) 3.10 (or 3.11)
- [Redis](https://redis.io) for session state
- [Postgres](https://postgresql.org) for backing store
- Some form of reverse proxy / tls termination. I used [nginx](https://nginx.org/). Reverse proxy must set `X-Forwarded-For` / `X-Forwarded-Proto` headers.

## Python Requirements

This example uses [Quart-Session](https://pypi.org/project/Quart-Session/) which requires a working backend. I used [Redis](https://redis.io) with a default configuration (listening on localhost).

```shell
python3.10 -m venv --upgrade-deps python3.10-env
. python3.10-env/bin/activate
pip install -r requirements.txt
```

## ESI ClientID and ClientSecret Environment Variables

Create an ESI Application at [developers.eveonline.com](https://developers.eveonline.com).

The application uses the following scopes:

- As a User:
  - `publicData`

- As a Contributor (to provide structure / extraction data):
  - `publicData`
  - `esi-characters.read_corporation_roles.v1`
  - `esi-corporations.read_structures.v1`
  - `esi-industry.read_corporation_mining.v1`

Set environent variables for the ClientID and ClientSecret:

```shell
export EVEONLINE_CLIENT_SECRET='<client_secret_from_developers_eveonline_com>'
export EVEONLINE_CLIENT_ID='<client_id_from_developers_eveonline_com'
```

## Database and SQLAlchemy 

Should work with a suitable [SQLAlchemy](https://www.sqlalchemy.org) engine URL. I use:

```shell
export SQLALCHEMY_DB_URL='postgresql+asyncpg://username:password@hostname/database'
```

Setting this up on a local machine was approximately:

```shell
createuser --pwprompt username
createdb --encoding=UTF8 --owner=username database
```

## Permissions / Access Controls

This example includes a very basic permissions system.
Alliances / Corporations / Characters can be included / excluded.
The intent is to be similar / familiar to the way in-game ACLs behave.

Permission is evaluated by checking Alliance then Corporation then Character id against the permission list. Check `AppFunctions.is_permitted` for the details.

**You will want to adjust either the defaults in `tasks/app_access_control_tasks.py`, at a minimum to permit your Character(s) and/or exclude the Alliances and Corporations in my defaults.**

## Open Telemetry

This example can be optionally configured to send telemetry to [honeycomb.io](https://www.honeycomb.io).
Check the [OpenTelemetry for Python](https://docs.honeycomb.io/getting-data-in/opentelemetry/python/) link for an introduction.
For telemetry to be transmitted, install the packages from `requirements.txt` and set the environment variables for your team:
```shell
export OTEL_EXPORTER_OTLP_ENDPOINT="https://api.honeycomb.io/"
export OTEL_EXPORTER_OTLP_HEADERS="x-honeycomb-team=your-api-key"
export OTEL_SERVICE_NAME="your-service-name"
```

## Notes

At startup the example will pull universe data from [ESI](https://esi.evetech.net/).

**This can be slow, especially on the first startup, when the database is empty.**

**Structure information will not populate until this universe data is collected (because of the ORM relationships that are defined between structures and systems and moons).**

There are small "tasks" - coroutines - (via [asyncio.create_task()](https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task) for collecting universe and structure data, and for periodically refreshing jwt keys and access_tokens for users.

-- Jay Blunt

## Copyright

© 2022 Jay Blunt. All rights reserved.

All EVE related materials © 2014 CCP hf. All rights reserved.

"EVE", "EVE Online", "CCP", and all related logos and images are trademarks or registered trademarks of CCP hf.
