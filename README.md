# esi-sso-quart
SSO for ESI with Quart

[Eve Online](https://www.eveonline.com/) ESI login example using [Quart](https://quart.palletsprojects.com/en/latest/) (asyncio implementation of the Flask api).

Requires a valid [application](https://developers.eveonline.com/applications) setup, and a reverse proxy implementation with `X-Forwarded-For` / `X-Forwarded-Proto` headers included.

This example uses [Quart-Session](https://pypi.org/project/Quart-Session/) which requires a working backend. I used [Redis](https://redis.io) with a default configuration (listening on localhost).

```shell
python3.10 -m venv python3.10-env
. python3.10-env/bin/activate
pip install -U pip wheel setuptools
pip install -r requirements.txt
```


