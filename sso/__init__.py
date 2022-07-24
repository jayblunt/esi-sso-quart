import asyncio
import base64
import inspect
import urllib.parse
import uuid
from typing import Dict, Final, List, Optional

import aiohttp
import aiohttp.client_exceptions
import jose.exceptions
import jose.jwt
import quart


class EveSSO:

    CONNFIGURATION_URL: Final = 'https://login.eveonline.com/.well-known/oauth-authorization-server'

    JWT_ISSUERS: Final = ["login.eveonline.com", "https://login.eveonline.com"]
    JWT_AUDIENCE: Final = "EVE Online"

    ESI_STATE: Final = "state"
    ESI_CHARACTER_NAME: Final = "character_name"
    ESI_CHARACTER_ID: Final = "character_id"
    ESI_CHARACTER_STATION_MANAGER_ROLE: Final = "station_manager_role"
    ESI_CORPORATEION_ID: Final = "corporation_id"
    ESI_ALLIANCE_ID: Final = "alliance_id"
    ESI_SECURITY_STATUS: Final = "security_status"
    ESI_ACCESS_TOKEN: Final = "access_token"
    ESI_TOKEN_SCOPES: Final = "scopes"
    ESI_BIRTHDAY: Final = "birthday"


    def __init__(self,
                app: quart.Quart,
                client_id: str,
                client_secret: str,
                configuration_url: str = None,
                scopes: List[str] = ['publicData'],
                login_route: str = '/sso/login',
                logout_route: str = '/sso/logout',
                callback_route: str = '/sso/callback') -> None:

        self.app: Final = app

        self.client_id: Final = client_id
        self.client_secret: Final = client_secret

        self.configuration_url: Final = configuration_url or self.CONNFIGURATION_URL
        self.scopes: Final = scopes

        self.login_route: Final = login_route
        self.logout_route: Final = logout_route
        self.callback_route: Final = callback_route

        self.configuration: Dict = None
        self.jwks_uri: str = None
        self.jwks: List[Dict] = None
        self.jwks_task: asyncio.Task = None

        @app.before_serving
        async def esi_sso_setup():
            self.configuration = await self.get_json(self.configuration_url)

            required_configuration_keys = ["token_endpoint", "authorization_endpoint", "issuer", "jwks_uri"]
            if not all(map(lambda x: bool(self.configuration.get(x)), required_configuration_keys)):
                raise Exception(f"{inspect.currentframe().f_code.co_name}: configuration at {self.configuration_url} is invalid")

            self.jwks_uri = self.configuration["jwks_uri"]
            self.jwks = await self.get_jwks(self.jwks_uri)
            self.jwks_task = asyncio.create_task(self.get_jwks_task())

            app.add_url_rule(self.callback_route, self.callback_endpoint, view_func=self.esi_sso_callback)
            app.add_url_rule(self.logout_route, self.logout_endpoint, view_func=self.esi_sso_logout)
            app.add_url_rule(self.login_route, self.login_endpoint, view_func=self.esi_sso_login)


        @app.after_serving
        async def esi_sso_teardown():
            if self.jwks_task:
                if not self.jwks_task.cancelled():
                    self.jwks_task.cancel()


    @property
    def base_url(self) -> str:
        headers = {k.lower(): v for k, v in dict(quart.request.headers).items()}
        parsed_url = urllib.parse.urlparse(quart.request.base_url)
        scheme = headers["x-forwarded-proto"] if "x-forwarded-proto" in headers else parsed_url[0]
        port = int(headers['x-forwarded-port']) if "x-forwarded-port" in headers else None
        port = f":{port}" if isinstance(port, int) else ""
        return f"{scheme}://{quart.request.host}{port}"


    @property
    def callback_url(self) -> str:
        return f"{self.base_url}{self.callback_route}"


    @property
    def login_endpoint(self) -> str:
        return f"login_{self.client_id}"


    @property
    def logout_endpoint(self) -> str:
        return f"logout_{self.client_id}"


    @property
    def callback_endpoint(self) -> str:
        return f"callback_{self.client_id}"


    async def get_json(self, url: str, token: Optional[str] = None) -> Dict:
        session_headers = dict()
        if token:
            session_headers["Authorization"] = f"Bearer {token}"

        json = None
        async with aiohttp.ClientSession(headers=session_headers) as client_session:
            async with client_session.get(url) as response:
                response.raise_for_status()
                if response.status in [200]:
                    json = dict(await response.json())
        return json


    async def get_jwks(self, url: str) -> List[Dict]:
        payload = await self.get_json(url)
        return payload.get("keys", [])


    async def get_jwks_task(self) -> None:
        while True:
            await asyncio.sleep(300)
            try:
                new_jwks = await self.get_json(self.jwks_uri)
                if new_jwks is not None:
                    self.jwks = new_jwks
            except Exception as ex:
                self.app.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")


    def esi_sso_login(self) -> quart.redirect:
        quart.session[self.ESI_STATE] = uuid.uuid4().hex

        redirect_params = [
            'response_type=code',
            f'redirect_uri={self.callback_url}',
            f'client_id={self.client_id}',
            f'scope={urllib.parse.quote(" ".join(self.scopes))}',
            f'state={quart.session[self.ESI_STATE]}'
        ]

        redirect_url = f"{self.configuration['authorization_endpoint']}?{'&'.join(redirect_params)}"

        return quart.redirect(redirect_url)


    async def esi_sso_logout(self) -> quart.redirect:
        quart.session.clear()

        return quart.redirect("/")


    async def esi_sso_callback(self) -> quart.redirect:

        required_callback_keys = ["code", "state"]
        if not all(map(lambda x: bool(quart.request.args.get(x)), required_callback_keys)):
            raise Exception(f"{inspect.currentframe().f_code.co_name}: invalid call to {self.callback_route}")

        if quart.request.args['state'] != quart.session[self.ESI_STATE]:
            raise Exception(f"{inspect.currentframe().f_code.co_name}: invalid session state in {self.callback_route}")

        post_character_url = f"{self.configuration['token_endpoint']}"
        basic_auth = f"{self.client_id}:{self.client_secret}"
        post_session_headers = {
            "Authorization": f"Basic {base64.urlsafe_b64encode(basic_auth.encode('utf-8')).decode()}",
            "Host": self.configuration["issuer"],
        }
        post_body = {
            "grant_type": "authorization_code",
            "code": quart.request.args['code'],
        }

        token_response = dict()
        async with aiohttp.ClientSession(headers=post_session_headers) as client_session:
            async with client_session.post(post_character_url, data=post_body) as response:
                if response.status in [200]:
                    token_response = dict(await response.json())

        required_response_keys = ["access_token", "token_type", "expires_in", "refresh_token"]
        if not all(map(lambda x: bool(token_response.get(x)), required_response_keys)):
            raise Exception(f"{inspect.currentframe().f_code.co_name}: invalid response to {post_character_url}")

        jwt_unverified_header: Final = jose.jwt.get_unverified_header(token_response["access_token"])
        jwt_key = None
        for jwk_candidate in self.jwks:
            jwt_key_match = True
            for header_key in set(jwt_unverified_header.keys()).intersection({"kid", "alg"}):
                if jwt_unverified_header.get(header_key) != jwk_candidate.get(header_key):
                    jwt_key_match = False
                    break
            if jwt_key_match:
                jwt_key = jwk_candidate
                break

        if jwt_key:
            decoded_jwt = None
            try:
                decoded_jwt = jose.jwt.decode(token_response["access_token"], key=jwt_key, issuer=self.JWT_ISSUERS, audience=self.JWT_AUDIENCE)
            except jose.exceptions.JWTError as ex:
                print(f"{inspect.currentframe().f_code.co_name}: {ex}")

            if decoded_jwt:
                # print(f"decoded_jwt: {decoded_jwt}")
                quart.session[self.ESI_CHARACTER_ID] = decoded_jwt.get('sub', '').split(':')[-1]
                quart.session[self.ESI_CHARACTER_NAME] = decoded_jwt.get('name', '')
                quart.session[self.ESI_ACCESS_TOKEN] = token_response["access_token"]
                quart.session[self.ESI_TOKEN_SCOPES] = decoded_jwt.get('scp', [])

        if bool(quart.session.get(self.ESI_CHARACTER_ID, False)):
            character_id: Final = quart.session[self.ESI_CHARACTER_ID]
            common_params: Final = {"datasource": "tranquility"}
            session_headers = {
                "Authorization": f"Bearer {quart.session.get(self.ESI_ACCESS_TOKEN)}"
            }

            character_response = None
            character_roles_response = None

            async with aiohttp.ClientSession(headers=session_headers) as client_session:

                post_character_url = f"https://esi.evetech.net/latest/characters/{character_id}/"
                async with client_session.get(post_character_url, params=common_params) as response:
                    if response.status in [200]:
                        character_response = dict(await response.json())

                if "esi-characters.read_corporation_roles.v1" in quart.session.get(self.ESI_TOKEN_SCOPES, []):
                    post_character_roles_url = f"https://esi.evetech.net/latest/characters/{character_id}/roles"
                    async with client_session.get(post_character_roles_url, params=common_params) as response:
                        if response.status in [200]:
                            character_roles_response = dict(await response.json())


            if character_response is not None:
                for k in [self.ESI_CORPORATEION_ID, self.ESI_ALLIANCE_ID, self.ESI_SECURITY_STATUS, self.ESI_BIRTHDAY]:
                    v = character_response.get(k, None)
                    if v is not None:
                        quart.session[k] = v

            if character_roles_response is not None:
                # print(quart.json.dumps(character_roles_response, ensure_ascii=True, indent=4))
                quart.session[self.ESI_CHARACTER_STATION_MANAGER_ROLE] = 'Station_Manager' in character_roles_response.get('roles', [])


        return quart.redirect("/")
