import typing


class AppConstants:

    ESI_USER_AGENT: typing.Final = ""
    ESI_API_ROOT: typing.Final = "https://esi.evetech.net/"
    ESI_API_VERSION: typing.Final = "latest"

    ESI_LIMIT_PER_HOST: typing.Final = 13
    ESI_ERROR_SLEEP_TIME: typing.Final = 7
    ESI_ERROR_RETRY_COUNT: typing.Final = 11

    # ESI throws off a 504 at daily restart, so let's double the retry
    # waiting period for those.
    ESI_ERROR_SLEEP_MODIFIERS: typing.Final = {
        500: 19,
        504: 37,
    }

    MAGIC_ADMINS: typing.Final = {}
    MAGIC_CONTRIBUTORS: typing.Final = {}
    MAGIC_SUSPECTS: typing.Final = {}

    CORPORATION_REFRESH_INTERVAL_SECONDS: typing.Final = 540
