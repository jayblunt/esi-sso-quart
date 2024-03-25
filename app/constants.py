import typing


class AppConstants:

    ESI_USER_AGENT: typing.Final = ""
    ESI_API_ROOT: typing.Final = "https://esi.evetech.net/"
    ESI_API_VERSION: typing.Final = "latest"

    ESI_LIMIT_PER_HOST: typing.Final = 3
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

    MARKET_REGION: typing.Final = 10000002
    MARKET_REFRESH_INTERVAL_SECONDS: typing.Final = 6 * 3600

    CORPORATION_REFRESH_INTERVAL_SECONDS: typing.Final = 480

    COMPRESSED_TYPE_DICT: typing.Final = {
        45490: 62463, 45491: 62460, 45492: 62454, 5493: 62457, 45494: 62474, 45495: 62471, 45496: 62477, 45497: 62468, 45498: 62483, 45501: 62480, 45504: 62498, 45510: 62510, 45511: 62507, 45513: 62513, 46282: 62461, 46280: 62464,
        46308: 62499, 45512: 62504, 46281: 62467, 46283: 62466, 46284: 62455, 46318: 62514, 46288: 62475, 46300: 62490, 46292: 62478, 46293: 62479, 46316: 62505, 46296: 62484, 46297: 62485, 46298: 62487, 45499: 62486, 45500: 62489,
        46301: 62491, 46302: 62481, 46303: 62482
    }
