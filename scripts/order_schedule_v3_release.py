"""Default-off V3 adjustment consumer gate. No data or deployment mutations."""
import os
import re

ENABLED_ENV = 'ORDER_ADJUSTMENT_SCHEDULE_V3_CONSUMERS_ENABLED'
ROUTES_ENV = 'ORDER_ADJUSTMENT_SCHEDULE_V3_CONSUMER_ROUTES'


def v3_consumers_allowed(route, environment=None):
    env = os.environ if environment is None else environment
    if env.get(ENABLED_ENV) != 'true' or type(route) is not str:
        return False
    raw = env.get(ROUTES_ENV, '')
    if type(raw) is not str or not raw or len(raw) > 1100:
        return False
    routes = raw.split(',')
    # Any malformed entry fails the whole configuration, never a partial allow.
    if len(routes) > 100 or any(not re.fullmatch(r'[0-9]{1,10}', item) for item in routes):
        return False
    return route in routes
