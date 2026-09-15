"""Release availability only. Never substitutes for route authorization.

Each feature retains its independent default-off kill switch. An explicit
all_authorized mode removes pilot membership, not actor/owner/entitlement checks.
Ambiguous or malformed configuration fails closed.
"""
import os
import re

MODE_ENV = 'ORDER_SEND_ROLLOUT_MODE'


def rollout_mode():
    value = os.environ.get(MODE_ENV, 'pilot')
    return value if value in ('pilot', 'all_authorized') else None


def all_authorized_enabled(enabled_env, routes_env):
    return (rollout_mode() == 'all_authorized' and os.environ.get(enabled_env) == 'true'
            and not os.environ.get(routes_env, ''))


def pilot_routes(enabled_env, routes_env, limit):
    if rollout_mode() != 'pilot' or os.environ.get(enabled_env) != 'true':
        return ()
    values = os.environ.get(routes_env, '').split(',')
    if (not 1 <= len(values) <= limit or len(set(values)) != len(values)
            or any(not re.fullmatch(r'[0-9]{1,10}', value) for value in values)):
        return ()
    return tuple(values)


def route_enabled(route, enabled_env, routes_env, limit=100):
    if type(route) is not str or not re.fullmatch(r'[0-9]{1,10}', route):
        return False
    return (all_authorized_enabled(enabled_env, routes_env)
            or route in pilot_routes(enabled_env, routes_env, limit))
