"""
Contains common logic for accessing config from a monolithic config object (assumed to be CONFIG)
"""
import contextlib
import threading
import urlparse

import dateutil.parser
from sd_consul import consul
from sd_consul.utils import get_host_ip

from sd_intel import exceptions
from sd_intel.config import common


_consul_lock = threading.RLock()

_per_service_consul = {}


@contextlib.contextmanager
def _safe_consul(service_name, location=None):
    # Yay, the consul library API changed without consultation -- let's just hack around it! Awesome!
    base_key = u'{}/locations/{}'.format(service_name, location) if location else service_name

    with _consul_lock:
        consul_instance = _per_service_consul.get(base_key)
        if consul_instance is None:
            consul.Consul.clear(consul.Consul)  # Consul is a singleton -- make sure we get a new instance
            consul_instance = consul.Consul(base_key=base_key, consul_host=get_host_ip())
            _per_service_consul[base_key] = consul_instance
            consul.Consul.clear(consul.Consul)
        yield consul_instance


_scheme_type_lookup = {
    'postgresql': 'postgres'
}


def _to_database_config_dict(connection_string):
    parsed = urlparse.urlparse(connection_string)

    if not parsed.hostname:
        raise exceptions.ConfigError('No hostname in database connection string.')

    if not parsed.username:
        raise exceptions.ConfigError('No user in database connection string.')

    parsed_path = parsed.path

    if parsed_path:
        parsed_path = parsed_path.strip('/')

    if not parsed_path:
        raise exceptions.ConfigError('No database name in database connection string.')

    scheme = parsed.scheme.lower()

    return {
        "dbname": parsed_path,
        "dbpassword": parsed.password,
        "dbserver": parsed.hostname,
        "dbuser": parsed.username,
        "type": _scheme_type_lookup.get(scheme, scheme),
    }


def _get_database_config(client_id, service_name):
    with _safe_consul('intel') as config:
        keys = config.key.get_service_config('service_database_keys', default={})
        key = keys.get(service_name)
        if key is None:
            return config.key.get_service_config('databases')[service_name]

        locations = config.key.get_service_config('service_client_locations', default={})

    client_locations = locations.get(service_name)
    if client_locations is not None:
        if client_id is None:
            raise exceptions.ConfigError(u'No client ID supplied for {} database lookup'.format(service_name))
        location = client_locations.get(client_id)
    else:
        location = None

    with _safe_consul(service_name, location=location) as config:
        return _to_database_config_dict(config.key.get_service_config(key))

def get_config():
    """Needed to fulfill the config submodule interface -- not used by this module."""
    return {}


def get_reporting_enabled_clients():
    with _safe_consul('intel') as config:
        return config.key.get_service_config('enabled_client_names')


def get_store_config(client_id):
    """Return storefront database config namedtuple."""
    return common.get_type_based_config(_get_database_config(client_id, 'storefront'))


def get_backstage_config(client_id):
    """Return backstage database config namedtuple."""
    return common.get_type_based_config(_get_database_config(client_id, 'backstage'))


def get_locker_config(client_id):
    """Return locker database config namedtuple."""
    return common.get_type_based_config(_get_database_config(client_id, 'locker'))


def get_pulse_config(client_id):
    """Return cassandra database config namedtuple."""
    return common.get_type_based_config(_get_database_config(client_id, 'pulse'))


def get_dw_config(client_id):
    """Return data warehouse database config namedtuple."""
    return common.get_type_based_config(_get_database_config(client_id, 'dw'))


def get_checkout_config(client_id):
    """Return data warehouse database config namedtuple."""
    return common.get_type_based_config(_get_database_config(client_id, 'checkout'))


def get_ultraviolet_config(client_id):
    """Return data warehouse database config namedtuple."""
    return common.get_type_based_config(_get_database_config(client_id, 'ultraviolet'))


def get_campaign_config(client_id):
    """Return data warehouse database config namedtuple."""
    return common.get_type_based_config(_get_database_config(client_id, 'campaign'))


def get_subscription_config(client_id):
    """Return data warehouse database config namedtuple."""
    return common.get_type_based_config(_get_database_config(client_id, 'subscription'))


def get_identity_config(client_id):
    """Return data warehouse database config namedtuple."""
    return common.get_type_based_config(_get_database_config(client_id, 'identity'))


def get_maxmind_config(client_id):
    config = _get_database_config(client_id, 'maxmind')
    return common.GeoIPConfig(
        config['db_path'], config['bucket'], config['key'], config['access_key'], config['secret_key'])


def get_notification_config():
    """Return notification config dictionary"""
    with _safe_consul('intel') as config:
        return config.key.get_service_config('notifications', default={})


def get_email_recipients():
    """Return list of email recipients for notifications."""
    with _safe_consul('intel') as config:
        return config.key.get_service_config('email_recipients')


def get_sftp_private_key():
    """Return string SFTP private key."""
    with _safe_consul('intel') as config:
        return config.key.get_service_config('sftp_private_key')


def get_sftp_location():
    """Return string SFTP location."""
    with _safe_consul('intel') as config:
        return config.key.get_service_config('sftp_location')


def get_etl_overlap_seconds():
    """Return integer ETL overlap in seconds."""
    with _safe_consul('intel') as config:
        return config.key.get_service_config('etl_overlap_seconds')


def get_crontab():
    with _safe_consul('intel') as config:
        return config.key.get_service_config('crontab')


def get_sftp():
    """Return SFTP configuration."""
    with _safe_consul('intel') as config:
        return config.key.get_service_config('sftp')


def get_client_sftp(client_name):
    """Return SFTP configuration."""
    return get_sftp()['client'].get(client_name)


def get_email():
    """Return SFTP configuration."""
    with _safe_consul('intel') as config:
        return config.key.get_service_config('email')


def get_client_email(client_name):
    """Return email configuration."""
    return get_email()['client'].get(client_name)


def get_minimum_data_datetime(client_id, etl_name, default=None):
    with _safe_consul('intel') as config:
        lookup = config.key.get_service_config('minimum_data_datetime', default={})

    value = lookup.get(client_id, {}).get(etl_name, default)

    if value != default:
        try:
            value = dateutil.parser.parse(value)
        except Exception as exc:
            raise exceptions.ConfigError(
                u'Error converting {} {} minimum_data_datetime value "{}": {}'.format(client_id, etl_name, value, exc))

    return value