import os

import pytest
from mock import Mock, patch

HERE = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture()
def realtime_instance():
    return {
        'collection_level': 4,
        'empty_default_hostname': True,
        'use_legacy_check_version': False,
        'host': os.environ.get('VSPHERE_URL', 'FAKE'),
        'username': os.environ.get('VSPHERE_USERNAME', 'FAKE'),
        'password': os.environ.get('VSPHERE_PASSWORD', 'FAKE'),
        'ssl_verify': False,
    }


@pytest.fixture()
def historical_instance():
    return {
        'collection_level': 1,
        'empty_default_hostname': True,
        'use_legacy_check_version': False,
        'host': os.environ.get('VSPHERE_URL', 'FAKE'),
        'username': os.environ.get('VSPHERE_USERNAME', 'FAKE'),
        'password': os.environ.get('VSPHERE_PASSWORD', 'FAKE'),
        'ssl_verify': False,
        'collection_type': 'historical',
    }


@pytest.fixture
def mock_type():
    with patch('datadog_checks.vsphere.cache.type') as cache_type, patch(
        'datadog_checks.vsphere.utils.type'
    ) as utils_type, patch('datadog_checks.vsphere.vsphere.type') as vsphere_type:
        new_type_function = lambda x: x.__class__ if isinstance(x, Mock) else type(x)  # noqa: E731
        cache_type.side_effect = new_type_function
        utils_type.side_effect = new_type_function
        vsphere_type.side_effect = new_type_function
        yield
