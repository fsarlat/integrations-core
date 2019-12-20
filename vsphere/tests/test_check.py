import json
import os

from mock import patch
from tests.conftest import HERE
from tests.mocked_api import MockedAPI

from datadog_checks.vsphere import VSphereCheck


def test_realtime_metrics(aggregator, realtime_instance, mock_type):
    """This test asserts that the same api content always produces the same metrics."""
    with patch('datadog_checks.vsphere.vsphere.VSphereAPI', MockedAPI):
        check = VSphereCheck('vsphere', {}, [realtime_instance])
        check._no_thread_mode = True
        check.check(realtime_instance)

    fixture_file = os.path.join(HERE, 'fixtures', 'metrics_realtime_values.json')
    with open(fixture_file, 'r') as f:
        data = json.load(f)
        for metric in data:
            aggregator.assert_metric(metric['name'], metric['value'], hostname=metric['hostname'])

    aggregator.assert_all_metrics_covered()


def test_historical_metrics(aggregator, historical_instance, mock_type):
    """This test asserts that the same api content always produces the same metrics."""
    with patch('datadog_checks.vsphere.vsphere.VSphereAPI', MockedAPI):
        check = VSphereCheck('vsphere', {}, [historical_instance])
        check._no_thread_mode = True
        check.check(historical_instance)

    fixture_file = os.path.join(HERE, 'fixtures', 'metrics_historical_values.json')
    with open(fixture_file, 'r') as f:
        data = json.load(f)
        for metric in data:
            aggregator.assert_metric(metric['name'], metric['value'], tags=metric['tags'])

    aggregator.assert_all_metrics_covered()
