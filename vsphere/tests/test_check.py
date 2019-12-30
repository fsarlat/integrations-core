import json
import os

from mock import MagicMock, patch
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
            tags = metric['tags']
            tags.append('vcenter_server:FAKE')
            aggregator.assert_metric(metric['name'], metric['value'], tags=metric['tags'])

    aggregator.assert_all_metrics_covered()


def test_external_host_tags(aggregator, realtime_instance, mock_type):
    check = VSphereCheck('vsphere', {}, [realtime_instance])
    check.api = MockedAPI(realtime_instance)
    with check.infrastructure_cache.update():
        check.refresh_infrastructure_cache()

    fixture_file = os.path.join(HERE, 'fixtures', 'host_tags_values.json')
    with open(fixture_file, 'r') as f:
        expected_tags = json.load(f)

    check.set_external_tags = MagicMock()
    check.submit_external_host_tags()
    submitted_tags = check.set_external_tags.mock_calls[0].args[0]
    submitted_tags.sort(key=lambda x: x[0])
    for ex, sub in zip(expected_tags, submitted_tags):
        ex_host, sub_host = ex[0], sub[0]
        ex_tags, sub_tags = ex[1]['vsphere'], sub[1]['vsphere']
        assert ex_host == sub_host
        assert ex_tags == sub_tags

    check.excluded_host_tags = ['vsphere_host']
    check.set_external_tags = MagicMock()
    check.submit_external_host_tags()
    submitted_tags = check.set_external_tags.mock_calls[0].args[0]
    submitted_tags.sort(key=lambda x: x[0])
    for ex, sub in zip(expected_tags, submitted_tags):
        ex_host, sub_host = ex[0], sub[0]
        ex_tags, sub_tags = ex[1]['vsphere'], sub[1]['vsphere']
        ex_tags = [t for t in ex_tags if 'vsphere_host:' not in t]
        assert ex_host == sub_host
        assert ex_tags == sub_tags

    check.set_external_tags = MagicMock()
    check.submit_external_host_tags()
