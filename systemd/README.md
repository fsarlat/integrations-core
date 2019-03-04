# Agent Check: Systemd

## Overview

This check monitors [Systemd][1] through the Datadog Agent.

## Setup

### Installation

The Systemd check is included in the [Datadog Agent][2] package. No additional installation is needed on your server.

### Configuration

1. Edit the `systemd.d/conf.yaml` file, in the `conf.d/` folder at the root of your
   Agent's configuration directory to start collecting your systemd performance data.
   See the [sample systemd.d/conf.yaml][2] for all available configuration options.

2. [Restart the Agent][3].

### Validation

[Run the Agent's status subcommand][4] and look for `systemd` under the Checks section.

## Data Collected

### Metrics

Systemd does not include any metrics.

### Service Checks

Systemd does not include any service checks.

### Events

Systemd does not include any events.

## Troubleshooting

Need help? Contact [Datadog support][5].

[1]: **LINK_TO_INTEGERATION_SITE**
[2]: https://github.com/DataDog/integrations-core/blob/master/systemd/datadog_checks/systemd/data/conf.yaml.example
[3]: https://docs.datadoghq.com/agent/faq/agent-commands/#start-stop-restart-the-agent
[4]: https://docs.datadoghq.com/agent/faq/agent-commands/#agent-status-and-information
[5]: https://docs.datadoghq.com/help/