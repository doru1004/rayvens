#
# Copyright IBM Corporation 2021
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import ray
import rayvens
import sys

# Send message to Slack sink using a stream which batches events operator.

# Command line arguments and validation:
if len(sys.argv) < 4:
    print(f'usage: {sys.argv[0]} <slack_channel> <slack_webhook> <run_mode>')
    sys.exit(1)
slack_channel = sys.argv[1]
slack_webhook = sys.argv[2]
run_mode = sys.argv[3]
if run_mode not in ['local', 'mixed', 'operator']:
    raise RuntimeError(f'Invalid run mode provided: {run_mode}')

# Initialize ray either on the cluster or locally otherwise.
if run_mode == 'operator':
    ray.init(address='auto')
else:
    ray.init()

# Start rayvens in operator mode.
rayvens.init(mode=run_mode)

# Create stream.
stream = rayvens.Stream('slack', batch_size=2)


# Operator task:
@ray.remote
def batching_operator(incoming_events):
    print(incoming_events)
    return " ".join(incoming_events)


# Event sink config.
sink_config = dict(kind='slack-sink',
                   route='/toslack',
                   channel=slack_channel,
                   webhook_url=slack_webhook)

# Add sink to stream.
sink = stream.add_sink(sink_config)

# Add multi-task operator to stream.
stream.add_operator(batching_operator)

# Sends messages to all sinks attached to this stream.
stream << "Hello"
stream << "World"
stream << "Hello"
stream << "Mars"
stream << "Hello"
stream << "Jupiter"

# Disconnect any sources or sinks attached to the stream 2 seconds after
# the stream is idle (i.e. no events were propagated by the stream).
stream.disconnect_all(after_idle_for=2)
