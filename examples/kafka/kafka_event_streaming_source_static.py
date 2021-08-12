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

# Event streaming from a third-party external source using Kafka and
# static subscribers.

# Command line arguments and validation:
if len(sys.argv) < 2:
    print(f'usage: {sys.argv[0]} <brokers> <password> <run_mode> OR'
          f'       {sys.argv[0]} <run_mode>')
    sys.exit(1)

# Brokers and run mode:
brokers = None
password = None
run_mode = sys.argv[1]
if len(sys.argv) == 4:
    brokers = sys.argv[1]
    password = sys.argv[2]
    run_mode = sys.argv[3]

if run_mode not in ['local', 'mixed', 'operator']:
    raise RuntimeError(f'Invalid run mode provided: {run_mode}')

# The Kafka topic used for communication.
topic = "externalTopicSourceStatic"

# Initialize ray either on the cluster or locally otherwise.
if run_mode == 'operator':
    ray.init(address='auto')
else:
    ray.init()

# Start rayvens in operator mode."
rayvens.init(mode=run_mode, transport="kafka")

stream = rayvens.Stream('http-to-kafka')

stream >> (lambda event: print('LOG:', event))


def test_function(event):
    print('FUNCTION:', event)


stream >> test_function


@ray.remote
def test_task(event):
    print('RAY TASK:', event)


stream >> test_task


# Ray actor to handle events
@ray.remote
class TestActor:
    def __init__(self, print_prefix):
        self.print_prefix = print_prefix

    def append(self, event):
        print(self.print_prefix, event)


test_actor = TestActor.remote("RAY ACTOR:")
stream >> test_actor

# Event source config.
source_config = dict(
    kind='http-source',
    url='https://query1.finance.yahoo.com/v7/finance/quote?symbols=AAPL',
    route='/from-http',
    period=3000,
    kafka_transport_topic=topic,
    kafka_transport_partitions=3,
    kafka_transport_static_subscribers=True)

# Attach source to stream.
source = stream.add_source(source_config)


# Ray actor to handle events
@ray.remote
class AnotherTestActor:
    def __init__(self, print_prefix):
        self.print_prefix = print_prefix

    def append(self, event):
        print(self.print_prefix, event)


# This actor will not be added to the list of subscribers because the
# list of subscribers for this stream is static i.e. no other subscribers
# can be added once the Stream is created.
another_test_actor = AnotherTestActor.remote("ANOTHER RAY ACTOR:")
stream >> another_test_actor

# Only events with the following tags will be visible in the output:
# - LOG
# - FUNCTION
# - RAY TASK
# - RAY ACTOR

# Disconnect source after 10 seconds.
stream.disconnect_all(after=10)
