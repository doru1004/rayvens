import ray
import rayvens

# this example demonstrate how to use a stream to connect
# event producers and consumers using a pub-sub pattern
#
# in this example, the event handlers are Ray tasks
# they are invoked in arbitrary order
# (within and across the two subscribers)

try:
    ray.init(address='auto')  # try to connect to cluster first
except ConnectionError:
    ray.init()  # fallback to local execution

# start rayvens client
client = rayvens.Client()

# create a stream actor
stream = client.create_stream('example')


# define a first event handling task
@ray.remote
def handler1(event):
    print('handler1 received', event)


# define a second event handling task
@ray.remote
def handler2(event):
    print('handler2 received', event)


# subscribe tasks to stream
stream >> handler1
stream >> handler2

# publish a few events
for i in range(10):
    stream << f'event {i}'
