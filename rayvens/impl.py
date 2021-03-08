import os
import ray
from ray import serve
import signal
import subprocess
import yaml


@ray.remote(num_cpus=0)
class Camel:
    @staticmethod
    def start():
        if os.getenv('KUBE_POD_NAMESPACE') is not None:
            return Camel.options(resources={'head': 1}).remote()
        else:
            return Camel.remote()

    def __init__(self):
        self.client = serve.start(http_options={
            'host': '0.0.0.0',
            'location': 'EveryNode'
        })
        self.integrations = []

    def add_source(self, name, topic, url, period=3000, prefix='/ravyens'):
        async def publish(data):
            topic.publish.remote(await data.body())

        self.client.create_backend(name,
                                   publish,
                                   config={'num_replicas': 1},
                                   ray_actor_options={'num_cpus': 0})
        self.client.create_endpoint(name,
                                    backend=name,
                                    route=f'{prefix}/{name}',
                                    methods=['POST'])
        endpoint = 'http://localhost:8000'
        namespace = os.getenv('KUBE_POD_NAMESPACE')
        if namespace is not None:
            with open('/etc/podinfo/labels', 'r') as f:
                for line in f:
                    k, v = line.partition('=')[::2]
                    if k == 'component':
                        endpoint = (f'http://{v[1:-2]}.{namespace}'
                                    '.svc.cluster.local:8000')
                        break
        integration = Integration(name, [{
            'from': {
                'uri': f'timer:tick?period={period}',
                'steps': [{
                    'to': url
                }, {
                    'to': f'{endpoint}{prefix}/{name}'
                }]
            }
        }])
        topic._register.remote(name, integration)
        if self.integrations is None:
            integration.cancel()
        else:
            self.integrations.append(integration)

    def add_sink(self, name, topic, to):
        integration = Integration(name, [{
            'from': {
                'uri': f'platform-http:/{name}',
                'steps': [{
                    'to': to
                }]
            }
        }])

        url = f'{integration.url}/{name}'
        topic.subscribe.remote(lambda data: topic._post.remote(url, data),
                               name)
        topic._register.remote(name, integration)
        if self.integrations is None:
            integration.cancel()
        else:
            self.integrations.append(integration)

    def cancel(self, integrations):
        for i in integrations:
            i['integration'].cancel()

    def exit(self):
        integrations = self.integrations
        self.integrations = None
        for i in integrations:
            i.cancel()


class Integration:
    def __init__(self, name, integration):
        self.name = name
        self.url = 'http://localhost:8080'
        filename = f'{name}.yaml'
        with open(filename, 'w') as f:
            yaml.dump(integration, f)
        command = ['kamel', 'local', 'run', filename]
        namespace = os.getenv('KUBE_POD_NAMESPACE')
        if namespace is not None:
            self.url = f'http://{self.name}.{namespace}.svc.cluster.local:80'
            command = [
                '/home/ray/rayvens/rayvens/linux-x86_64/kamel', 'run', '--dev',
                filename
            ]
        process = subprocess.Popen(command, start_new_session=True)
        self.pid = process.pid

    def cancel(self):
        try:
            os.killpg(os.getpgid(self.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
