import sys
import time
from contextlib import ContextDecorator, contextmanager
from pathlib import Path
from textwrap import dedent
from pytest import mark, skip
from mccode_antlr import Flavor
from scipp import Variable



class MockKafkaServer:
    """Lightweight Kafka broker using Redpanda in Docker via testcontainers
    
    This class automatically works with both Docker and Podman.
    
    For Podman users: 
    1. Enable the Podman socket:
        systemctl --user enable --now podman.socket
    
    2. Set DOCKER_HOST environment variable before running tests:
        export DOCKER_HOST=unix:///run/user/$UID/podman/podman.sock
    
    The Ryuk reaper container is automatically disabled for Podman compatibility.
    """
    def __init__(self, topic):
        self.topic = topic
        self.container = None
        
    def start(self):
        """Start a Redpanda container (Kafka-compatible broker)"""
        # Ensure Podman socket is accessible if using Podman
        import os
        if not os.environ.get('DOCKER_HOST'):
            # Try to set DOCKER_HOST for Podman if socket exists
            podman_sock = f"unix:///run/user/{os.getuid()}/podman/podman.sock"
            if os.path.exists(f"/run/user/{os.getuid()}/podman/podman.sock"):
                os.environ['DOCKER_HOST'] = podman_sock
        
        # Disable Ryuk (reaper container) for Podman compatibility
        # Ryuk is used for cleanup but can cause connection issues with Podman
        os.environ['TESTCONTAINERS_RYUK_DISABLED'] = 'true'
        
        try:
            from testcontainers.kafka import RedpandaContainer
        except ImportError:
            raise ImportError(
                "testcontainers is required for Kafka integration tests. "
                "Install with: pip install testcontainers"
            )
        
        
        # Start Redpanda container (lightweight Kafka-compatible broker)
        self.container = RedpandaContainer()
        self.container.start()
        
        # Get the bootstrap server address
        broker = self.container.get_bootstrap_server()
        return broker
    
    def stop(self):
        """Stop the Redpanda container"""
        if self.container:
            self.container.stop()
    
    def get_messages(self):
        """Consume and return all messages from the topic"""
        if not self.container:
            return []
        
        try:
            from confluent_kafka import Consumer, KafkaError
            
            # Create a consumer to read messages from the topic
            consumer = Consumer({
                'bootstrap.servers': self.container.get_bootstrap_server(),
                'group.id': 'test_consumer',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            })
            
            # Subscribe to the topic
            consumer.subscribe([self.topic])
            
            # Collect all messages (timeout after 5 seconds of no messages)
            messages = []
            timeout = 5.0
            
            while True:
                msg = consumer.poll(timeout)
                if msg is None:
                    break  # No more messages
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break  # End of partition
                    else:
                        print(f"Consumer error: {msg.error()}")
                        continue
                
                # Message received successfully
                messages.append(msg.value())
            
            consumer.close()
            return messages
            
        except ImportError:
            print("Warning: confluent_kafka not available for consuming messages")
            return []
        except Exception as e:
            print(f"Error consuming messages: {e}")
            return []


@contextmanager
def make_kafka_server(topic, source):
    """Manage running a mock Kafka server with a single topic expecting only messages from one source"""
    mock = MockKafkaServer(topic)
    broker = mock.start()
    try:
        yield broker, mock
    finally:
        mock.stop()


def get_data(mock):
    """Get data from the Kafka broker and deserialize FlatBuffer messages"""
    from streaming_data_types.dataarray_da00 import deserialise_da00
    
    messages = mock.get_messages()
    
    if not messages:
        return None
    
    # Try to deserialize each message as a FlatBuffer
    for idx, msg_data in enumerate(messages):
        if not isinstance(msg_data, bytes):
            continue
        
        try:
            # Try to deserialize directly
            result = deserialise_da00(msg_data)
            if result is not None:
                return result
        except Exception as e:
            continue
    
    return None
    


class Timer(ContextDecorator):
    def __init__(self, name: str = 'block'):
        self.name = name

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.end = time.perf_counter()
        self.interval = self.end - self.start
        print(f'Timer [{self.name}]: {self.interval:.3f} s')


def compiled(method):
    from mccode_antlr.compiler.check import simple_instr_compiles
    if simple_instr_compiles('cc'):
        return method
    @mark.skip(reason=f"Working C compiler required for {method}")
    def skipped_method(*args, **kwargs):
        return method(*args, **kwargs)
    return skipped_method


def containered(method):
    import sys
    if sys.platform.startswith("linux"):
        return method
    @mark.skip(reason="No CI Docker/Podman/etc. except for Linux")
    def skipped(*args, **kwargs):
        return method(*args, **kwargs)
    return skipped


def time_compile_and_run(instr,
                    parameters,
                    directory: Path | None = None,
                    run=True,
                    dump_source=True,
                    target: dict | None = None,
                    config: dict | None = None,
                    flavor: Flavor = Flavor.MCSTAS):
    from pathlib import Path
    from tempfile import TemporaryDirectory
    from datetime import datetime
    from mccode_antlr.run import mccode_compile, mccode_run_compiled

    kwargs = dict(target=target, config=config, dump_source=dump_source)

    times = {}

    def actual_compile_run(inside):
        with Timer('compile') as t:
            binary, target = mccode_compile(instr, inside, flavor=flavor, **kwargs)
        times['compile_time'] = t.interval
        # The runtime output directory used *can not* exist for McStas/McXtrace to work properly.
        # So find a name inside this directory that doesn't exist (any name should work)
        output = inside / datetime.now().isoformat()
        with Timer('run') as t:
            result = mccode_run_compiled(binary, target, output, parameters) if run else (None, None)
        times['run_time'] = t.interval
        return times, result

    if directory is None or not isinstance(directory, Path) or not directory.is_dir():
        with TemporaryDirectory() as tmpdir:
            return actual_compile_run(Path(tmpdir))

    return actual_compile_run(directory)


def this_registry():
    from git import Repo, InvalidGitRepositoryError
    from mccode_antlr.reader.registry import LocalRegistry
    try:
        repo = Repo('.', search_parent_directories=True)
        root = repo.working_tree_dir
        return LocalRegistry('this_registry', root)
    except InvalidGitRepositoryError as ex:
        raise RuntimeError(f"Unable to identify base repository, {ex}")


def primary(command: str):
    from mccode_antlr.assembler import Assembler
    name = 'test_frame_monitor_json'
    a = Assembler(name, flavor=Flavor.MCSTAS, registries=[this_registry()])
    a.parameter('int dummy/"s"=0')

    origin = a.component("origin", "Progress_bar")
    source = a.component("source", "Source_simple", parameters={
        "yheight": 0.1, "xwidth": 0.1, "dist": 2.,
        "focus_xw": 0.01, "focus_yh": 0.01, "lambda0": 1.5, "dlambda": 0.1
        }, at=((0,0,0), origin), rotate=[(0,45,0), origin])
    frame = a.component("monitor", "Frame_monitor_json", parameters={
        "xwidth": 0.02, "yheight": 0.02, "nt": 10, "frame": 1000.,
        #"command": '"mccode-to-kafka json --topic dummy --source monitor"'
        "command": command
        }, at=[[0, 0, 2.], source])

    return a.instrument


@compiled
def test_frame_monitor_echos():
    from mccode_to_kafka.datfile import DatFileCommon
    instr = primary(command='"echo json = "')
    times, output = time_compile_and_run(instr, '-n 1000 dummy=0', run=True)
    stdout, stderr = output
    for line in stdout.decode().splitlines():
        if line.startswith('json = '):
            dat = DatFileCommon.from_json(line[7:])
            assert sum(dat["N"]) == 1000


@containered
@compiled
def test_frame_monitor_integrates():
    from mccode_to_kafka.datfile import DatFileCommon
    # setup a temporary mock Kafka server
    topic, source = 'temp_monitors', 'monitor'
    with make_kafka_server(topic, source) as (broker, mock):
        instr = primary(command=f'"mccode-to-kafka json --broker {broker} --topic {topic} --source {source}"')
        times, output = time_compile_and_run(instr, '-n 1000 dummy=0', run=True)
        da00 = get_data(mock)
        assert da00 is not None
        
        data = {v.name: v for v in da00.data}
        assert all(x in data for x in ('t', 'signal', 'signal_errors'))
        assert len(data['signal'].data) == 10
        assert len(data['t'].data) == 11





