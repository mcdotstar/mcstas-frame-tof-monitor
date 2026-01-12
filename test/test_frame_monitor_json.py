import time
from contextlib import ContextDecorator, contextmanager
from pathlib import Path
from textwrap import dedent
from pytest import mark
from mccode_antlr import Flavor
from scipp import Variable


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


def get_registries():
    from mccode_antlr.reader import GitHubRegistry

    registries = [
        'mccode-mcpl-filter',
    ]
    registries = [GitHubRegistry(
        name,
        url=f'https://github.com/mcdotstar/{name}',
        filename='pooch-registry.txt',
        version='main'
    ) for name in registries]

    return registries + [this_registry()]


def primary(command: str):
    from mccode_antlr.assembler import Assembler
    name = 'test_frame_montitor_json'
    a = Assembler(name, flavor=Flavor.MCSTAS, registries=get_registries())
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


def test_frame_monitor_echos():
    from mccode_to_kafka.datfile import DatFileCommon
    instr = primary(command='"echo json = "')
    cmds = '-n 1000 dummy=0'
    times, output = time_compile_and_run(instr, '-n 1000 dummy=0', run=True)
    stdout, stderr = output
    for line in stdout.decode().splitlines():
        if line.startswith('json = '):
            dat = DatFileCommon.from_json(line[7:])
            assert sum(dat["N"]) == 1000


@contextmanager
def make_kafka_server(topic, source):
    """Manage running a Kafka server with a single topic expecting only messages from one source"""
    resource = acquire_temporary_kafka_server(topic, source)
    try:
        yield resource
    finally:
        release_temporary_kafka_server(resource)


def get_data(broker, topic, source):
    pass


def do_not_test_frame_monitor_integrates():
    from mccode_to_kafka.datfile import DatFileCommon
    # setup a temporary Kafka server ?!
    topic, source = 'temp_monitors', 'monitor'
    with make_kakfa_server(topic, source) as broker:
        instr = primary(command=f'"mccode-to-kafka json --broker {broker} --topic {topic} --source {source}"')
        times, output = time_compile_and_run(instr, '-n 1000 dummy=0', run=True)
        dat = get_data(broker, topic, source)
        assert sum(dat["N"]) == 1000





