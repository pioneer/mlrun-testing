import os
import sys
from copy import deepcopy
import traceback
from mlrun import code_to_function, v3io_cred, set_environment
from storey.steps import Flatten
from mlrun.serving.server import MockEvent
import v3io.dataplane
from handler import *


def _mockevent_copy(
    self,
    body=None,
    key=None,
    time=None,
    id=None,
    headers=None,
    method=None,
    path=None,
    content_type=None,
    awaitable_result=None,
    deep_copy=False,
):
    if deep_copy and body is None and self.body is not None:
        body = deepcopy(self.body)

    return MockEvent(
        body=body or self.body,
        headers=headers or self.headers,
        method=method or self.method,
        path=path or self.path,
        content_type=content_type or self.content_type,
    )


MockEvent.copy = _mockevent_copy


def _remove_stream_container(path):
    if path.startswith("v3io://"):
        path = path[7:]
    return "/" + "/".join(path.split("/")[2:])


def _try_create_v3io_stream(stream_name):
    v3io_client = v3io.dataplane.Client(
        endpoint=os.environ["V3IO_API"], access_key=os.environ["V3IO_ACCESS_KEY"]
    )
    try:
        stream_path = _remove_stream_container(stream_name)
        print(f"Removing '{stream_path}' if exists...")
        try:
            v3io_client.delete_stream("projects", stream_path)
        except RuntimeError as e:
            print(f"This may be not an error: {e}")
        print(f"Creating '{stream_path}'...")
        v3io_client.create_stream("projects", stream_path, shard_count=1)
    except:
        print("v" * 15 + " THIS MAY BE NOT AN ERROR!!! " + "v" * 15)
        traceback.print_exc()
        print("^" * 15 + " THIS MAY BE NOT AN ERROR!!! " + "^" * 15)


PROJECT_NAME = "mlrun-testing-uv-st"
REQUIREMENTS = ["mlrun==0.7.1", "storey==0.7.8", "rand_string"]
BASE_IMAGE = "python:3.8"  # "mlrun/mlrun"
SEEK_TO = "EARLIEST"
SHARDS = 1  # 4


def main(deploy=False):
    project_name = os.getenv("PROJECT_NAME", PROJECT_NAME)
    print(f"PROJECT_NAME: {project_name}")
    data_generator_stream = f"v3io:///projects/{project_name}/data_generator/output" if deploy else ""
    data_enricher_stream = f"v3io:///projects/{project_name}/data_enricher/output" if deploy else ""
    data_formatter_stream = f"v3io:///projects/{project_name}/data_formatter/output" if deploy else ""
    error_stream = f"v3io:///projects/{project_name}/errors" if deploy else ""

    # if deploy:
    #     for stream_name in [
    #         data_generator_stream,
    #         data_enricher_stream,
    #         data_formatter_stream,
    #         # error_stream,
    #     ]:
    #         _try_create_v3io_stream(stream_name)

    root_function = code_to_function(
        "data-generator",
        project=project_name,
        filename="handler.py",
        kind="serving",
        image=os.getenv("BASE_IMAGE", BASE_IMAGE),
        requirements=REQUIREMENTS,
    )
    root_function.spec.error_stream = error_stream

    graph = root_function.set_topology("flow", engine="async", exist_ok=True)

    data_enricher_function = root_function.add_child_function(
        "data-enricher",
        url="handler.py",
        image=os.getenv("BASE_IMAGE", BASE_IMAGE),
        requirements=REQUIREMENTS,
    )
    data_enricher_function.spec.error_stream = error_stream

    data_formatter_function = root_function.add_child_function(
        "data-formatter",
        url="handler.py",
        image=os.getenv("BASE_IMAGE", BASE_IMAGE),
        requirements=REQUIREMENTS,
    )
    data_formatter_function.spec.error_stream = error_stream

    (
        graph.to("DataGenerator", name="data_generator")
        .to("storey.steps.Flatten")
        .to(
            "DataAdder",
            name="data_adder"
        )
        .to(
            ">>",
            name="data_generator_v3io",
            path=data_generator_stream,
            seek_to=SEEK_TO,
            shards=SHARDS
        )
        .to(
            "DataEnricher",
            name="data_enricher",
            function="data-enricher" if deploy else None
        )
        .error_handler("error_catcher")
        .to(
            ">>",
            name="data_enricher_v3io",
            path=data_enricher_stream,
            seek_to=SEEK_TO,
            shards=SHARDS
        )
        .to(
            "DataFormatter",
            name="data_formatter",
            function="data-formatter" if deploy else None,
        )
        .to(
            ">>",
            name="data_formatter_v3io",
            path=data_formatter_stream,
            seek_to=SEEK_TO,
            shards=SHARDS
        )
    )
    graph.add_step("ErrorCatcher", name="error_catcher", full_event=True, after="")

    if deploy:
        root_function.apply(v3io_cred())
        root_function.deploy()
    else:
        server = root_function.to_mock_server()
        server.test(
            body={
                "chunk_size": 100,
                "num_events": 3,
                "max_fact": 100,
                "err_rate": 0,
                # "__empty": True,
                "none_count": 0,
            }
        )
        server.wait_for_completion()


if __name__ == "__main__":
    deploy = False
    if len(sys.argv) >= 2 and sys.argv[1] == "deploy":
        deploy = True
    main(deploy=deploy)
