import os
import sys
from mlrun import code_to_function, v3io_cred, set_environment
from storey.steps import Flatten
from copy import deepcopy
from mlrun.serving.server import MockEvent
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


def main(deploy=False):
    project_name = os.getenv("PROJECT_NAME", "mlrun-testing-uv-st")
    error_stream = (f"v3io:///projects/{project_name}/errors" if deploy else "",)

    root_function = code_to_function(
        "data-generator",
        project=project_name,
        filename="handler.py",
        kind="serving",
        image=os.getenv("BASE_IMAGE", "mlrun/mlrun"),
        requirements=["storey", "rand_string"],
    )
    # root_function.spec.error_stream = error_stream

    graph = root_function.set_topology("flow", engine="async", exist_ok=True)

    data_enricher_function = root_function.add_child_function(
        "data-enricher",
        url="handler.py",
        image=os.getenv("BASE_IMAGE", "mlrun/mlrun"),
        requirements=["storey", "rand_string"],
    )
    data_enricher_function.spec.error_stream = error_stream

    data_formatter_function = root_function.add_child_function(
        "data-formatter",
        url="handler.py",
        image=os.getenv("BASE_IMAGE", "mlrun/mlrun"),
        requirements=["storey", "rand_string"],
    )
    data_formatter_function.spec.error_stream = error_stream

    (
        graph.to("DataGenerator", name="data_generator")
        .to("storey.steps.Flatten")
        .to(
            ">>",
            name="data_generator_v3io",
            path=f"v3io:///projects/{project_name}/data_generator/output" if deploy else "",
        )
        .to(
            "DataEnricher",
            name="data_enricher",
            function="data-enricher" if deploy else None,
        )
        .error_handler("error_catcher")
        .to(
            ">>",
            name="data_enricher_v3io",
            path=f"v3io:///projects/{project_name}/data_enricher/output" if deploy else "",
        )
        .to(
            "DataFormatter",
            name="data_formatter",
            function="data-formatter" if deploy else None,
        )
        .to(
            ">>",
            name="data_formatter_v3io",
            path=f"v3io:///projects/{project_name}/data_formatter/output" if deploy else "",
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
                "num_events": 100,
                "max_fact": 100,
                "err_rate": 0.1,
            }
        )
        server.wait_for_completion()


if __name__ == "__main__":
    deploy = False
    if len(sys.argv) >= 2 and sys.argv[1] == "deploy":
        deploy = True
    main(deploy=deploy)
