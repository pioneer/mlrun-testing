import os
import sys
from mlrun import code_to_function, v3io_cred


def main(deploy=False):
    project_name = os.getenv("PROJECT_NAME", "steps")

    root_function = code_to_function(
        "step1",
        project=project_name,
        filename="functions/step1.py",
        kind="serving",
        image=os.getenv("BASE_IMAGE", "mlrun/mlrun"),
    )

    graph = root_function.set_topology(
        "flow", engine="async", exist_ok=True
    )

    step2_function = root_function.add_child_function(
        "step2",
        url="./functions/step2.py",
        image=os.getenv("BASE_IMAGE", "mlrun/mlrun"),
    )

    if deploy:
        STEP1_KWARGS = {"class_name": "Step1", "name": "step1"}
        STEP1_V3IO_KWARGS = {"name": "step1_v3io", "path": f'projects/{project_name}/step1'}
        STEP2_KWARGS = {"class_name": "Step2", "name": "step2", "function": "step2"}
        STEP2_V3IO_KWARGS = {"name": "step2_v3io", "path": f'projects/{project_name}/step2'}
    else:
        STEP1_KWARGS = {"class_name": "functions.step1.Step1", "name": "step1"}
        STEP1_V3IO_KWARGS = {"name": "step1_v3io", "path": ""}
        STEP2_KWARGS = {"class_name": "functions.step2.Step2", "name": "step2"}
        STEP2_V3IO_KWARGS = {"name": "step2_v3io", "path": ""}

    (
        graph.to(**STEP1_KWARGS)
        .to(">>", **STEP1_V3IO_KWARGS)
        .to(**STEP2_KWARGS)
        .to('>>', **STEP2_V3IO_KWARGS)
    )

    if deploy:
        root_function.apply(v3io_cred())
        root_function.deploy()
    else:
        server = root_function.to_mock_server()
        server.test(body=5)
        server.wait_for_completion()
    

if __name__ == "__main__":
    deploy = False
    if len(sys.argv) >= 2 and sys.argv[1] == "deploy":
        deploy = True
    main(deploy=deploy)
