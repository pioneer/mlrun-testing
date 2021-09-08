# Simple MLRun graph example with multiple outputs

**Step 1:** consumes a single input value, emits multiple values as a generator

**Step 2:** consumes each value from Step 1

# Usage

- `python main.py` to run the graph locally
- `python main.py deploy` to deploy to your Iguazio cluster (you'll need environment variables set, see `mlrun.sh.template` for the details)