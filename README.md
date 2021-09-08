# Simple MLRun graph example with multiple outputs using Flatten and error catching

**DataGenerator:** consumes a single input value, emits multiple values as a generator

**DataEnricher:** consumes each value from DataGenerator, imitates enrichment and error raising/handling

**DataFormatter:** last step, imitates final data processing

# Usage

Create your `mlrun.sh` from `mlrun.sh.template`, fill it with your values. Then run `source mlrun.sh` in order to be able to deploy (not needed for local runs).

- `python main.py` to run the graph locally
- `python main.py deploy` to deploy to your Iguazio cluster (you'll need environment variables set, see the above)