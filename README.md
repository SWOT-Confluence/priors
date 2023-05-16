# priors

The priors module creates and uploads a new version of the SoS priors and overwrites GRADES data with gaged prior data for constrained runs.

Priors runs on the continent level.

The priors module will create priors for the gage data indicated by the `-p` argument. When overwriting GRADES data for constrained runs, the indexes and source of the overwritten data are kept to track data provenance.

# installation

Build a Docker image: `docker build -t priors .`

# execution

**Command line arguments:**
- -i: index to locate continent in JSON file
- -r: run type for workflow execution: 'constrained' or 'unconstrained'
- -p: list of data to generate priors for: usgs, riggs, gbpriors
- -l: forces priors to pull a certail level sos ex: 0000

**Execute a Docker container:**

AWS credentials will need to be passed as environment variables to the container so that `datagen` may access AWS infrastructure to generate JSON files.

```
# Credentials
export aws_key=XXXXXXXXXXXXXX
export aws_secret=XXXXXXXXXXXXXXXXXXXXXXXXXX

# Docker run command
docker run --rm --name priors -e AWS_ACCESS_KEY_ID=$aws_key -e AWS_SECRET_ACCESS_KEY=$aws_secret -e AWS_DEFAULT_REGION=$default_region -e AWS_BATCH_JOB_ARRAY_INDEX=3 -v /data/priors:/data priors:latest -i -235 -r constrained -p usgs grdc gbpriors
```

# tests

1. Run the unit tests: `python3 -m unittest discover tests`
(Note test data is not included.)