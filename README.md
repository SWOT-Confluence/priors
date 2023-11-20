# priors

The priors module creates and uploads a new version of the SoS priors and overwrites GRADES data with gaged prior data for constrained runs.

Priors runs on the continent level.

The priors module will create priors for the gage data indicated by the `-p` argument. When overwriting GRADES data for constrained runs, the indexes and source of the overwritten data are kept to track data provenance.

## installation

Build a Docker image: `docker build -t priors .`

## execution

**Command line arguments:**
- -i: index to locate continent in JSON file
- -r: run type for workflow execution: 'constrained' or 'unconstrained'
- -p: list of data to generate priors for: usgs, riggs, gbpriors
- -l: forces priors to pull a certail level sos ex: 0000

**Execute a Docker container:**

AWS credentials will need to be passed as environment variables to the container so that `priors` may access AWS infrastructure to generate JSON files.

```bash
# Credentials
export aws_key=XXXXXXXXXXXXXX
export aws_secret=XXXXXXXXXXXXXXXXXXXXXXXXXX

# Docker run command
docker run --rm --name priors -e AWS_ACCESS_KEY_ID=$aws_key -e AWS_SECRET_ACCESS_KEY=$aws_secret -e AWS_DEFAULT_REGION=$default_region -e AWS_BATCH_JOB_ARRAY_INDEX=3 -v /data/priors:/data priors:latest -i -235 -r constrained -p usgs riggs gbpriors
```

## tests

1. Run the unit tests: `python3 -m unittest discover tests`
(Note test data is not included.)

## deployment

There is a script to deploy the Docker container image and Terraform AWS infrastructure found in the `deploy` directory.

Script to deploy Terraform and Docker image AWS infrastructure

REQUIRES:

- jq (<https://jqlang.github.io/jq/>)
- docker (<https://docs.docker.com/desktop/>) > version Docker 1.5
- AWS CLI (<https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html>)
- Terraform (<https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli>)

Command line arguments:

[1] registry: Registry URI
[2] repository: Name of repository to create
[3] prefix: Prefix to use for AWS resources associated with environment deploying to
[4] s3_state_bucket: Name of the S3 bucket to store Terraform state in (no need for s3:// prefix)
[5] profile: Name of profile used to authenticate AWS CLI commands

Example usage: ``./deploy.sh "account-id.dkr.ecr.region.amazonaws.com" "container-image-name" "prefix-for-environment" "s3-state-bucket-name" "confluence-named-profile"`

Note: Run the script from the deploy directory.