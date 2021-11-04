# priors

The priors module generates and uploads a new version of the SoS priors and overwrites GRADES data with gaged prior data.

Notes:
- Current iteration copies local SoS file and ignores AWS infrastructure.
- This program uses `boto3` to interface with the AWS API.

# installation

1. Clone the repository to your file system.
2. confluence-aws is best run with Python virtual environments so install venv and create a virutal environment: https://docs.python.org/3/library/venv.html
3. Activate the virtual environment and use pip to install dependencies: `pip install -r requirements.txt`

# execution

Command line arguments
----------------------
1. run_type (required): 'unconstrained' or 'constrained' 
2. grdc (optional): 'grdc'
3. usgs (optional): 'usgs'
4. gbpriors (optional): 'gbpriors'

To execute
----------
1. Activate your virtual environment.
2. Run `python3 update_priors.py constrained gbpriors usgs grdc` 

# tests

1. Run the unit tests: `python3 -m unittest discover tests`
(Note test data is not included.)