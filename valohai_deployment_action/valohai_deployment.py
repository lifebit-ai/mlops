import argparse
import json
import logging
import os

import requests
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Load Valohai yaml file to get datum names (endpoint configurations)
valohai_config = yaml.load(open("valohai.yaml", "r"), Loader=yaml.FullLoader)

# Valohai authentication token
AUTH_TOKEN = os.getenv('AUTH_TOKEN')
# Project id for event extraction model in Valohai
PROJECT_ID = os.getenv('PROJECT_ID')
# Deployment ID for this deployment in Valohai
DEPLOYMENT_ID = os.getenv('DEPLOYMENT_ID')

headers = {
    'Authorization': 'Token {}'.format(AUTH_TOKEN),
    'Content-Type': 'application/json'
}


def get_datum_ids_of_files_for_deployment():
    """
    Create a list of files (valohai.yaml) -> valohai datum id mapping
    to be send as payload to create a new version
    """
    # If there are no files required for this endpoint
    if 'files' not in valohai_config[0]['endpoint']:
        return {}

    # Create a dictionary of data files based on valohai.yaml config
    required_data_files = dict(
        (file['name'], file['path'])
        for file in valohai_config[0]['endpoint']['files']
    )

    datum_ids = {}

    # If any data file is required for the endpoint, get it's datum id
    if required_data_files:
        datum_api_url = 'https://app.valohai.com/api/v0/datum-aliases/'
        datum_res = requests.get(datum_api_url, headers=headers)
        datum_res.raise_for_status()

        datums = json.loads(datum_res.content)

        # Filter out datum for this project get a mapping of file UUIDs
        for datum in datums['results']:
            if datum['project']['id'] == PROJECT_ID:
                datum_ids[datum['datum']['name']] = datum['datum']['id']

    # Verify all required data files are covered in the  datum_ids
    if set(required_data_files.values()) == set(datum_ids.keys()):
        return dict((file, datum_ids[path]) for file, path in required_data_files.items())
    else:
        logging.error("No Datum Files found. Deployment might not succeed!")
        return None


def create_version(
        branch: str,
        commit_id: str,
        replicas: int,
        memory_limit: int,
        cpu_request: int
) -> None:
    """
    Deploy a new version for `PROJECT_ID/DEPLOYMENT_ID`.

    :param branch: Github branch name
    :param commit_id: Github Commit id to be used for the version
    :param replicas: No. of replicas instances to deploy for this version
    :param memory_limit: Memory limit for this deployment
    :param cpu_request: Proportion of cpus to use for this deployment
    """

    # VALOHAI API URLs to fetch new changes from github repo and to deploy a new version
    fetch_repo_api_url = 'https://app.valohai.com/api/v0/projects/{0}/fetch/'.format(
        PROJECT_ID)
    deployment_api_url = 'https://app.valohai.com/api/v0/deployment-versions/'

    # Fetch all new changes from the repository
    # https://app.valohai.com/api/docs/#projects-fetch
    # This will fetch changes from all the branches that
    # you've defined on the Project->Settings->Repository tab
    fetch_repo_changes = requests.post(fetch_repo_api_url, json={
        'id': PROJECT_ID}, headers=headers)

    datum_ids = get_datum_ids_of_files_for_deployment()

    endpoint_config = {
        'predict': {
            'enabled': True,
            'files': datum_ids,
            'replicas': replicas,
            'memory_limit': memory_limit,
            'cpu_request': cpu_request
        },
    }

    # Deployment name is same as the commit id
    # Additionally we also use `VH_CLEAN` valohai env variable
    # to ensure updated image is pulled from ECR
    payload = {
        'commit': commit_id,
        'deployment': DEPLOYMENT_ID,
        'name': "{}.{}".format(branch, commit_id),
        'enabled': True,
        'endpoint_configurations': endpoint_config,
        'environment_variables': {'VH_CLEAN': '1'},
    }

    # Send a POST request to create a new version for this deployment
    deployment_response = requests.post(
        deployment_api_url, json=payload, headers=headers)

    logging.info(json.loads(deployment_response.content))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Valohai version deployment.""")

    parser.add_argument(
        "--branch",
        "-b",
        type=str,
        dest="branch",
        help="Branch Name",
        required=True
    )

    parser.add_argument(
        "--commit_id",
        "-ci",
        type=str,
        dest="commit_id",
        help="Commit ID",
        required=True
    )

    parser.add_argument(
        "--replicas",
        "-r",
        type=int,
        dest="replicas",
        help="No. of replicas instances to deploy for this version",
        default=1
    )

    parser.add_argument(
        "--memory_limit",
        "-mem",
        type=int,
        dest="memory_limit",
        help="Memory limit for this endpoint",
        default=0
    )

    parser.add_argument(
        "--cpu_request",
        "-cpu",
        type=float,
        dest="cpu_request",
        help="Proportion of CPUs to use for this verion",
        default=0.1
    )

    args = parser.parse_args()
    create_version(args.branch, args.commit_id, args.replicas, args.memory_limit, args.cpu_request)