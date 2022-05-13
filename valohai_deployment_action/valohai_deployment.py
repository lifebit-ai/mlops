import argparse
import json
import logging
import os
from typing import Optional, Dict

import requests
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Load Valohai yaml file to get datum names (endpoint configurations)
valohai_config = yaml.load(open("../valohai.yaml", "r"), Loader=yaml.FullLoader)

# Valohai authentication token
AUTH_TOKEN = os.getenv('AUTH_TOKEN')
# Project id for event extraction model in Valohai
PROJECT_ID = os.getenv('PROJECT_ID')
# Deployment ID for this deployment in Valohai
DEPLOYMENT_ID = os.getenv('DEPLOYMENT_ID')

headers = {
    'Authorization': f'Token {AUTH_TOKEN}',
    'Content-Type': 'application/json'
}


def get_datum_ids() -> Optional[Dict]:
    """
    Create a list of files (valohai.yaml) -> valohai datum id mapping
    to be sent as payload to create a new version
    """
    # Create a dictionary of data files based on valohai.yaml config
    required_data_files = {file['name']: file['path']
                           for file in valohai_config[0]['endpoint']['files']}

    datum_ids = {}

    # If any data file is required for the endpoint, get its datum id
    if required_data_files:
        datum_api_url = 'https://app.valohai.com/api/v0/datum-aliases/'
        datum_res = requests.get(datum_api_url, headers=headers)
        datum_res.raise_for_status()

        datums = json.loads(datum_res.content)

        # Filter out datum for this project get a mapping of file UUIDs
        for datum in datums['results']:
            if datum['project']['id'] == PROJECT_ID:
                logging.info(datum['datum']['name'], datum['datum']['id'])
                datum_ids[datum['datum']['name']] = datum['datum']['id']

    # Verify all required data files are covered in the  datum_ids
    if set(required_data_files.values()) == set(datum_ids.keys()):
        return {file: datum_ids[path] for file, path in required_data_files.items()}
    else:
        logging.warning("Missing Datum Files. Deployment might not succeed!")


def create_version(commit_id: str) -> None:
    """
    Deploy a new version for `PROJECT_ID/DEPLOYMENT_ID`. 

    :param commit_id: Commit id to be used for the version
    """

    # VALOHAI API URLs to fetch new changes from GitHub repo and to deploy a new version
    fetch_repo_api_url = f'https://app.valohai.com/api/v0/projects/{PROJECT_ID}/fetch/'

    deployment_api_url = 'https://app.valohai.com/api/v0/deployment-versions/'

    # Fetch all new changes from the repository
    # https://app.valohai.com/api/docs/#projects-fetch
    # This will fetch changes from all the branches that
    # you've defined on the Project->Settings->Repository tab
    fetch_repo_changes = requests.post(fetch_repo_api_url, json={
        'id': PROJECT_ID}, headers=headers)

    datum_ids = get_datum_ids()

    # Deployment name is same as the commit id
    # Additionally we also use `VH_CLEAN` valohai env variable
    # to ensure updated image is pulled from ECR
    payload = {
        'commit': commit_id,
        'deployment': DEPLOYMENT_ID,
        'name': commit_id + "v91-local",
        'enabled': True,
        'endpoint_configurations': {'predict': {'enabled': True, "files": datum_ids}},
        'environment_variables': {'VH_CLEAN': '1'},
    }

    # Send a POST request to create a new version for this deployment
    deployment_response = requests.post(
        deployment_api_url, json=payload, headers=headers)

    deployment_response.raise_for_status()
    logging.info(json.loads(deployment_response.content))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Valohai version deployment.""")
    parser.add_argument(
        "--commit_id",
        "-ci",
        type=str,
        dest="commit_id",
        help="Commit ID",
        required=True
    )

    args = parser.parse_args()
    create_version(args.commit_id)
