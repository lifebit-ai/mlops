import argparse
import json
import logging
import os
import time
from typing import Dict, Optional

import requests
import yaml

from custom_exceptions import MissingDatumException, VersionNotCreatedException, AliasNotCreatedException, \
    ApiNotWorkingException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Load Valohai yaml file to get datum names (endpoint configurations)
valohai_config = yaml.load(open("valohai.yaml", "r"), Loader=yaml.FullLoader)

# Valohai authentication token
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
# Project id for event extraction model in Valohai
PROJECT_ID = os.getenv("PROJECT_ID")
# Deployment ID for this deployment in Valohai
DEPLOYMENT_ID = os.getenv("DEPLOYMENT_ID")

headers = {"Authorization": f"Token {AUTH_TOKEN}", "Content-Type": "application/json"}

VALOHAI_API_BASE_URL = "https://app.valohai.com/api/v0/"

DELAY_TIMES = {
    'relation': 1800,
    'ner': 600,
    'event': 1200
}


def get_datum_ids_of_files_for_deployment() -> Optional[Dict]:
    """
    Create a list of files (valohai.yaml) -> valohai datum id mapping
    to be send as payload to create a new version
    """
    # If there are no files required for this endpoint
    if "files" not in valohai_config[0]["endpoint"]:
        return {}

    # Create a dictionary of test_data files based on valohai.yaml config
    required_data_files = dict(
        (file["name"], file["path"]) for file in valohai_config[0]["endpoint"]["files"]
    )

    datum_ids = {}

    # If any test_data file is required for the endpoint, get it's datum id
    if required_data_files:
        datum_api_url = VALOHAI_API_BASE_URL + "datum-aliases/"
        datum_res = requests.get(datum_api_url, headers=headers)
        datum_res.raise_for_status()

        datums = json.loads(datum_res.content)

        # Filter out datum for this project get a mapping of file UUIDs
        for datum in datums["results"]:
            if datum["project"]["id"] == PROJECT_ID:
                datum_ids[datum["datum"]["name"]] = datum["datum"]["id"]

    # Verify all required test_data files are covered in the datum_ids
    if set(required_data_files.values()) == set(datum_ids.keys()):
        return dict(
            (file, datum_ids[path]) for file, path in required_data_files.items()
        )
    else:
        logging.error("No Datum Files found. Deployment will not succeed!")
        raise MissingDatumException("Missing Datum files.")


def check_api(url: str):
    logging.info("Waiting for version to be ready on Valohai")
    if 'real-relationship' in url:
        time.sleep(DELAY_TIMES['relation'])
    elif "ner_v1_aug_21" in url:
        time.sleep(DELAY_TIMES['ner'])
    else:
        time.sleep(DELAY_TIMES['event'])

    logging.info("Sending request...")
    if 'real-relationship' in url or "ner_v1_aug_21" in url:
        data = json.load(open("mlops/test_data/relation_ner_test_data.json"))
    else:
        data = json.load(open("mlops/test_data/event_test_data.json"))

    fetch_repo_changes = requests.post(url, json=data, headers={"Content-type": "application/json"})
    if fetch_repo_changes.status_code != 200:
        raise ApiNotWorkingException("API is not working. Check logs in Valohai version.")
    else:
        logging.info("API is ready!")


def create_version(
        branch: str,
        commit_id: str,
        replicas: int,
        memory_limit: int,
        cpu_request: int,
        alias_name: str,
) -> None:
    """
    Deploy a new version for `PROJECT_ID/DEPLOYMENT_ID`.

    :param branch: Github branch name
    :param commit_id: Github Commit id to be used for the version
    :param replicas: No. of replicas instances to deploy for this version
    :param memory_limit: Memory limit for this deployment
    :param cpu_request: Proportion of cpus to use for this deployment
    :param alias_name: Name for the new alias
    """

    # VALOHAI API URLs to fetch new changes from github repo and to deploy a new version
    fetch_repo_api_url = f"{VALOHAI_API_BASE_URL}projects/{PROJECT_ID}/fetch/"
    deployment_api_url = VALOHAI_API_BASE_URL + "deployment-versions/"
    deployment_aliases_api_url = VALOHAI_API_BASE_URL + "deployment-version-aliases/"

    # Fetch all new changes from the repository
    # https://app.valohai.com/api/docs/#projects-fetch
    # This will fetch changes from all the branches that
    # you've defined on the Project->Settings->Repository tab
    fetch_repo_changes = requests.post(
        fetch_repo_api_url, json={"id": PROJECT_ID}, headers=headers
    )

    datum_ids = get_datum_ids_of_files_for_deployment()

    endpoint_config = {
        "predict": {
            "enabled": True,
            "files": datum_ids,
            "replicas": replicas,
            "memory_limit": memory_limit,
            "cpu_request": cpu_request,
        },
    }

    # Deployment name is same as the commit id
    # Additionally we also use `VH_CLEAN` valohai env variable
    # to ensure updated image is pulled from ECR
    version_name = f"{branch}.{commit_id}"
    payload = {
        "commit": commit_id,
        "deployment": DEPLOYMENT_ID,
        "name": version_name,
        "enabled": True,
        "endpoint_configurations": endpoint_config,
        "environment_variables": {"VH_CLEAN": "1"},
        "inherit_environment_variables": True
    }

    # Send a POST request to create a new version for this deployment
    deployment_response = requests.post(
        deployment_api_url, json=payload, headers=headers
    )

    response = json.loads(deployment_response.content)
    logging.info(response)
    if deployment_response.status_code != 201:
        raise VersionNotCreatedException(f"Version can not be created! {deployment_response.status_code}")

    logging.info("New version created!")

    body = {
        "deployment": DEPLOYMENT_ID,
        "target": response["endpoints"][0]["version"],
        "name": alias_name,
    }

    get_alias_response = requests.get(
        deployment_aliases_api_url,
        params={"project": response["commit"]["project_id"]},
        headers=headers,
    )
    logging.info(f"Aliases found:")
    logging.info(json.loads(get_alias_response.content))

    for aliases in json.loads(get_alias_response.content)["results"]:
        if alias_name == aliases["name"]:
            logging.info("Alias is being updated.")

            # Send a PUT request to update an existing alias for this deployment
            alias_update_response = requests.put(
                aliases["url"],
                json={
                    "target": response["endpoints"][0]["version"],
                    "name": alias_name,
                },
                headers=headers,
            )

            logging.info(json.loads(alias_update_response.content))
            if alias_update_response.status_code != 200:
                raise AliasNotCreatedException(f"Alias can not be updated! {alias_update_response.status_code}")

            break
    else:
        logging.info("Alias is being created.")

        # Send a POST request to create a new alias for this deployment
        create_alias_response = requests.post(
            deployment_aliases_api_url, json=body, headers=headers
        )

        logging.info(json.loads(create_alias_response.content))
        if create_alias_response.status_code != 201:
            raise AliasNotCreatedException(f"Alias can not be created! {create_alias_response.status_code}")

    url = response['endpoint_urls']['predict']
    check_api(url)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""Valohai version deployment.""")

    parser.add_argument(
        "--branch", "-b", type=str, dest="branch", help="Branch Name", required=True
    )

    parser.add_argument(
        "--commit_id",
        "-ci",
        type=str,
        dest="commit_id",
        help="Commit ID",
        required=True,
    )

    parser.add_argument(
        "--replicas",
        "-r",
        type=int,
        dest="replicas",
        help="No. of replicas instances to deploy for this version",
        default=1,
    )

    parser.add_argument(
        "--memory_limit",
        "-mem",
        type=int,
        dest="memory_limit",
        help="Memory limit for this endpoint",
        default=0,
    )

    parser.add_argument(
        "--cpu_request",
        "-cpu",
        type=float,
        dest="cpu_request",
        help="Proportion of CPUs to use for this verion",
        default=0.1,
    )

    parser.add_argument(
        "--alias_name",
        "-a",
        type=str,
        dest="alias_name",
        help="Name for the new alias",
        default="staging",
    )

    parser.add_argument(
        "--commit_message",
        "-cm",
        type=str,
        dest="commit_message",
        help="commit message",
        default=None,
    )

    args = parser.parse_args()

    # cancel deployement if PR/commit message mentions non-deployement keywords.
    if (args.commit_message is not None and
            ('do-not-deploy' in args.commit_message or
             'dnd' in args.commit_message or
             'read-me-like' in args.commit_message)):
        logging.info('deployement cancelled')

    else:
        create_version(
            args.branch,
            args.commit_id,
            args.replicas,
            args.memory_limit,
            args.cpu_request,
            args.alias_name,
        )
