import os
from datetime import datetime, timezone, timedelta

import requests

AUTH_TOKEN = os.getenv("AUTH_TOKEN")
headers = {"Authorization": f"Token {AUTH_TOKEN}", "Content-Type": "application/json"}
VALOHAI_API_BASE_URL = "https://app.valohai.com/api/v0/"
deployment_versions_api = VALOHAI_API_BASE_URL + "deployment-versions/"


def find_versions(all_versions):
    for version in all_versions['results']:
        url = version['commit']['urls']['display']
        if 'REAL-events' in url or 'REAL-relationship' in url or 'ner_v1_aug_21' in url:
            if version['enabled'] and version['effective_enabled']:
                version_url = version['url']
                v = requests.get(version_url, headers=headers)
                endpoint_id = v.json()['endpoints'][0]['id']
                if 'dev' not in v.json()['endpoints'][0]['endpoint_url']:
                    continue
                print(v.json()['name'], ":", v.json()['endpoints'][0]['endpoint_url'])

                now = datetime.now(timezone.utc)
                twelve_hours_before = now - timedelta(hours=12)
                logs = requests.get(
                    VALOHAI_API_BASE_URL + f"deployment-endpoints/{endpoint_id}/logs/",
                    params={"start": twelve_hours_before.strftime("%Y-%m-%dT%H:%M"),
                            "end": now.strftime("%Y-%m-%dT%H:%M")},
                    headers=headers,
                )
                logs = logs.json()
                key = next(iter(logs))
                last_message = logs[key][-1]
                try:
                    last_used_date, last_used_time = last_message['time'].split('T')
                except AttributeError:
                    continue
                last_used_time = last_used_time.split('.')[0]
                date_time_obj = datetime.strptime(last_used_date + " " + last_used_time, '%Y-%m-%d %H:%M:%S')

                delta = now.replace(tzinfo=None) - date_time_obj.replace(tzinfo=None)
                unused_h = delta.seconds / 60 / 60
                print("Unused", unused_h, "hours")


def main():
    versions = requests.get(deployment_versions_api, headers=headers)
    while True:
        versions = versions.json()
        find_versions(versions)
        if versions['next']:
            versions = requests.get(versions['next'], headers=headers)
        else:
            break


if __name__ == "__main__":
    main()
