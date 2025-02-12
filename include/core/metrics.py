import os 
import requests
from airflow.decorators import task


@task
def get_external_queries(start: str, end: str, token: str) -> list[dict]:
    org_id = os.getenv("ASTRO_ORGANIZATION_ID")

    get_queries_url = f"https://api.astronomer-dev.io/private/v1alpha1/organizations/{org_id}/observability/external-queries?earliestTime={start}&latestTime={end}"

    print(f"Getting queries from {get_queries_url}")

    resp = requests.get(
        get_queries_url,
        headers={
            "Authorization": f"Bearer {token}",
            "X-Astro-Client-Identifier": "observe-cost-attribution",
        },
    )

    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        raise Exception(
            f"Failed to get queries: {resp.status_code}:{resp.reason} {resp.text}"
        )

    queries = resp.json().get("externalQueries")
    print(f"Collected {len(queries)} queries.")

    return queries

@task
def post_metrics(token: str, category: str, type: str, data: list) -> None:
    print(f"::group::Posting {len(data)} {type} items:")
    pprint(data, indent=2)
    print("::endgroup::")
    
    org_id = os.getenv("ASTRO_ORGANIZATION_ID")

    resp = requests.post(
        f"https://api.astronomer-dev.io/private/v1alpha1/organizations/{org_id}/observability/metrics",
        json={
            "category": category, 
            "type": f"{type}", 
            "metrics": data
        },
        headers={
            "Authorization": f"Bearer {token}",
            "X-Astro-Client-Identifier": "observe-cost-attribution",
        },
    )

    if resp.status_code != 200:
        raise Exception(f"Failed to post data: {resp.text}")
