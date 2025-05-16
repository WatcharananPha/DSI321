import lakefs_client
from lakefs_client.models import CommitCreation
from lakefs_client.client import LakeFSClient
import os
import datetime
from config.path_config import (
    get_lakefs_client_config,
    LAKEFS_REPOSITORY,
    LAKEFS_BRANCH,
    LAKEFS_EGAT_DATA_PATH_PREFIX
)

def get_lakefs_client():
    client_config = get_lakefs_client_config()
    configuration = lakefs_client.Configuration()
    configuration.host = client_config["endpoint"]
    configuration.username = client_config["access_key_id"]
    configuration.password = client_config["secret_access_key"]
    return LakeFSClient(configuration)

def upload_and_commit_data_to_lakefs(
    local_file_path: str,
    lakefs_object_name: str,
    commit_message_prefix: str = "Ingest EGAT Data",
    commit_metadata: dict | None = None
) -> str:
    if not os.path.exists(local_file_path):
        return None

    client = get_lakefs_client()
    full_lakefs_path = (LAKEFS_EGAT_DATA_PATH_PREFIX.rstrip('/') + '/' + lakefs_object_name).lstrip('/')

    with open(local_file_path, 'rb') as f_content:
        client.objects.upload_object(
            repository=LAKEFS_REPOSITORY,
            branch=LAKEFS_BRANCH,
            path=full_lakefs_path,
            content=f_content
        )

    final_commit_message = f"{commit_message_prefix}: {lakefs_object_name} at {datetime.datetime.now(datetime.timezone.utc).isoformat()}"
    
    default_metadata = {
        "source_application": "Prefect EGAT Ingestion Pipeline",
        "original_local_file": os.path.basename(local_file_path),
        "upload_timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    
    if commit_metadata:
        default_metadata.update(commit_metadata)

    commit_input = CommitCreation(message=final_commit_message, metadata=default_metadata)
    
    commit_result = client.commits.commit(
        repository=LAKEFS_REPOSITORY,
        branch=LAKEFS_BRANCH,
        commit_creation=commit_input
    )
    
    return commit_result.id