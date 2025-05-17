import os
import pandas as pd
from lakefs_client import LakeFSClient
from lakefs_client.models import CommitCreation
from lakefs_client.client import LakeFSClientConfig
from lakefs_client.exceptions import NotFoundException

class LakeFSLoader:
    def __init__(self, repository_name: str, lakefs_endpoint: str, access_key_id: str, secret_access_key: str):
        self.repository_name = repository_name
        config = LakeFSClientConfig(
            host=lakefs_endpoint,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key
        )
        self.client = LakeFSClient(config)

    def _ensure_branch_exists(self, branch_name: str, source_branch: str = "main"):
        if not self.client.branches.get_branch(repository=self.repository_name, branch=branch_name):
            self.client.branches.create_branch(
                repository=self.repository_name,
                branch_creation={'name': branch_name, 'source': source_branch}
            )

    def upload_dataframe(self, df: pd.DataFrame, branch: str, remote_path: str, file_format: str = "parquet"):
        if df.empty:
            return None
            
        self._ensure_branch_exists(branch)
        temp_filename = f"temp_data.{file_format}"
        
        if file_format == "parquet":
            df.to_parquet(temp_filename, index=False)
        else:
            df.to_csv(temp_filename, index=False, encoding='utf-8-sig')

        with open(temp_filename, 'rb') as f:
            self.client.objects.upload_object(
                repository=self.repository_name,
                branch=branch,
                path=remote_path,
                content=f
            )
            
        os.remove(temp_filename)
        return remote_path

    def commit_changes(self, branch: str, commit_message: str, metadata: dict = None):
        commit_creation = CommitCreation(message=commit_message, metadata=metadata or {})
        result = self.client.commits.commit(
            repository=self.repository_name,
            branch=branch,
            commit_creation=commit_creation
        )
        return result.id

    def merge_branch(self, source_branch: str, target_branch: str = "main", commit_message: str = None):
        if not commit_message:
            commit_message = f"Merge {source_branch} into {target_branch}"
            
        return self.client.refs.merge_into_branch(
            repository=self.repository_name,
            source_ref=source_branch,
            destination_branch=target_branch,
            merge={'message': commit_message}
        )