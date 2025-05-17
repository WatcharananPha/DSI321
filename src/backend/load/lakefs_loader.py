import pandas as pd
from lakefs_client import LakeFSClient
from lakefs_client.models import CommitCreation
from lakefs_client.client import LakeFSClientConfig
from lakefs_client.exceptions import NotFoundException
import io

class LakeFSLoader:
    def __init__(self, repository_name: str, lakefs_endpoint: str, access_key_id: str, secret_access_key: str):
        config = LakeFSClientConfig(
            host=lakefs_endpoint,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key
        )
        self.repository_name = repository_name
        self.client = LakeFSClient(config)

    def _ensure_branch_exists(self, branch_name: str, source_branch: str = "main"):
        if not self._branch_exists(branch_name):
            self.client.branches.create_branch(
                repository=self.repository_name,
                branch_creation={'name': branch_name, 'source': source_branch}
            )

    def _branch_exists(self, branch_name: str):
        try:
            self.client.branches.get_branch(repository=self.repository_name, branch=branch_name)
            return True
        except NotFoundException:
            return False

    def upload_dataframe_as_parquet(self, df: pd.DataFrame, branch: str, remote_path: str):
        if df.empty:
            return None
        self._ensure_branch_exists(branch)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)
        self.client.objects.upload_object(
            repository=self.repository_name,
            branch=branch,
            path=remote_path,
            content=parquet_buffer,
        )
        parquet_buffer.close()
        return remote_path

    def commit_changes(self, branch: str, commit_message: str, metadata: dict = None):
        commit_creation = CommitCreation(message=commit_message, metadata=metadata or {})
        commit_result = self.client.commits.commit(
            repository=self.repository_name,
            branch=branch,
            commit_creation=commit_creation
        )
        return commit_result.id

    def merge_branch(self, source_branch: str, target_branch: str = "main", commit_message: str = None):
        merge_input = {'message': commit_message or f"Merge {source_branch} into {target_branch}"}
        merge_result = self.client.refs.merge_into_branch(
            repository=self.repository_name,
            source_ref=source_branch,
            destination_branch=target_branch,
            merge=merge_input
        )
        return merge_result