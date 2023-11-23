import os
from airflow import AirflowException
import tempfile


def prepare_temp_storage(ti):
    temp_prefix = tempfile.mkdtemp()
    print(f"create a tempdir {temp_prefix} for file storage")
    ti.xcom_push(key="tempdir", value=temp_prefix)
    return temp_prefix


def get_temp_file_abs_path(ti, relative_path):
    temp_prefix = ti.xcom_pull(key="tempdir", task_ids="setup")
    if temp_prefix is None:
        raise AirflowException("failed to get tempdir path from xcom")
    return os.path.join(temp_prefix, relative_path)
