import os
import pathlib
import time
from datetime import timedelta

from airflow import AirflowException
from airflow.decorators import task, setup, teardown
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from bson import ObjectId

from data import mongoengine_connect, Archive, ArchiveVideo, FileClosedEventDoc
from file import prepare_temp_storage
from lock import try_lock_video, unlock_video
from util import construct_title_from_videos, check_binary_exists
from webdav import get_client, upload


@task()
def ensure_rush_c(**context):
    ti = context["ti"]
    if not check_binary_exists(ti, "rush_c_upload"):
        raise AirflowException("RushC upload program doesn't exist")
    if not check_binary_exists(ti, "rush_c_submit"):
        raise AirflowException("RushC submit program doesn't exist")


@task(execution_timeout=timedelta(minutes=10))
def setup(**context):
    ti = context["ti"]

    prepare_temp_storage(ti)
    mongoengine_connect()

    bililive_root = Variable.get("bililive_root")

    path = None
    filename = None
    abs_path = None
    file_obj_id = None
    archive_id = None
    dry_run = False

    if "path" in context["params"]:
        path = context["params"]["path"]
        filename = os.path.basename(path)
        abs_path = os.path.join(bililive_root, path)
    if "file_obj_id" in context["params"]:
        file_obj_id = context["params"]["file_obj_id"]
    if "archive_id" in context["params"]:
        archive_id = context["params"]["archive_id"]
    if "dry_run" in context["params"]:
        dry_run = context["params"]["dry_run"]
        print(f"dry_run == {dry_run}")

    if path is not None:
        ti.xcom_push(key="path", value=path)
    if abs_path is not None:
        ti.xcom_push(key="abs_path", value=abs_path)
    if filename is not None:
        ti.xcom_push(key="filename", value=filename)
    if file_obj_id is not None:
        ti.xcom_push(key="file_obj_id", value=file_obj_id)
    if archive_id is not None:
        ti.xcom_push(key="archive_id", value=archive_id)
    if dry_run is not None:
        ti.xcom_push(key="dry_run", value=dry_run)
    else:
        ti.xcom_push(key="dry_run", value=False)

    if file_obj_id:
        print(f"try to acquire lock for video {file_obj_id} ({path})")
        while True:
            video_lock_id = try_lock_video(ObjectId(file_obj_id))
            if video_lock_id is not None:
                break
            print(f"cannot acquire lock for video {file_obj_id} ({path}), wait for 20 seconds and retry")
            time.sleep(20)
        ti.xcom_push(key="video_lock_id", value=str(video_lock_id))


@task
def teardown(**context):
    ti = context["ti"]
    mongoengine_connect()
    file_obj_id = ti.xcom_pull(key="file_obj_id", task_ids="setup")
    video_lock_id = ti.xcom_pull(key="video_lock_id", task_ids="setup")
    print(f"file_obj_id={file_obj_id}, lock_id={video_lock_id}")
    if file_obj_id and video_lock_id:
        print(f"unlock video {file_obj_id} (lock_id={video_lock_id})")
        unlock_video(ObjectId(file_obj_id), lock_id=ObjectId(video_lock_id))


@task
def validate_record_info(**context):
    ti = context["ti"]
    mongoengine_connect()
    file_obj_id = ti.xcom_pull(key="file_obj_id", task_ids="setup")
    file_event = FileClosedEventDoc.objects.get(id=ObjectId(file_obj_id))
    if file_event["EventData"]["Duration"] < 30:
        raise AirflowException(f'record duration={file_event["EventData"]["Duration"]}, which is shorter than threshold')


def create_upload_archive_to_webdav_task(key, task_ids):
    @task
    def upload_archive_to_webdav_task(**context):
        ti = context["ti"]
        archive_id = ti.xcom_pull(key=key, task_ids=task_ids)
        if archive_id is None:
            raise AirflowException(f"cannot get archive id from key={key}, task_ids={task_ids}")
        mongoengine_connect()

        print(archive_id)
        archive = Archive.objects.get(id=ObjectId(archive_id))
        print(f"got {len(archive.Videos)} videos from archive")

        directory_name = construct_title_from_videos(archive.Videos)
        remote_path_prefix = f"{Variable.get('alist_directory')}/{directory_name}"
        bililive_root = Variable.get("bililive_root")
        client = get_client()
        for video in archive.Videos:
            title = pathlib.Path(video["RelativePath"]).stem
            local_path = f"{bililive_root}/{video['RelativePath']}"
            remote_path = f"{remote_path_prefix}/{title}.flv"
            print(f"upload from {local_path} to {remote_path}")
            upload(client, remote_path=remote_path, local_path=local_path)

    return upload_archive_to_webdav_task()


def upload_webdav(**kwargs):
    ti = kwargs["ti"]
    path = ti.xcom_pull(key="path", task_ids="setup")
    bililive_root = Variable.get("bililive_root")
    local_path = os.path.join(bililive_root, path)
    remote_path_prefix = Variable.get("alist_directory")
    if remote_path_prefix[-1] != '/':
        remote_path_prefix += '/'
    remote_path = remote_path_prefix + path
    client = get_client()
    print(f"upload file from {local_path} to {remote_path}")
    upload(client, remote_path=remote_path, local_path=local_path)


upload_webdav_task = PythonOperator(
    task_id="upload_webdav",
    python_callable=upload_webdav,
    dag=None,
)