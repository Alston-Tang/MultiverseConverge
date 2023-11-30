import json
import os
import time
import uuid
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, AirflowException
from airflow.decorators import task_group
from airflow.models import Param, Variable
from airflow.operators.python import PythonOperator

from bson import ObjectId

from lock import try_lock_video, unlock_video, try_lock_archive, unlock_archive
from webdav import upload, get_client
from file import prepare_temp_storage, get_temp_file_abs_path
from mongoengine import connect
from data import BilibiliUploadResult, BilibiliArchiveInfo
from command import run_command


def check_binary_exists(ti, name):
    bin_root = Variable.get("bin_root")
    if bin_root is None:
        raise AirflowException("bin_root variable is not defined")
    path = os.path.join(bin_root, name)
    if os.path.isfile(path):
        ti.xcom_push(f"{name}_path", path)
        return True
    return False


def mongoengine_connect():
    mongodb_uri = Variable.get(key="mongodb_uri")
    print(f"connect to mongodb {mongodb_uri}")
    connect(host=mongodb_uri, alias='airflow', db='airflow')


on_new_record = DAG(
    dag_id="on_new_record",
    catchup=False,
    start_date=datetime(year=2023, month=11, day=16),
    schedule_interval=None,
    params={
        "path": Param(None),
        "file_obj_id": Param(None),
        "archive_id": Param(None)
    },
)


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


def setup(**kwargs):
    ti = kwargs["ti"]
    prepare_temp_storage(ti)
    mongoengine_connect()
    path: str = kwargs["path"]
    filename: str = os.path.basename(path)
    file_obj_id: str = kwargs["file_obj_id"]
    archive_id: str = kwargs["archive_id"]
    bililive_root: str = Variable.get("bililive_root")
    abs_path: str = os.path.join(bililive_root, path)

    ti.xcom_push(key="path", value=path)
    ti.xcom_push(key="abs_path", value=abs_path)
    ti.xcom_push(key="filename", value=filename)
    ti.xcom_push(key="file_obj_id", value=file_obj_id)
    ti.xcom_push(key="archive_id", value=archive_id)

    while True:
        video_lock_id = try_lock_video(ObjectId(file_obj_id))
        if video_lock_id is not None:
            break
        print(f"cannot acquire lock for video {path} ({file_obj_id}), wait for 20 seconds and retry")
        time.sleep(20)
    ti.xcom_push(key="video_lock_id", value=str(video_lock_id))


def teardown(**kwargs):
    ti = kwargs["ti"]
    mongoengine_connect()
    file_obj_id = ti.xcom_pull(key="file_obj_id", task_ids="setup")
    video_lock_id = ti.xcom_pull(key="video_lock_id", task_ids="setup")
    print(f"file_obj_id={file_obj_id}, lock_id={video_lock_id}")
    if file_obj_id and video_lock_id:
        print(f"unlock video {file_obj_id} (lock_id={video_lock_id})")
        unlock_video(ObjectId(file_obj_id), lock_id=ObjectId(video_lock_id))


setup_task = PythonOperator(
    task_id="setup",
    op_kwargs={
        "path": "{{ params.path }}",
        "file_obj_id": "{{ params.file_obj_id }}",
        "archive_id": "{{ params.archive_id }}"},
    python_callable=setup,
    dag=on_new_record,
    execution_timeout=timedelta(minutes=10)
)

"""
upload_webdav_task = PythonOperator(
    task_id="upload_webdav",
    python_callable=upload_webdav,
    dag=on_new_record,
)
"""

teardown_task = PythonOperator(
    task_id="teardown",
    python_callable=teardown,
    dag=on_new_record
)


def ensure_danmaku_factory(**kwargs):
    ti = kwargs["ti"]
    if not check_binary_exists(ti, "DanmakuFactory"):
        raise AirflowException("DanmakuFactory binary doesn't exist")


def convert_danmaku_to_ass(**kwargs):
    ti = kwargs["ti"]
    danmaku_factory_path = ti.xcom_pull(key="DanmakuFactory_path",
                                        task_ids="generate_danmaku_version.ensure_danmaku_factory")
    video_path = ti.xcom_pull(key="path", task_ids="setup")
    xml_path = os.path.splitext(video_path)[0] + ".xml"
    bililive_root = Variable.get("bililive_root")
    xml_abs_path = os.path.join(bililive_root, xml_path)
    if not os.path.isfile(xml_abs_path):
        raise AirflowException(f"no xml file found at location {xml_abs_path}")
    ass_filename = os.path.splitext(os.path.basename(xml_path))[0] + ".ass"
    temp_ass_path = get_temp_file_abs_path(ti, ass_filename)
    command = [danmaku_factory_path, "-o", temp_ass_path, "-i", xml_abs_path, "-O", "255"]
    return_code = run_command(command)
    if return_code != 0:
        raise AirflowException(f"{command} failed")
    # check if ass file has been generated
    if not os.path.isfile(temp_ass_path):
        raise AirflowException("DanmakuFactory command returns 0 but cannot find generated file")
    ti.xcom_push("ass_path", temp_ass_path)


def ensure_ffmpeg(**kwargs):
    ti = kwargs["ti"]
    # check if ffmpeg has been installed
    command = ["ffmpeg", "-version"]
    return_code = run_command(command)
    if return_code == 0:
        ti.xcom_push("ffmpeg_path", "ffmpeg")
        return
    raise AirflowException("ffmpeg is not installed on this host")


def generate_danmaku_record(**kwargs):
    ti = kwargs["ti"]
    bililive_root = Variable.get("bililive_root")
    ffmpeg_path = ti.xcom_pull(key="ffmpeg_path", task_ids="generate_danmaku_version.ensure_ffmpeg")
    ass_file = ti.xcom_pull(key="ass_path", task_ids="generate_danmaku_version.convert_danmaku_to_ass")
    path = ti.xcom_pull(key="path", task_ids="setup")
    abs_path = os.path.join(bililive_root, path)
    filename = ti.xcom_pull(key="filename", task_ids="setup")
    [filename_no_extension, extension] = os.path.splitext(filename)
    danmaku_filename = filename_no_extension + "_danmaku.mp4"
    output_path = get_temp_file_abs_path(ti, danmaku_filename)
    if ass_file is None:
        raise AirflowException("cannot get previously generated ass file from xcom")
    print(f"using {ass_file} and {abs_path} to generate {output_path}")
    command =  [ffmpeg_path, "-hwaccel", "vaapi", "-hwaccel_device", "/dev/dri/renderD128", "-hwaccel_output_format", "vaapi",
         "-i", abs_path, "-vf", f"scale_vaapi,hwmap=mode=read+write+direct,format=nv12,ass={ass_file},hwmap", "-c:v",
         "hevc_vaapi", output_path]
    return_code = run_command(command)
    if return_code != 0:
        raise AirflowException(f"{command} returns {return_code}")
    ti.xcom_push("danmaku_path", output_path)


@task_group(
    group_id="generate_danmaku_version",
    dag=on_new_record
)
def generate_danmaku_version_group():
    ensure_danmaku_factory_task = PythonOperator(
        task_id="ensure_danmaku_factory",
        python_callable=ensure_danmaku_factory,
        dag=on_new_record
    )
    convert_danmaku_to_ass_task = PythonOperator(
        task_id="convert_danmaku_to_ass",
        python_callable=convert_danmaku_to_ass,
        dag=on_new_record
    )
    ensure_ffmpeg_task = PythonOperator(
        task_id="ensure_ffmpeg",
        python_callable=ensure_ffmpeg,
        dag=on_new_record
    )
    generate_danmaku_record_task = PythonOperator(
        task_id="generate_danmaku_record",
        python_callable=generate_danmaku_record,
        dag=on_new_record
    )
    ensure_danmaku_factory_task >> convert_danmaku_to_ass_task
    ensure_ffmpeg_task >> generate_danmaku_record_task
    convert_danmaku_to_ass_task >> generate_danmaku_record_task


def ensure_rush_c(**kwargs):
    ti = kwargs["ti"]
    if not check_binary_exists(ti, "rush_c_upload"):
        raise AirflowException("RushC upload program doesn't exist")
    if not check_binary_exists(ti, "rush_c_submit"):
        raise AirflowException("RushC submit program doesn't exist")


def setup_for_upload_type(ti, upload_type):
    if upload_type == "raw":
        file = ti.xcom_pull(key="abs_path", task_ids="setup")
    elif upload_type == "danmaku":
        file = ti.xcom_pull(key="danmaku_path", task_ids="generate_danmaku_version.generate_danmaku_record")
    else:
        raise AirflowException(f"unknown upload type {upload_type}")
    return file

def rush_c_upload(**kwargs):
    ti = kwargs["ti"]
    upload_type = kwargs["upload_type"]
    file = setup_for_upload_type(ti, upload_type)
    cookie_path = Variable.get("cookie_path")
    file_obj_id = ti.xcom_pull(key="file_obj_id", task_ids="setup")
    rush_c_upload_path = ti.xcom_pull(key="rush_c_upload_path", task_ids=f"upload_to_bilibili_{upload_type}.ensure_rush_c")
    summary_file_name = str(uuid.uuid1())
    execution_summary_path = get_temp_file_abs_path(ti, summary_file_name)

    command = [rush_c_upload_path, "--cookie", cookie_path, "--execution-summary-path", execution_summary_path, file]
    return_code = run_command(command)
    if return_code != 0:
        raise AirflowException(f"{command} returns {return_code}")
    with open(execution_summary_path) as summary_json_file:
        summary = json.load(summary_json_file)
        mongoengine_connect()
        query_result = BilibiliUploadResult.objects(RecorderEventId=ObjectId(file_obj_id),
                                                                  Type=upload_type)
        if len(query_result) > 0:
            bilibili_upload_result = query_result[0]
        else:
            bilibili_upload_result = BilibiliUploadResult(RecorderEventId=ObjectId(file_obj_id), Type=upload_type)

        bilibili_upload_result.Title = summary["Title"]
        bilibili_upload_result.Filename = summary["Filename"]
        bilibili_upload_result.Desc = summary["Desc"]

        bilibili_upload_result.save()
        ti.xcom_push("bilibili_upload_result_id", str(bilibili_upload_result.id))


def lock_archive(**kwargs):
    ti = kwargs["ti"]
    mongoengine_connect()
    archive_id = ti.xcom_pull(key="archive_id", task_ids="setup")
    while True:
        lock_id = try_lock_archive(ObjectId(archive_id))
        if lock_id is not None:
            break
        print(f"cannot acquire lock for archive {archive_id}, wait for 20 seconds and retry")
        time.sleep(20)
    ti.xcom_push("archive_lock_id", str(lock_id))


def teardown_unlock_archive(**kwargs):
    ti = kwargs["ti"]
    mongoengine_connect()
    upload_type = kwargs["upload_type"]
    archive_id = ti.xcom_pull(key="archive_id", task_ids="setup")
    lock_id = ti.xcom_pull(key="archive_lock_id", task_ids=f"upload_to_bilibili_{upload_type}.lock_archive")
    print(lock_id)
    unlock_archive(ObjectId(archive_id), ObjectId(lock_id))


def construct_submit_command(archive_info, execution_summary_path, rush_c_submit_path, bvid):
    cookie_path = Variable.get("cookie_path")
    cover_path = Variable.get("cover_path")
    archive_title = ""
    prev_live_title = None
    title_date_str = None
    files = []
    for video in archive_info["Videos"]:
        title = video["Title"]
        filename = video["Filename"]
        title_split = parse_title(title)
        files.append(f"{filename}:{title}")
        if title_date_str is None:
            title_date_str = title_split["date_str"]

        live_title = title_split["live_title"]
        if live_title != prev_live_title:
            archive_title = archive_title + live_title + '/'
            prev_live_title = live_title
    if archive_title[-1] == '/':
        archive_title = archive_title[:-1]
    archive_title = f"【直播录像】{title_date_str}-" + archive_title

    command = [rush_c_submit_path, "--cookie", cookie_path, "--cover", cover_path, "--execution-summary-path", execution_summary_path, "--title", archive_title]
    if bvid is not None:
        command.append("--vid")
        command.append(bvid)
    command.append(','.join(files))
    return command



def parse_title(title):
    title_split = title.split('_')
    if len(title_split) < 4:
        raise AirflowException(f"cannot parse title {title}")
    date_str = title_split[0]
    live_title = title_split[2]
    time_str = title_split[3]
    return {"date_str": date_str, "live_title": live_title, "time_str": time_str}

def video_sort_key(video):
    title = video["Title"]
    title_split = parse_title(title)
    return f"{title_split['date_str']}_{title_split['time_str']}"


def rush_c_submit(**kwargs):
    ti = kwargs["ti"]
    mongoengine_connect()
    upload_type = kwargs["upload_type"]
    archive_id = ti.xcom_pull(key="archive_id", task_ids="setup")
    rush_c_submit_path = ti.xcom_pull(key="rush_c_submit_path", task_ids=f"upload_to_bilibili_{upload_type}.ensure_rush_c")
    bilibili_upload_result_id = ti.xcom_pull(
        key="bilibili_upload_result_id",
        task_ids=f"upload_to_bilibili_{upload_type}.rush_c_upload")
    upload_result = BilibiliUploadResult.objects.get(id=ObjectId(bilibili_upload_result_id))

    archive_info_query = BilibiliArchiveInfo.objects(ArchiveId=ObjectId(archive_id))
    if len(archive_info_query) == 0:
        archive_info = BilibiliArchiveInfo(ArchiveId=archive_id)
    else:
        archive_info = archive_info_query[0]

    # don't save this until submit() success
    archive_info["Videos"].append(upload_result)
    archive_info["Videos"].sort(key=video_sort_key)

    bvid = archive_info["Bvid"]
    print(f"bvid={bvid}")

    summary_file_name = str(uuid.uuid1())
    execution_summary_path = get_temp_file_abs_path(ti, summary_file_name)
    command = construct_submit_command(archive_info, execution_summary_path, rush_c_submit_path, bvid)
    return_code = run_command(command)
    if return_code != 0:
        raise AirflowException(f"{command} returns {return_code}")

    with open(execution_summary_path) as summary_json_file:
        summary = json.load(summary_json_file)
        if "aid" not in summary or "bvid" not in summary:
            raise AirflowException("submit program returns 0 but bvid/aid cannot be found from execution summary")
        if archive_info["Bvid"] is None:
            archive_info["Bvid"] = summary["bvid"]
        if archive_info["Aid"] is None:
            archive_info["Aid"] = summary["aid"]

    archive_info.save()


def create_bilibili_upload_group(upload_type):
    @task_group(
        group_id=f"upload_to_bilibili_{upload_type}",
        dag=on_new_record
    )
    def upload_to_bilibili_group(upload_type_):
        ensure_rush_c_task = PythonOperator(
            task_id="ensure_rush_c",
            python_callable=ensure_rush_c,
            dag=on_new_record
        )
        rush_c_upload_task = PythonOperator(
            task_id="rush_c_upload",
            python_callable=rush_c_upload,
            dag=on_new_record,
            op_kwargs={"upload_type": upload_type_},
            retries=3
        )
        lock_archive_task = PythonOperator(
            task_id="lock_archive",
            python_callable=lock_archive,
            dag=on_new_record,
            execution_timeout=timedelta(minutes=10),
        )
        unlock_archive_task = PythonOperator(
            task_id="unlock_archive",
            python_callable=teardown_unlock_archive,
            dag=on_new_record,
            op_kwargs={"upload_type": upload_type_}
        )
        rush_c_submit_task = PythonOperator(
            task_id="rush_c_submit",
            python_callable=rush_c_submit,
            dag=on_new_record,
            op_kwargs={"upload_type": upload_type_},
            retries=3,
        )
        ensure_rush_c_task >> rush_c_upload_task >> lock_archive_task >> rush_c_submit_task >> unlock_archive_task.as_teardown(setups=lock_archive_task)

    return upload_to_bilibili_group(upload_type)


setup_task.as_setup() >> [create_bilibili_upload_group("raw"), generate_danmaku_version_group()] >> teardown_task.as_teardown()
