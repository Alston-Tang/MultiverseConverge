import json
import os
import subprocess
from datetime import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, AirflowException
from airflow.decorators import task_group
from airflow.models import Param, Variable
from airflow.operators.python import PythonOperator

from bson import ObjectId

from lock import try_lock_video, unlock_video
from webdav import upload, get_client
from file import prepare_temp_storage, get_temp_file_abs_path
from mongoengine import connect
from data import BilibiliUploadResult


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
    ti.xcom_push(key="abs_path", value=path)
    ti.xcom_push(key="filename", value=filename)
    ti.xcom_push(key="file_obj_id", value=file_obj_id)
    ti.xcom_push(key="archive_id", value=archive_id)

    video_lock_id = try_lock_video(ObjectId(file_obj_id))
    if video_lock_id is None:
        raise AirflowException(f"can not lock video {file_obj_id} ({path})")
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
)

upload_webdav_task = PythonOperator(
    task_id="upload_webdav",
    python_callable=upload_webdav,
    dag=on_new_record,
)

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
    danmaku_factory_path = ti.xcom_pull(key="danmaku_factory_path",
                                        task_ids="generate_danmaku_version.ensure_danmaku_factory")
    video_path = ti.xcom_pull(key="path", task_ids="setup")
    xml_path = os.path.splitext(video_path)[0] + ".xml"
    bililive_root = Variable.get("bililive_root")
    xml_abs_path = os.path.join(bililive_root, xml_path)
    if not os.path.isfile(xml_abs_path):
        raise AirflowException(f"no xml file found at location {xml_abs_path}")
    ass_filename = os.path.splitext(os.path.basename(xml_path))[0] + ".ass"
    temp_ass_path = get_temp_file_abs_path(ti, ass_filename)
    res = subprocess.run(
        [danmaku_factory_path, "-o", temp_ass_path, "-i", xml_abs_path, "-O", "255"],
        capture_output=True
    )
    print("stdout:")
    print(res.stdout.decode("utf-8"))
    print("stderr:")
    print(res.stderr.decode("utf-8"))
    res.check_returncode()
    # check if ass file has been generated
    if not os.path.isfile(temp_ass_path):
        raise AirflowException("DanmakuFactory command returns 0 but cannot find generated file")
    ti.xcom_push("ass_path", temp_ass_path)


def ensure_ffmpeg(**kwargs):
    ti = kwargs["ti"]
    # check if ffmpeg has been installed
    res = subprocess.run(["ffmpeg", "-version"])
    if res.returncode == 0:
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
    # ffmpeg -hwaccel vaapi -hwaccel_device /dev/dri/renderD128 -hwaccel_output_format vaapi -i 20231121_七海Nana7mi_时尚造梦_210150.flv -vf 'scale_vaapi,hwmap=mode=read+write+direct,format=nv12,ass=test.ass,hwmap' -c:v h264_vaapi output.mp4
    res = subprocess.run(
        [ffmpeg_path, "-hwaccel", "vaapi", "-hwaccel_device", "/dev/dri/renderD128", "-hwaccel_output_format", "vaapi",
         "-i", abs_path, "-vf", f"scale_vaapi,hwmap=mode=read+write+direct,format=nv12,ass={ass_file},hwmap", "-c:v",
         "hevc_vaapi", output_path], capture_output=True)
    print(res.args)
    print("stdout:")
    print(res.stdout.decode("utf-8"))
    print("stderr:")
    print(res.stderr.decode("utf-8"))
    res.check_returncode()
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


def rush_c_upload(**kwargs):
    ti = kwargs["ti"]
    is_danmaku = kwargs["is_danmaku"]
    if is_danmaku:
        file = ti.xcom_pull("danmaku_path", "generate_danmaku_version.generate_danmaku_record")
        upload_type = "danmaku"
    else:
        file = ti.xcom_pull("abs_path", "setup")
        upload_type = "raw"
    cookie_path = Variable.get("cookie_path")
    file_obj_id = ti.xcom_pull("file_obj_id", "setup")
    rush_c_upload_path = ti.xcom_pull("rush_c_upload_path", "upload_to_bilibili.ensure_rush_c")
    execution_summary_path = get_temp_file_abs_path(ti, f"upload_{file}.json")

    result = subprocess.run(
        [rush_c_upload_path, "--cookie", cookie_path, "--execution_summary_path", execution_summary_path, file],
        capture_output=True)
    print(result.args)
    print("stdout")
    print(result.stdout.decode("utf-8"))
    print("stderr")
    print(result.stderr.decode("utf-8"))
    result.check_returncode()
    with (open(execution_summary_path) as summary_json_file):
        summary = json.load(summary_json_file)
        mongoengine_connect()
        bilibili_upload_result = BilibiliUploadResult.objects.get(RecorderEventId=ObjectId(file_obj_id),
                                                                  Type=upload_type)
        if bilibili_upload_result is None:
            bilibili_upload_result = BilibiliUploadResult(RecorderEventId=ObjectId(file_obj_id), Type=upload_type)

        bilibili_upload_result.Title = summary["Title"]
        bilibili_upload_result.Filename = summary["Filename"]
        bilibili_upload_result.Desc = summary["Desc"]


@task_group(
    group_id="upload_to_bilibili",
    dag=on_new_record
)
def upload_to_bilibili_group(is_danmaku):
    ensure_rush_c_task = PythonOperator(
        task_id="ensure_rush_c",
        python_callable=ensure_rush_c,
        dag=on_new_record
    )
    rush_c_upload_task = PythonOperator(
        task_id="rush_c_upload",
        python_callable=rush_c_upload,
        dag=on_new_record,
        op_kwargs={"is_danmaku": is_danmaku}
    )


setup_task.as_setup() >> [upload_webdav_task, generate_danmaku_version_group()] >> teardown_task.as_teardown()
