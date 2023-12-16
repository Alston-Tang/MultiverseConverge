import json
import os
import time
import uuid
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow.decorators import dag
from airflow import DAG, AirflowException
from airflow.decorators import task_group
from airflow.models import Param, Variable
from airflow.operators.python import PythonOperator

from bson import ObjectId

from common_tasks import setup, teardown, validate_record_info, ensure_rush_c
from lock import try_lock_archive, unlock_archive
from util import parse_title, check_binary_exists
from file import get_temp_file_abs_path
from data import BilibiliUploadResult, BilibiliArchiveInfo, mongoengine_connect
from command import run_command


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
    command = [ffmpeg_path, "-hwaccel", "vaapi", "-hwaccel_device", "/dev/dri/renderD128", "-hwaccel_output_format",
               "vaapi",
               "-i", abs_path, "-vf", f"scale_vaapi,hwmap=mode=read+write+direct,format=nv12,ass={ass_file},hwmap",
               "-c:v",
               "hevc_vaapi", output_path]
    return_code = run_command(command)
    if return_code != 0:
        raise AirflowException(f"{command} returns {return_code}")
    ti.xcom_push("danmaku_path", output_path)


@task_group(
    group_id="generate_danmaku_version",
)
def generate_danmaku_version_group():
    ensure_danmaku_factory_task = PythonOperator(
        task_id="ensure_danmaku_factory",
        python_callable=ensure_danmaku_factory,
    )
    convert_danmaku_to_ass_task = PythonOperator(
        task_id="convert_danmaku_to_ass",
        python_callable=convert_danmaku_to_ass,
    )
    ensure_ffmpeg_task = PythonOperator(
        task_id="ensure_ffmpeg",
        python_callable=ensure_ffmpeg,
    )
    generate_danmaku_record_task = PythonOperator(
        task_id="generate_danmaku_record",
        python_callable=generate_danmaku_record,
    )
    ensure_danmaku_factory_task >> convert_danmaku_to_ass_task
    ensure_ffmpeg_task >> generate_danmaku_record_task
    convert_danmaku_to_ass_task >> generate_danmaku_record_task


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
    rush_c_upload_path = ti.xcom_pull(key="rush_c_upload_path",
                                      task_ids=f"ensure_rush_c")
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


def construct_p_title(title, video_type):
    title = title.removesuffix("_danmaku")
    if video_type == "danmaku":
        return f"弹幕_{title}"
    return title


def construct_archive_title(archive_info):
    title_date_str = None
    prev_live_title = None
    archive_title = ""
    for video in archive_info["Videos"]:
        # ["raw", "danmaku"]
        video_type = video["Type"]
        if video_type != 'raw':
            continue
        # 20231125_七海Nana7mi_嗨暗之魂3_220124
        title = video["Title"]
        # [20231125, 七海Nana7mi, 嗨暗之魂3, 220124]
        title_split = parse_title(title)
        # use the earliest date str as the prefix of archive title
        if title_date_str is None:
            title_date_str = title_split["date_str"]
        live_title = title_split["live_title"]
        if live_title != prev_live_title:
            archive_title = archive_title + live_title + '/'
            prev_live_title = live_title

    if archive_title[-1] == '/':
        archive_title = archive_title[:-1]
    archive_title = f"【直播录像】{title_date_str}-" + archive_title
    return archive_title


def construct_files_list(archive_info):
    files = []
    for video in archive_info["Videos"]:
        # n231126qn3lignju7nynr611zk23om0f
        filename = video["Filename"]
        # ["raw", "danmaku"]
        video_type = video["Type"]
        # 20231125_七海Nana7mi_嗨暗之魂3_220124
        title = video["Title"]

        p_title = construct_p_title(title, video_type)
        files.append(f"{filename}:{p_title}")

    return ','.join(files)


def construct_submit_command(archive_info, execution_summary_path, rush_c_submit_path, bvid):
    cookie_path = Variable.get("cookie_path")
    cover_path = Variable.get("cover_path")

    files_list = construct_files_list(archive_info)
    archive_title = construct_archive_title(archive_info)

    command = [rush_c_submit_path,
               "--cookie", cookie_path,
               "--cover", cover_path,
               "--execution-summary-path", execution_summary_path,
               "--title", archive_title]
    if bvid is not None:
        command.append("--vid")
        command.append(bvid)
    command.append(files_list)
    return command


def video_sort_key(video):
    title = video["Title"]
    title_split = parse_title(title)
    return video["Type"], title_split['date_str'], title_split['time_str']


def rush_c_submit(**kwargs):
    ti = kwargs["ti"]
    mongoengine_connect()
    upload_type = kwargs["upload_type"]
    archive_id = ti.xcom_pull(key="archive_id", task_ids="setup")
    dry_run = ti.xcom_pull(key="dry_run", task_ids="setup")
    if dry_run:
        print("rush c submit will not actually submit video to bilibili because dry_run == True")
    rush_c_submit_path = ti.xcom_pull(key="rush_c_submit_path",
                                      task_ids=f"ensure_rush_c")
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
    if not dry_run:
        return_code = run_command(command)
    else:
        print(command)
        print("in dry run mode. exit without executing command")
        return

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
        group_id=f"upload_to_bilibili_{upload_type}"
    )
    def upload_to_bilibili_group(upload_type_):
        rush_c_upload_task = PythonOperator(
            task_id="rush_c_upload",
            python_callable=rush_c_upload,
            op_kwargs={"upload_type": upload_type_},
            retries=3
        )
        lock_archive_task = PythonOperator(
            task_id="lock_archive",
            python_callable=lock_archive,
            execution_timeout=timedelta(minutes=10),
        )
        unlock_archive_task = PythonOperator(
            task_id="unlock_archive",
            python_callable=teardown_unlock_archive,
            op_kwargs={"upload_type": upload_type_}
        )
        rush_c_submit_task = PythonOperator(
            task_id="rush_c_submit",
            python_callable=rush_c_submit,
            op_kwargs={"upload_type": upload_type_},
            retries=3,
        )
        rush_c_upload_task >> lock_archive_task >> rush_c_submit_task >> unlock_archive_task.as_teardown(
            setups=lock_archive_task)

    return upload_to_bilibili_group(upload_type)


@dag(
    schedule_interval=None,
    start_date=datetime(year=2023, month=11, day=16),
    catchup=False,
    params={
        "path": Param(None),
        "file_obj_id": Param(None),
        "archive_id": Param(None),
        "dry_run": Param(default=False)
    })
def on_new_record():
    setup_task = setup().as_setup()
    ensure_rush_c_task = ensure_rush_c()
    validate_record_info_task = validate_record_info()
    generate_danmaku_version_group_tasks = generate_danmaku_version_group()
    bilibili_upload_group_raw_task = create_bilibili_upload_group("raw")
    bilibili_upload_group_danmaku_task = create_bilibili_upload_group("danmaku")
    teardown_task = teardown().as_teardown()

    setup_task >> validate_record_info_task
    setup_task >> ensure_rush_c_task
    validate_record_info_task >> generate_danmaku_version_group_tasks
    validate_record_info_task >> bilibili_upload_group_raw_task
    ensure_rush_c_task >> bilibili_upload_group_raw_task
    generate_danmaku_version_group_tasks >> bilibili_upload_group_danmaku_task
    ensure_rush_c_task >> bilibili_upload_group_danmaku_task
    bilibili_upload_group_raw_task >> teardown_task
    bilibili_upload_group_danmaku_task >> teardown_task


on_new_record()
