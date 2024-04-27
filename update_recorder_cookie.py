import requests
import json

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow import AirflowException
from common_tasks import ensure_biliup
from file import prepare_temp_storage, get_temp_file_abs_path
from command import run_command


@task()
def setup(**context):
    ti = context["ti"]
    prepare_temp_storage(ti)


@task()
def get_recorder_cookie(**context):
    ti = context["ti"]
    api_url = Variable.get("recorder_api_url")
    get_global_config_url = f"{api_url}/api/config/global"
    res = requests.get(get_global_config_url).json()
    print(res)
    existing_cookie = None
    if "optionalCookie" in res and res["optionalCookie"]["hasValue"] is True:
        existing_cookie = res["optionalCookie"]["value"]
    else:
        raise AirflowException("optionalCookie field is missing in the returned config")

    existing_cookie_temp_path = get_temp_file_abs_path(ti, "existing_cookie")
    with open(existing_cookie_temp_path, "w") as existing_cookie_temp_file:
        json.dump(existing_cookie, existing_cookie_temp_file)


@task()
def renew_cookie(**context):
    ti = context["ti"]
    biliup_path = ti.xcom_pull(key="biliup_path", task_ids="ensure_biliup")
    resources_root = Variable.get("resources_root")
    cookie_path = f"{resources_root}/r46mht.json"
    exit_code = run_command([biliup_path, "--user-cookie", cookie_path, "renew"])
    if exit_code != 0:
        raise AirflowException("Failed to renew cookie using biliup cli")


@task()
def update_recorder_config(**context):
    ti = context["ti"]
    resources_root = Variable.get("resources_root")
    cookie_path = f"{resources_root}/r46mht.json"
    cookie_data = None
    with open(cookie_path, "r") as cookie_file:
        cookie_data = json.load(cookie_file)

    if cookie_data is None:
        raise AirflowException(f"Failed to read cookie data from {cookie_path}")
    print(cookie_data)

    cookie_str = ""
    if "cookie_info" not in cookie_data or "cookies" not in cookie_data["cookie_info"]:
        raise AirflowException(f"Cannot find cookie list in {cookie_path}")
    cookie_list = cookie_data["cookie_info"]["cookies"]
    for cookie_entry in cookie_list:
        if "name" not in cookie_entry or "value" not in cookie_entry:
            raise AirflowException("'name' or 'value' key not found in cooke object")
        cookie_str += f"{cookie_entry['name']}={cookie_entry['value']};"

    if cookie_str[-1] == ";":
        cookie_str = cookie_str[:-1]
    print(cookie_str)

    upload_payload = {"optionalCookie": {"hasValue": True, "value": cookie_str}}
    api_url = Variable.get("recorder_api_url")
    set_global_config_url = f"{api_url}/api/config/global"

    res = requests.post(
        set_global_config_url,
        data=json.dumps(upload_payload),
        headers={"Content-Type": "application/json", "accept": "text/json"})
    if not res.ok:
        raise AirflowException(f"set global config request failed with code: {res.status_code}")


@dag(
    schedule_interval="@weekly",
    start_date=datetime(year=2023, month=11, day=16),
    catchup=False,
    params={})
def update_recorder_cookie():
    setup() >> ensure_biliup() >> renew_cookie() >> update_recorder_config()


update_recorder_cookie()