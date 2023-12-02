from datetime import datetime

from airflow.decorators import dag
from airflow.models import Param

from common_tasks import setup, teardown, create_upload_archive_to_webdav_task


@dag(
    schedule_interval=None,
    start_date=datetime(year=2023, month=11, day=16),
    catchup=False,
    params={
        "archive_id": Param(None)
    })
def upload_archive_to_webdav():
    upload_archive_to_webdav_task = create_upload_archive_to_webdav_task("archive_id", "setup")

    setup().as_setup() >> upload_archive_to_webdav_task >> teardown().as_teardown()


upload_archive_to_webdav()