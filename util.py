from typing import List
from pathlib import Path

from airflow import AirflowException

from data import ArchiveVideo


def parse_relative_path(relative_path):
    return Path(relative_path).stem


def parse_title(title):
    title_split = title.split('_')
    if len(title_split) < 4:
        raise AirflowException(f"cannot parse title {title}")
    date_str = title_split[0]
    live_title = title_split[2]
    time_str = title_split[3]
    return {"date_str": date_str, "live_title": live_title, "time_str": time_str}


def construct_title_from_videos(videos: List[ArchiveVideo]):
    title_date_str = None
    prev_live_title = None
    archive_title = ""
    for video in videos:
        relative_path = video["RelativePath"]
        title = parse_relative_path(relative_path)
        title_split = parse_title(title)
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
