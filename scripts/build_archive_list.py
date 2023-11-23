# build bilibili archive list
# 1. get FileClosed events from db
# 2. get existing archives from db
# 2. group file into archive using below method:
#   2.1 sort all FileClosed events by FileCloseTime
#   2.2 iterate from beginning of the sorted FileClosed events and try to merge to an existing archive
#       2.2.1 modify existing archive if FileClosed events not there, identified by path
#   2.3 create a new archive and save to db if archive not exist


import os
from datetime import timedelta

from mongoengine import connect
from data import FileClosedEventDoc, BilibiliArchive, BilibiliVideo


def get_file_closed_events() -> list[FileClosedEventDoc]:
    res: list[FileClosedEventDoc] = []
    query_result = FileClosedEventDoc.objects(EventType="FileClosed")
    for event in query_result:
        res.append(event)
    return res


def get_existing_archives(host_id: str) -> list[BilibiliArchive]:
    res: list[BilibiliArchive] = []
    query_result = BilibiliArchive.objects(HostId=host_id)
    for archive in query_result:
        res.append(archive)
    return res


def construct_archives(host_id: str, files: list[FileClosedEventDoc]) -> list[BilibiliArchive]:
    res: list[BilibiliArchive] = []
    for file in files:
        # extend latest archive or create a new archive?
        if len(res) <= 0 or res[len(res) - 1]["ArchiveEndTime"] + timedelta(hours=4) < file["EventData"]["FileOpenTime"]:
            res.append(BilibiliArchive(HostId=host_id))

        video_obj: BilibiliVideo = BilibiliVideo()
        video_obj.RelativePath = file["EventData"]["RelativePath"]
        video_obj.FileCloseTime = file["EventData"]["FileCloseTime"]
        video_obj.FileOpenTime = file["EventData"]["FileOpenTime"]
        video_obj.Duration = file["EventData"]["Duration"]
        video_obj.RecorderEventId = file.id

        res[-1].Videos.append(video_obj)
        if len(res[-1]["Videos"]) == 1:
            res[-1].ArchiveStartTime = file["EventData"]["FileOpenTime"]
        res[-1].ArchiveEndTime = file["EventData"]["FileCloseTime"]
        res[-1].ArchiveDuration = res[-1]["ArchiveDuration"] + file["EventData"]["Duration"]

    return res


def same_archive(a: BilibiliArchive, b: BilibiliArchive) -> bool:
    if a["ArchiveStartTime"] != b["ArchiveStartTime"]:
        return False
    if a["ArchiveEndTime"] != b["ArchiveEndTime"]:
        return False
    if a["ArchiveDuration"] != b["ArchiveDuration"]:
        return False
    if a["HostId"] != b["HostId"]:
        return False
    if len(a["Videos"]) != len(b["Videos"]):
        return False
    return True


def update_archives(archives: list[BilibiliArchive], ex_archives: list[BilibiliArchive]):
    ar_idx = 0
    ex_idx = 0
    while True:
        if ar_idx >= len(archives):
            break
        should_save: bool = False
        if ex_idx < len(ex_archives):
            if not same_archive(archives[ar_idx], ex_archives[ex_idx]):
                if ex_idx != len(ex_archives) - 1:
                    raise Exception("Archive get modified. This is unexpected because only newer FileClosedEvent "
                                    "should be added to db.")
                # last existing archive
                # there might be new video file appending to this archive
                should_save = True
                archives[ar_idx].id = ex_archives[ex_idx].id
            ex_idx = ex_idx + 1
        else:
            # new archive
            should_save = True
        if should_save:
            archives[ar_idx].save()
        ar_idx = ar_idx + 1


if __name__ == '__main__':
    # mongodb uri
    # e.g. mongodb+srv://mongodb:<password>@<host>/?retryWrites=true&w=majority
    db_uri: str = os.getenv("DB_URI")
    env_host_id: str = os.getenv("HOST_ID")

    connect(host=db_uri)

    file_closed_events: list[FileClosedEventDoc] = get_file_closed_events()
    existing_archives: list[BilibiliArchive] = get_existing_archives(env_host_id)
    existing_archives.sort(key=lambda x: x["ArchiveEndTime"])

    # sort on host
    # shouldn't have a lot of events anyway
    file_closed_events.sort(key=lambda x: x["EventData"]["FileCloseTime"])
    # archives.sort(key=lambda x: x["ArchiveEndTime"])

    update_archives(construct_archives(env_host_id, file_closed_events), existing_archives)
