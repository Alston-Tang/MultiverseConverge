import os
from typing import Optional
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from mongoengine import connect, Document, StringField, ReferenceField, DateTimeField, IntField, BooleanField, \
    EmbeddedDocumentField, EmbeddedDocument, LongField, FloatField


class FileDoc(Document):
    RelativePath = StringField()
    HostId = StringField()
    FileOpenTime = DateTimeField(required=False)
    FileCloseTime = DateTimeField(required=False)
    Duration = LongField(required=False)
    RecorderEventId = ReferenceField(FileCloseEventDoc, required=False)

    meta = {'collection': 'files'}


class ChunkScanner:
    def __init__(self, db_uri, host_id):
        self.db_uri = db_uri
        self.host_id = host_id
        self.connect_db()

    def connect_db(self) -> None:
        connect(host=self.db_uri)

    def get_files(self):
        return FileDoc.objects(HostId=self.host_id)

    db_uri: Optional[str] = None
    host_id: str


def get_duration(path: str) -> int:
    import cv2
    video = cv2.VideoCapture(path)
    if not video.isOpened():
        raise Exception("video.isOpened() == False")
    fps: float = video.get(cv2.CAP_PROP_FPS)
    frame_count: float = video.get(cv2.CAP_PROP_FRAME_COUNT)
    return int(frame_count * 1000 // fps)


def get_start_datetime(path: str) -> Optional[datetime]:
    import re
    print(path)
    match = re.search("^21452505-七海Nana7mi/录制-21452505-(\\d\\d\\d\\d)(\\d\\d)(\\d\\d)-(\\d\\d)(\\d\\d)("
                      "\\d\\d)-\\d\\d\\d.*\\.flv$",
                      path)
    if match is None or len(match.groups()) != 6:
        match = re.search(
            "^\\d+_七海Nana7mi_.*/(\\d\\d\\d\\d)(\\d\\d)(\\d\\d)_七海Nana7mi_.*_(\\d\\d)(\\d\\d)(\\d\\d)\\.flv$", path)
    if match is None or len(match.groups()) != 6:
        return None

    print(match.groups())
    # [year, month, date, hour, minute, second]
    date_time_tuple: list[int] = [-1] * 6
    for i in range(6):
        date_time_tuple[i] = int(match.groups()[i])
    print(date_time_tuple)

    return datetime(year=date_time_tuple[0], month=date_time_tuple[1], day=date_time_tuple[2], hour=date_time_tuple[3],
                    minute=date_time_tuple[4],
                    second=date_time_tuple[5], tzinfo=ZoneInfo("PRC")).astimezone(ZoneInfo("UTC"))


if __name__ == '__main__':
    db_uri: str = os.getenv("DB_URI")
    host_id: str = os.getenv("HOST_ID")

    scanner = ChunkScanner(db_uri=db_uri, host_id=host_id)
    for file in scanner.get_files():
        print(file.to_json())
        if file.RecorderEventId is None:
            abs_path: str = os.path.join("/mnt/storage/records", file.RelativePath)
            # millisecond (int) -> seconds (float)
            duration: float = get_duration(abs_path)
            start_time: Optional[datetime] = get_start_datetime(file.RelativePath)
            if start_time is None:
                print(f"cannot infer start time from file name: {file.RelativePath}")
                break
            end_time: datetime = start_time + timedelta(milliseconds=duration)

            file.FileOpenTime = start_time
            file.FileCloseTime = end_time
            file.Duration = int(duration)
            file.save()
