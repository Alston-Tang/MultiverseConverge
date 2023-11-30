from datetime import datetime
from os import path
from typing import Optional, List

from mongoengine import Document, StringField, DateTimeField, EmbeddedDocumentField, EmbeddedDocument, LongField, \
    FloatField, IntField, BooleanField, EmbeddedDocumentListField, ReferenceField, ObjectIdField, DictField, MapField, \
    ListField, connect
from bson import ObjectId


class BililiveFile:
    path: path
    file_id: ObjectId
    recorder_event_id: ObjectId
    # estimated start/end time from recorder event
    # may not be correct and does not actually reflect the time in the video
    start_time: datetime
    end_time: datetime


class Archive:
    files: List[BililiveFile]
    title: str
    cover_path: path
    # 转载来源
    source: str
    # 投稿分区id
    # 参考B站定义: https://www.bilibili.com/read/cv18327205/
    category_id: int
    tags: List[str]
    description: str
    # B站投稿id
    bv_id: Optional[str] = None


# MongoEngine definition
class FileClosedDataDoc(EmbeddedDocument):
    SessionId = StringField()
    RelativePath = StringField()
    FileSize = LongField()
    Duration = FloatField()
    FileOpenTime = DateTimeField()
    FileCloseTime = DateTimeField()
    RoomId = IntField()
    ShortId = IntField()
    Name = StringField()
    Title = StringField()
    AreaNameParent = StringField()
    AreaNameChild = StringField()
    Recording = BooleanField()
    Streaming = BooleanField()
    DanmakuConnected = BooleanField()


class FileClosedEventDoc(Document):
    EventType = StringField()
    EventTimestamp = DateTimeField()
    EventId = StringField()
    EventData = EmbeddedDocumentField(FileClosedDataDoc)
    Host = StringField()

    meta = {'collection': 'records'}


class BilibiliVideo(EmbeddedDocument):
    RelativePath = StringField()
    FileCloseTime = DateTimeField()
    FileOpenTime = DateTimeField()
    Duration = FloatField()
    RecorderEventId = ReferenceField(FileClosedEventDoc, required=False)


class BilibiliArchive(Document):
    Videos = EmbeddedDocumentListField(BilibiliVideo)
    ArchiveStartTime = DateTimeField()
    ArchiveEndTime = DateTimeField()
    ArchiveDuration = FloatField(default=0.0)
    HostId = StringField(required=True)

    meta = {'collection': 'archive', 'indexes': ['-ArchiveEndTime']}


class AirflowArchiveLock(Document):
    DocId = ObjectIdField(required=True)
    LockId = ObjectIdField(required=False, default=None)

    meta = {"db_alias": "airflow", "indexes": [{'fields': ['DocId'], 'unique': True}]}


class AirflowVideoLock(Document):
    DocId = ObjectIdField(require=True)
    LockId = ObjectIdField(required=False, default=None)

    meta = {"db_alias": "airflow", "indexes": [{'fields': ['DocId'], 'unique': True}]}


class BilibiliUploadResult(Document):
    RecorderEventId = ObjectIdField(required=True)
    Type = StringField(required=True)
    Title = StringField()
    Filename = StringField(required=True)
    Desc = StringField()

    meta = {"db_alias": "airflow"}


class BilibiliArchiveInfo(Document):
    ArchiveId = ObjectIdField(required=True)
    Bvid = StringField()
    Aid = LongField()
    Videos = ListField(ReferenceField(BilibiliUploadResult))

    meta = {"db_alias": "airflow", "indexes": [{"fields": ["ArchiveId"], 'unique': True}]}


if __name__ == "__main__":
    connect(host="mongodb+srv://mongodb:TYmbYH4yQ1sDb5xJ@773.4oignmq.mongodb.net", alias='airflow', db='airflow')
    video = BilibiliUploadResult.objects.get(RecordEventId=ObjectId("65620c486495fab9c0d81a1c"), Type="raw")
    BilibiliArchiveInfo(
        ArchiveId=ObjectId("65620c486495fab9c0d81a1e"),
        Bvid="BV1E34y1F7Cq",
        Aid=0,
        Videos=[video]
    ).save()
