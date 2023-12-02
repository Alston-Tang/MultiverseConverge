from datetime import datetime
from os import path
from typing import Optional, List

from airflow.models import Variable
from mongoengine import Document, StringField, DateTimeField, EmbeddedDocumentField, EmbeddedDocument, LongField, \
    FloatField, IntField, BooleanField, EmbeddedDocumentListField, ReferenceField, ObjectIdField, DictField, MapField, \
    ListField, connect
from bson import ObjectId


def mongoengine_connect():
    mongodb_uri = Variable.get(key="mongodb_uri")
    print(f"connect to mongodb {mongodb_uri}")
    connect(host=mongodb_uri, alias='airflow', db='airflow')
    connect(host=mongodb_uri, alias='bililive', db='bililive')


class BililiveFile:
    path: path
    file_id: ObjectId
    recorder_event_id: ObjectId
    # estimated start/end time from recorder event
    # may not be correct and does not actually reflect the time in the video
    start_time: datetime
    end_time: datetime


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

    meta = {'collection': 'records', 'db_alias': 'bililive'}


class ArchiveVideo(EmbeddedDocument):
    RelativePath = StringField()
    FileCloseTime = DateTimeField()
    FileOpenTime = DateTimeField()
    Duration = FloatField()
    RecorderEventId = ObjectIdField()


class Archive(Document):
    ArchiveStartTime = DateTimeField()
    ArchiveEndTime = DateTimeField()
    ArchiveDuration = FloatField()
    HostId = StringField()
    Videos = ListField(EmbeddedDocumentField(ArchiveVideo))

    meta = {'collection': 'archive', 'db_alias': 'bililive'}


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
