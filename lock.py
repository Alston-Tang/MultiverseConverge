from typing import Optional, Any

from mongoengine import NotUniqueError

from data import AirflowVideoLock, AirflowArchiveLock
from bson import ObjectId


def try_lock(lock_doc: Any, doc_id: ObjectId) -> Optional[ObjectId]:
    new_lock_id: ObjectId = ObjectId()
    try:
        if lock_doc.objects(DocId=doc_id, LockId=None).update_one(set__LockId=new_lock_id, upsert=True) > 0:
            return new_lock_id
    except NotUniqueError:
        pass
    return None


def unlock(lock_doc: Any, doc_id: ObjectId, lock_id: ObjectId) -> None:
    try:
        lock_doc.objects(DocId=doc_id, LockId=lock_id).update_one(set__LockId=None, upsert=True)
    except NotUniqueError:
        pass


def try_lock_video(video_id: ObjectId) -> Optional[ObjectId]:
    return try_lock(AirflowVideoLock, video_id)


def unlock_video(video_id: ObjectId, lock_id: ObjectId) -> None:
    return unlock(AirflowVideoLock, video_id, lock_id)


def try_lock_archive(archive_id: ObjectId) -> Optional[ObjectId]:
    return try_lock(AirflowArchiveLock, archive_id)


def unlock_archive(archive_id: ObjectId, lock_id: ObjectId) -> None:
    return unlock(AirflowArchiveLock, archive_id, lock_id)


if __name__ == '__main__':
    import os
    from mongoengine import connect
    # mongodb uri
    # e.g. mongodb+srv://mongodb:<password>@<host>/?retryWrites=true&w=majority
    db_uri: str = os.getenv("DB_URI")

    connect(host=db_uri, alias='airflow', db='airflow')

    try_lock_video(ObjectId("654926679ccb2d8cd29f903d"))