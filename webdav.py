import os

from webdav3.client import Client
from airflow.models import Variable


def get_client():
    url = Variable.get("alist_url")
    username = Variable.get("alist_username")
    password = Variable.get("alist_password")
    options = {
        'webdav_hostname': url,
        'webdav_login': username,
        'webdav_password': password
    }
    print("constructing webdav client")
    print(options)
    return Client(options)


def webdav_recursively_create_directory(client, path):
    directory = ""
    for t in os.path.dirname(os.path.normpath(path)).split(os.path.sep):
        directory = directory + "/" + t
        if not client.check(directory):
            print(f"mkdir {directory}")
            if not client.mkdir(directory):
                return False
    return True


def upload(client, local_path, remote_path):
    if not webdav_recursively_create_directory(client, remote_path):
        print("failed to recursively create parent directory")
        return False
    client.upload_sync(remote_path=remote_path, local_path=local_path)


def download(client, local_path, remote_path):
    client.download_sync(remote_path=remote_path, local_path=local_path)


if __name__ == "__main__":
    options = {
        'webdav_hostname': "",
        'webdav_login': "",
        'webdav_password': ""
    }
    client = Client(options)
    res = webdav_recursively_create_directory(client, "")
    print(res)