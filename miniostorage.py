import os
from minio import Minio

# contains the helpers for minio upload


endpoint = os.getenv("MINIO_ENDPOINT")
bucket = "pdf-storage"

client = Minio(
    endpoint,
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
)

# this is just checking to ensure the bucket exists for persistent storage
def init_minio_bucket():
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

# this is uploading as a stream so we don't need to 
def upload_pdf(file, fname):
    file.stream.seek(0, os.SEEK_END)
    filesize = file.stream.tell()
    file.stream.seek(0)

    client.put_object(
        bucket,
        fname,
        file.stream,
        filesize,
        content_type=file.content_type
    )

# gets pdf data (this is specifically for vector embedding)
def get_pdf(fname, fpath):
    client.fget_object(
        bucket,
        fname,
        fpath
    )

# remove a pdf from the bucket
def delete_pdf(fname):
    client.remove_object(
        bucket,
        fname
    )