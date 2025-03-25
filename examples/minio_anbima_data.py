from stpstone.utils.connections.clouds.minio import MinioClient


minio_client = MinioClient("minioadmin", "minioadmin")

file_path = "file.txt"
bucket_name = "example-bucket"
object_name = "uploaded-file.txt"

blame_s3 = minio_client.put_object_from_file(
    bucket_name=bucket_name,
    object_name=object_name,
    file_path=file_path
)

if blame_s3:
    print(f"Successfully uploaded {file_path} as {object_name}")
else:
    print(f"Failed to upload {file_path}")
