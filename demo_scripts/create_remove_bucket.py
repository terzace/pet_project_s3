from utils.s3_utils import (
    S3_CONFIGS,
    boto3_create_bucket,
    boto3_remove_bucket,
    minio_create_bucket,
    minio_remove_bucket,
)

# Для создания бакета:
bucket_name = "ivanov-test-s3-as-standard-from-api"
minio_create_bucket(conn_params=S3_CONFIGS["minio"], bucket_name=bucket_name)
minio_remove_bucket(conn_params=S3_CONFIGS["minio"], bucket_name=bucket_name)
boto3_create_bucket(conn_params=S3_CONFIGS["minio"], bucket_name=bucket_name)
boto3_remove_bucket(conn_params=S3_CONFIGS["minio"], bucket_name=bucket_name)

# minio_create_bucket(conn_params=S3_CONFIGS["selectel"], bucket_name=bucket_name)
# minio_remove_bucket(conn_params=S3_CONFIGS["selectel"], bucket_name=bucket_name)
# boto3_create_bucket(conn_params=S3_CONFIGS["selectel"], bucket_name=bucket_name)
# boto3_remove_bucket(conn_params=S3_CONFIGS["selectel"], bucket_name=bucket_name)

# minio_create_bucket(conn_params=S3_CONFIGS["vk"], bucket_name=bucket_name)
# minio_remove_bucket(conn_params=S3_CONFIGS["vk"], bucket_name=bucket_name)
# boto3_create_bucket(conn_params=S3_CONFIGS["vk"], bucket_name=bucket_name)
# boto3_remove_bucket(conn_params=S3_CONFIGS["vk"], bucket_name=bucket_name)

# minio_create_bucket(conn_params=S3_CONFIGS["aws"], bucket_name=bucket_name)
# minio_remove_bucket(conn_params=S3_CONFIGS["aws"], bucket_name=bucket_name)
# boto3_create_bucket(conn_params=S3_CONFIGS["aws"], bucket_name=bucket_name)
# boto3_remove_bucket(conn_params=S3_CONFIGS["aws"], bucket_name=bucket_name)
