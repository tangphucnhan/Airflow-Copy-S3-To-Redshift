import os

CFG_CSV = "csv"
CFG_S3 = "s3"
CFG_MYSQL = "mysql"
CFG_POSTGRESQL = "postgresql"
CFG_REDSHIFT = "redshift"

cfg_lakehouse = {
    "csv": {
        "csv_path": os.getenv("CSV_PATH", "")
    },
    "s3": {
        "aws_region_name": os.getenv("AWS_REGION_NAME", ""),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "aws_endpoint_url": os.getenv("AWS_ENDPOINT_URL", ""),
        "aws_iam_role": os.getenv("IAM_ROLE", ""),
        "s3_bucket": os.getenv("S3_BUCKET", ""),
        "s3_prefix": os.getenv("S3_PREFIX", ""),
    },
    "postgresql": {
        "db_user": os.getenv("PG_DB_USER", ""),
        "db_pwd": os.getenv("PG_DB_PWD", ""),
        "db_host": os.getenv("PG_DB_HOST", ""),
        "db_port": os.getenv("PG_DB_PORT", ""),
        "db_name": os.getenv("PG_DB_NAME", ""),
    },
}

cfg_warehouse = {
    "postgresql": {
        "db_user": os.getenv("WH_PG_DB_USER", ""),
        "db_pwd": os.getenv("WH_PG_DB_PWD", ""),
        "db_host": os.getenv("WH_PG_DB_HOST", ""),
        "db_port": os.getenv("WH_PG_DB_PORT", ""),
        "db_name": os.getenv("WH_PG_DB_NAME", ""),
    },
    "redshift": {
        "db_region": os.getenv("RED_DB_REGION", ""),
        "db_access_key": os.getenv("RED_AWS_ACCESS_KEY", ""),
        "db_secret_key": os.getenv("RED_AWS_SECRET_KEY", ""),
        "db_conn_id": os.getenv("RED_CONN_ID", ""),
        "db_user": os.getenv("RED_DB_USER", ""),
        "db_pwd": os.getenv("RED_DB_PWD", ""),
        "db_host": os.getenv("RED_DB_HOST", ""),
        "db_port": os.getenv("RED_DB_PORT", ""),
        "db_name": os.getenv("RED_DB_NAME", ""),
        "db_workgroup": os.getenv("RED_DB_WORKGROUP", ""),
    },
}
