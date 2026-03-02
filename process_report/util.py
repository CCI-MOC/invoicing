import os
import datetime
import yaml
import functools

import boto3

from process_report.institute_list_models import InstituteList


DEFAULT_INSTITUTE_LIST = "process_report/institute_list.yaml"


@functools.lru_cache
def get_invoice_bucket():
    try:
        s3_resource = boto3.resource(
            service_name="s3",
            endpoint_url=os.environ.get(
                "S3_ENDPOINT", "https://s3.us-east-005.backblazeb2.com"
            ),
            aws_access_key_id=os.environ["S3_KEY_ID"],
            aws_secret_access_key=os.environ["S3_APP_KEY"],
        )
    except KeyError:
        raise RuntimeError(
            "Please set the environment variables S3_KEY_ID and S3_APP_KEY"
        )
    return s3_resource.Bucket(os.environ.get("S3_BUCKET_NAME", "nerc-invoicing"))


def load_institute_list() -> InstituteList:
    with open(DEFAULT_INSTITUTE_LIST, "r") as f:
        institute_list = yaml.safe_load(f)
        return InstituteList.model_validate(institute_list)


def get_iso8601_time():
    return datetime.datetime.now().strftime("%Y%m%dT%H%M%SZ")


def get_month_diff(month_1, month_2):
    """Returns a positive integer if month_1 is ahead in time of month_2"""
    dt1 = datetime.datetime.strptime(month_1, "%Y-%m")
    dt2 = datetime.datetime.strptime(month_2, "%Y-%m")
    return (dt1.year - dt2.year) * 12 + (dt1.month - dt2.month)


def fetch_s3(s3_filepath):
    local_name = os.path.basename(s3_filepath)
    invoice_bucket = get_invoice_bucket()
    invoice_bucket.download_file(s3_filepath, local_name)
    return local_name
