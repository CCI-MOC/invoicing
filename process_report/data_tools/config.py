from pydantic_settings import BaseSettings


class DataToolsSettings(BaseSettings):
    """Iceberg warehouse path and S3 credentials for data_tools queries."""

    iceberg_warehouse_base: str = "s3://nerc-invoicing-iceberg/warehouse"
    iceberg_table_subpath: str = "nerc_invoicing_iceberg/nerc_invoicing_iceberg"
    iceberg_s3_access_key_id: str | None = None
    iceberg_s3_secret_access_key: str | None = None
    iceberg_s3_endpoint: str | None = None
    iceberg_s3_region: str = "us-east-005"

    @property
    def table_path(self) -> str:
        return f"{self.iceberg_warehouse_base}/{self.iceberg_table_subpath}"

    def iceberg_s3_properties(self) -> dict[str, str]:
        if not all(
            [
                self.iceberg_s3_access_key_id,
                self.iceberg_s3_secret_access_key,
                self.iceberg_s3_endpoint,
            ]
        ):
            raise ValueError(
                "Iceberg S3 credentials required: "
                "ICEBERG_S3_ACCESS_KEY_ID, ICEBERG_S3_SECRET_ACCESS_KEY, ICEBERG_S3_ENDPOINT"
            )
        return {
            "s3.access-key-id": self.iceberg_s3_access_key_id,
            "s3.secret-access-key": self.iceberg_s3_secret_access_key,
            "s3.endpoint": self.iceberg_s3_endpoint,
            "s3.region": self.iceberg_s3_region,
        }


data_tools_settings = DataToolsSettings()
