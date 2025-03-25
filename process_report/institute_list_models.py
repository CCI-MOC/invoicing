from typing import Annotated
import datetime

import pydantic
import validators


def parse_date(v: str) -> str:
    try:
        datetime.datetime.strptime(v, "%Y-%m")
        return v
    except ValueError:
        raise ValueError(f"Invalid date string {v}. Must be in format YYYY-MM")


def validate_domain(v: str) -> str:
    if not validators.domain(
        v, consider_tld=True
    ):  # Ensures only TLDs allowed by IANA are valid
        raise ValueError(f"Invalid domain {v}")
    return v


DateField = Annotated[str, pydantic.BeforeValidator(parse_date)]
DomainField = Annotated[str, pydantic.AfterValidator(validate_domain)]


class InstituteInfo(pydantic.BaseModel):
    display_name: str
    domains: list[DomainField]
    mghpcc_partnership_start_date: DateField | None = None
    include_in_nerc_total_invoice: bool = False

    model_config = pydantic.ConfigDict(extra="forbid")


class InstituteList(pydantic.RootModel):
    root: list[InstituteInfo]

    @pydantic.model_validator(mode="after")
    def validate_no_display_name_duplicates(self):
        name_set = set()
        for institute in self.root:
            if institute.display_name in name_set:
                raise ValueError(
                    f"Duplicate institute display name found: {institute.display_name}"
                )
            name_set.add(institute.display_name)

        return self

    @pydantic.model_validator(mode="after")
    def validate_no_domain_duplicates(self):
        domain_name_set = set()
        for institute in self.root:
            for domain in institute.domains:
                if domain in domain_name_set:
                    raise ValueError(f"Duplicate domain: {domain}")
                domain_name_set.add(domain)

        return self
