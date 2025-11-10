from typing import Annotated
import datetime
import functools
import logging

import pydantic
import validators


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


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
    courses_nonbillable: bool = False

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

    @functools.cached_property
    def nonbillable_course_list(self) -> list[str]:
        """List of institutions, by `display_name`, whose courses are nonbillable"""
        institute_list = []
        for institute_info in self.root:
            if institute_info.courses_nonbillable:
                institute_list.append(institute_info.display_name)
        return institute_list

    @functools.cached_property
    def domain_institute_mapping(self) -> dict[str, str]:
        """Dict mapping web domains to institution display names"""
        institute_map = dict()
        for institute_info in self.root:
            for domain in institute_info.domains:
                institute_map[domain] = institute_info.display_name

        return institute_map

    def get_institution_from_pi(self, pi_email) -> str:
        institution_domain = pi_email.split("@")[-1]
        for i in range(institution_domain.count(".") + 1):
            if institution_name := self.domain_institute_mapping.get(
                institution_domain, ""
            ):
                break
            institution_domain = institution_domain[institution_domain.find(".") + 1 :]

        if institution_name == "":
            logger.warning(f"PI name {pi_email} does not match any institution!")

        return institution_name
