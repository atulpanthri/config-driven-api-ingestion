"""Extraction layer: one interface for every way of getting raw data (API, file, database).

Each source declares `extraction.type` in its YAML. The rest of the pipeline (landing files,
bronze, contract check, DQ, silver/gold, audit) only sees `RawUnit`s, so adding a new extraction
type means adding one Extractor class here and registering it.

Implemented:  api   (api_client: templated requests, auth from secrets, retries, pagination)
Planned:      file  (e.g. JSON/CSV files in a UC Volume path per entity)
              db    (e.g. a JDBC query per entity)
Planned types are registered so configs fail with a clear message instead of a crash; config
validation (source_config.validate_source) already rejects them before any run starts.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Protocol

from .api_client import SecretResolver, fetch_entity
from .source_config import Entity, SourceConfig


@dataclass
class RawUnit:
    """One raw piece of source data as received: an API response page, a file, or a query result."""
    entity_id: str
    page: int                              # 1-based position within the entity's extraction
    ok: bool
    source_uri: str                        # request URL (secrets masked), file path, or table/query id
    request_params: dict[str, Any]         # secrets masked
    request_start: datetime
    request_end: datetime | None
    status_code: int | None                # HTTP status for api; None for file/db
    attempts: int
    body_text: str | None                  # None when nothing was received (timeout, connection error)
    error_type: str | None = None
    error_message: str | None = None


class Extractor(Protocol):
    extraction_type: str

    def extract(self, entity: Entity, window: dict[str, Any], *, params_override: dict[str, Any] | None = None,
                base_url_override: str | None = None, secret_resolver: SecretResolver | None = None,
                sleep: Callable[[float], None] | None = None) -> tuple[list[RawUnit], str | None]:
        """Return (raw units, extraction-level error such as a pagination limit, or None)."""
        ...


class ApiExtractor:
    extraction_type = "api"

    def __init__(self, cfg: SourceConfig, session: Any):
        self.cfg, self.session = cfg, session

    def extract(self, entity, window, *, params_override=None, base_url_override=None, secret_resolver=None,
                sleep=None):
        kwargs: dict[str, Any] = {"secret_resolver": secret_resolver, "params_override": params_override,
                                  "base_url_override": base_url_override}
        if sleep is not None:
            kwargs["sleep"] = sleep
        pages, error = fetch_entity(self.session, self.cfg, entity, window, **kwargs)
        return [RawUnit(entity_id=p.entity_id, page=p.page, ok=p.ok, source_uri=p.request_url,
                        request_params=p.request_params, request_start=p.request_start, request_end=p.request_end,
                        status_code=p.http_status, attempts=p.attempts, body_text=p.body_text,
                        error_type=p.error_type, error_message=p.error_message) for p in pages], error


class _PlannedExtractor:
    extraction_type = "planned"

    def __init__(self, cfg: SourceConfig, session: Any = None):
        self.cfg = cfg

    def extract(self, entity, window, **kwargs):
        raise NotImplementedError(f"extraction.type {self.extraction_type!r} is not implemented yet "
                                  f"(source {self.cfg.source_name})")


class FileExtractor(_PlannedExtractor):
    """Planned: read the files configured for an entity (e.g. /Volumes/.../<entity>/*.json) as RawUnits."""
    extraction_type = "file"


class DbExtractor(_PlannedExtractor):
    """Planned: run the configured query per entity over JDBC and return the result as RawUnits."""
    extraction_type = "db"


EXTRACTORS: dict[str, type] = {"api": ApiExtractor, "file": FileExtractor, "db": DbExtractor}


def get_extractor(cfg: SourceConfig, session: Any = None) -> Extractor:
    try:
        return EXTRACTORS[cfg.extraction_type](cfg, session)
    except KeyError:
        raise ValueError(f"unknown extraction.type {cfg.extraction_type!r}; known: {sorted(EXTRACTORS)}") from None
