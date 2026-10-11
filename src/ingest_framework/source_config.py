"""Load and validate a source definition (sources/<name>.yml) plus framework.yml.

A new API pipeline is onboarded by writing one YAML file (plus fixtures and a gold SQL
file). Everything is validated here, before any API call or table write, and every
problem is reported at once so a bad config fails fast and is easy to fix.
"""
from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TEMPLATE = re.compile(r"\{(entity|window)\.([A-Za-z_][A-Za-z0-9_]*)\}")
SILVER_TYPES = {"string", "double", "int", "bigint", "boolean", "timestamp", "date"}
CONTRACT_TYPES = {"string", "number", "integer", "boolean", "object", "array"}
PAGINATION_TYPES = {"none", "next_url", "cursor", "page_number", "offset"}
FAULT_ACTIONS = {"override_param", "set_value", "set_null", "drop_field", "add_field"}
SECRET_LIKE = re.compile(r"(?i)(api[_-]?key|token|secret|password|authorization)")
TIMESTAMP_FORMATS = {"epoch_s", "epoch_ms", "iso8601"}
CLASSIFICATIONS = {"public", "internal", "confidential", "restricted"}
LAYERS = ("raw", "silver", "gold", "audit", "quarantine")
EXTRACTION_TYPES = {"api", "file", "db"}
IMPLEMENTED_EXTRACTION_TYPES = {"api"}


class ConfigError(ValueError):
    """Raised when a source or framework config is invalid. Lists every problem found."""


@dataclass(frozen=True)
class Entity:
    id: str
    attributes: dict[str, Any]


@dataclass(frozen=True)
class EntityColumn:
    name: str
    type: str
    description: str | None = None


@dataclass(frozen=True)
class FieldContract:
    name: str                 # path under data_path, e.g. "temperature_2m" or "properties.mag"
    type: str
    required: bool
    unit_path: str | None = None
    unit: str | None = None


@dataclass(frozen=True)
class Rule:
    value: float
    reason: str


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    source: str
    type: str
    required: bool
    reason_name: str
    format: str | None = None
    min: Rule | None = None
    max: Rule | None = None
    allowed_values: tuple[Any, ...] | None = None
    on_the_hour: bool = False
    description: str | None = None


@dataclass(frozen=True)
class DerivedSpec:
    name: str
    type: str
    from_column: str
    description: str | None = None


@dataclass(frozen=True)
class RetrySpec:
    max_attempts: int
    backoff_base_seconds: float
    backoff_cap_seconds: float
    retry_after_cap_seconds: float
    retry_statuses: frozenset[int]


@dataclass(frozen=True)
class SourceConfig:
    source_name: str
    description: str
    root_dir: Path
    catalog: str
    schemas: dict[str, str]            # layer -> schema name (raw, silver, gold, audit, quarantine), suffix applied
    landing_volume: str
    extraction_type: str               # api | file | db
    bronze_table: str
    silver_table: str
    gold_table: str
    method: str
    base_url: str
    timeout_seconds: float
    params: dict[str, Any]
    headers: dict[str, Any]
    auth: dict[str, Any]
    pagination: dict[str, Any]
    retry: RetrySpec
    window_spec: dict[str, dict[str, Any]]
    entity_id_field: str
    entity_columns: tuple[EntityColumn, ...]
    entities: tuple[Entity, ...]
    min_entities: int
    response_format: str
    data_path: str
    error_flag_path: str | None
    error_message_path: str | None
    utc_offset_path: str | None
    allow_empty: bool
    contract: tuple[FieldContract, ...]
    ignore_fields: tuple[str, ...]
    silver_keys: tuple[str, ...]
    silver_partition_by: tuple[str, ...]
    numeric_strings: str
    duplicate_reason: str
    columns: tuple[ColumnSpec, ...]
    derived: tuple[DerivedSpec, ...]
    gold_sql_file: Path
    gold_keys: tuple[str, ...]
    gold_params: dict[str, Any]
    fixtures_dir: Path
    fault_default_target: str | None
    fault_scenarios: dict[str, dict[str, Any]] = field(default_factory=dict)
    data_owner: str = "unassigned"
    data_steward: str = "unassigned"
    classification: str = "internal"
    retention_policy: str = ""
    retention_days: dict[str, int] = field(default_factory=dict)   # layer -> days (raw, silver, gold)
    shared_governance: dict[str, str] = field(default_factory=dict)  # owner/steward/classification of audit tables
    gold_descriptions: dict[str, str] = field(default_factory=dict)

    def column_descriptions(self) -> dict[str, str]:
        """Silver column -> description (from entities.columns, silver.columns, silver.derived)."""
        docs = {c.name: c.description for c in self.entity_columns if c.description}
        docs.update({c.name: c.description for c in self.columns if c.description})
        docs.update({d.name: d.description for d in self.derived if d.description})
        return docs

    # ---- schemas and table names ---------------------------------------------------
    def schema(self, layer: str) -> str:
        """Schema name for a layer: raw, silver, gold, audit or quarantine."""
        return self.schemas[layer]

    def table_in(self, layer: str, name: str) -> str:
        return f"{self.catalog}.{self.schemas[layer]}.{name}"

    def audit_table(self, name: str) -> str:
        return self.table_in("audit", name)

    def quarantine_table(self, name: str) -> str:
        return self.table_in("quarantine", name)

    @property
    def bronze_fqn(self) -> str:
        return self.table_in("raw", self.bronze_table)

    @property
    def silver_fqn(self) -> str:
        return self.table_in("silver", self.silver_table)

    @property
    def gold_fqn(self) -> str:
        return self.table_in("gold", self.gold_table)

    @property
    def landing_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schemas['raw']}/{self.landing_volume}"

    def gold_sql(self) -> str:
        return self.gold_sql_file.read_text(encoding="utf-8")

    def contract_field(self, name: str) -> FieldContract | None:
        return next((c for c in self.contract if c.name == name), None)

    def entity(self, entity_id: str) -> Entity:
        for e in self.entities:
            if e.id == entity_id:
                return e
        raise ValueError(f"{entity_id!r} is not a configured entity of {self.source_name}")

    def silver_column_types(self) -> dict[str, str]:
        """All silver data columns (entity + typed + derived) -> type, in table order."""
        types = {c.name: c.type for c in self.entity_columns}
        types.update({c.name: c.type for c in self.columns})
        types.update({d.name: d.type for d in self.derived})
        return types

    def run_parameters(self, window: dict[str, Any]) -> dict[str, Any]:
        """Run-level settings recorded (per entity) in run_log_dtl.parameters."""
        return {
            "source_name": self.source_name,
            "extraction_type": self.extraction_type,
            "base_url": self.base_url,
            "window": window,
            "targets": {layer: f"{self.catalog}.{name}" for layer, name in self.schemas.items()},
            "pagination": self.pagination.get("type", "none"),
            "auth": self.auth.get("type", "none"),
            "timeout_seconds": self.timeout_seconds,
            "max_attempts": self.retry.max_attempts,
        }


# ======================================================================== loading
def load_framework(root_dir: str | Path) -> dict[str, Any]:
    path = Path(root_dir) / "framework.yml"
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    problems = [f"framework.yml: {k} must be a simple identifier"
                for k in ("catalog", "landing_volume") if not IDENTIFIER.match(str(data.get(k, "")))]
    schemas = data.get("schemas") or {}
    missing = [layer for layer in LAYERS if layer not in schemas]
    if missing:
        problems.append(f"framework.yml: schemas must define {list(LAYERS)}; missing {missing}")
    problems += [f"framework.yml: schemas.{k} must be a simple identifier"
                 for k, v in schemas.items() if not IDENTIFIER.match(str(v))]
    if len(set(schemas.values())) != len(schemas):
        problems.append("framework.yml: every layer needs its own schema")
    if problems:
        raise ConfigError("; ".join(problems))
    data.setdefault("sources_dir", "sources")
    return data


def list_sources(root_dir: str | Path) -> list[str]:
    framework = load_framework(root_dir)
    return sorted(p.stem for p in (Path(root_dir) / framework["sources_dir"]).glob("*.yml"))


def load_source(root_dir: str | Path, source_name: str, *, catalog: str | None = None,
                schema_suffix: str | None = None) -> SourceConfig:
    """Load sources/<source_name>.yml. Overrides (job parameters): catalog, and schema_suffix which is
    appended to every layer schema (e.g. '_edge' -> met_raw_edge) to isolate demo runs."""
    root = Path(root_dir)
    framework = load_framework(root)
    if not IDENTIFIER.match(source_name or ""):
        raise ConfigError(f"source name {source_name!r} must be a simple identifier")
    path = root / framework["sources_dir"] / f"{source_name}.yml"
    if not path.exists():
        raise ConfigError(f"no source config {path}; available: {list_sources(root)}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return build_source(raw, root, framework, catalog=catalog, schema_suffix=schema_suffix)


def build_source(raw: dict[str, Any], root: Path, framework: dict[str, Any], *, catalog: str | None = None,
                 schema_suffix: str | None = None) -> SourceConfig:
    """Turn a parsed YAML dict into a validated SourceConfig (also used by tests)."""
    try:
        cfg = _parse(raw, root, framework, catalog, schema_suffix or "")
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        if isinstance(exc, ConfigError):
            raise
        raise ConfigError(f"malformed source config: {exc!r}") from exc
    validate_source(cfg)
    return cfg


def _coerce_entity(item: dict[str, Any], columns: tuple[EntityColumn, ...]) -> dict[str, Any]:
    """Cast declared entity attributes to their column type (YAML reads `30` as int, not double)."""
    casts = {"double": float, "int": int, "bigint": int, "string": str}
    for c in columns:
        if c.name in item and item[c.name] is not None and c.type in casts:
            try:
                item[c.name] = casts[c.type](item[c.name])
            except (TypeError, ValueError) as exc:
                raise ConfigError(f"entity attribute {c.name}={item[c.name]!r} is not a valid {c.type}") from exc
    return item


def _rule(value: Any, default_reason: str) -> Rule | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return Rule(float(value["value"]), str(value.get("reason", default_reason)))
    return Rule(float(value), default_reason)


def _parse(raw, root, framework, catalog, schema_suffix) -> SourceConfig:
    target, response, silver, gold = raw["target"], raw["response"], raw["silver"], raw["gold"]
    extraction = raw.get("extraction") or {"type": "api"}
    request = raw.get("request") or {}           # required for extraction.type = api (validated below)
    ents = raw["entities"]
    id_field = ents["id_field"]
    entity_columns = tuple(EntityColumn(c["name"], c["type"], c.get("description")) for c in ents["columns"])
    entities = tuple(Entity(str(item[id_field]), _coerce_entity(dict(item), entity_columns))
                     for item in ents["items"])

    columns = []
    for c in silver["columns"]:
        reason_name = str(c.get("reason_name", c["name"])).upper()
        columns.append(ColumnSpec(
            name=c["name"], source=c["source"], type=c["type"], required=bool(c.get("required", False)),
            reason_name=reason_name, format=c.get("format"),
            min=_rule(c.get("min"), f"{reason_name}_BELOW_MIN"),
            max=_rule(c.get("max"), f"{reason_name}_ABOVE_MAX"),
            allowed_values=tuple(c["allowed_values"]) if c.get("allowed_values") is not None else None,
            on_the_hour=bool(c.get("on_the_hour", False)),
            description=c.get("description"),
        ))
    retry = raw.get("retry", {})
    fault = raw.get("fault_injection") or {}
    governance = raw.get("governance") or {}
    return SourceConfig(
        source_name=str(raw["source_name"]),
        description=str(raw.get("description", "")),
        root_dir=root,
        catalog=catalog or framework["catalog"],
        schemas={layer: f"{framework['schemas'][layer]}{schema_suffix}" for layer in LAYERS},
        landing_volume=framework["landing_volume"],
        extraction_type=str(extraction.get("type", "api")).lower(),
        bronze_table=target["bronze_table"],
        silver_table=target["silver_table"],
        gold_table=target["gold_table"],
        method=str(request.get("method", "GET")).upper(),
        base_url=str(request.get("base_url", "")),
        timeout_seconds=float(request.get("timeout_seconds", 10)),
        params=dict(request.get("params") or {}),
        headers=dict(request.get("headers") or {}),
        auth=dict(request.get("auth") or {"type": "none"}),
        pagination=dict(request.get("pagination") or {"type": "none"}),
        retry=RetrySpec(
            max_attempts=int(retry.get("max_attempts", 3)),
            backoff_base_seconds=float(retry.get("backoff_base_seconds", 1.0)),
            backoff_cap_seconds=float(retry.get("backoff_cap_seconds", 8.0)),
            retry_after_cap_seconds=float(retry.get("retry_after_cap_seconds", 30)),
            retry_statuses=frozenset(int(s) for s in retry.get("retry_statuses", [429, 500, 502, 503, 504])),
        ),
        window_spec={k: dict(v) for k, v in (raw.get("window") or {}).items()},
        entity_id_field=id_field,
        entity_columns=entity_columns,
        entities=entities,
        min_entities=int(ents.get("min_count", 1)),
        response_format=str(response["format"]),
        data_path=str(response["data_path"]),
        error_flag_path=response.get("error_flag_path"),
        error_message_path=response.get("error_message_path"),
        utc_offset_path=response.get("utc_offset_path"),
        allow_empty=bool(response.get("allow_empty", False)),
        contract=tuple(FieldContract(name, spec["type"], bool(spec.get("required", False)),
                                     spec.get("unit_path"), spec.get("unit"))
                       for name, spec in response["contract"].items()),
        ignore_fields=tuple(response.get("ignore_fields") or ()),
        silver_keys=tuple(silver["keys"]),
        silver_partition_by=tuple(silver.get("partition_by") or ()),
        numeric_strings=str(silver.get("numeric_strings", "convert")),
        duplicate_reason=str(silver.get("duplicate_reason", "DUPLICATE_KEY")),
        columns=tuple(columns),
        derived=tuple(DerivedSpec(d["name"], d["type"], d["from"], d.get("description"))
                      for d in silver.get("derived") or ()),
        gold_sql_file=root / gold["sql_file"],
        gold_keys=tuple(gold["keys"]),
        gold_params=dict(gold.get("params") or {}),
        fixtures_dir=root / raw.get("fixtures_dir", f"tests/fixtures/{raw['source_name']}"),
        fault_default_target=fault.get("default_target"),
        fault_scenarios={k: dict(v) for k, v in (fault.get("scenarios") or {}).items()},
        data_owner=str(governance.get("data_owner", raw.get("owner", "unassigned"))),
        data_steward=str(governance.get("data_steward", governance.get("data_owner", raw.get("owner", "unassigned")))),
        classification=str(governance.get("classification", "internal")),
        retention_policy=str(governance.get("retention_policy", "")),
        retention_days={str(k): int(v) for k, v in (governance.get("retention_days") or {}).items()},
        shared_governance={str(k): str(v) for k, v in (framework.get("governance") or {}).items()},
        gold_descriptions={str(k): str(v) for k, v in (gold.get("column_descriptions") or {}).items()},
    )


# ======================================================================== validation
def template_refs(value: Any) -> list[tuple[str, str]]:
    return TEMPLATE.findall(value) if isinstance(value, str) else []


def validate_source(cfg: SourceConfig) -> None:
    p: list[str] = []
    names = [("source_name", cfg.source_name), ("catalog", cfg.catalog), ("bronze_table", cfg.bronze_table),
             ("silver_table", cfg.silver_table), ("gold_table", cfg.gold_table)]
    names += [(f"schemas.{layer}", schema) for layer, schema in cfg.schemas.items()]
    for label, value in names:
        if not IDENTIFIER.match(value):
            p.append(f"{label} {value!r} must be a simple identifier")

    # extraction type: the framework is generic; only API extraction is implemented so far
    if cfg.extraction_type not in EXTRACTION_TYPES:
        p.append(f"extraction.type must be one of {sorted(EXTRACTION_TYPES)}")
    elif cfg.extraction_type not in IMPLEMENTED_EXTRACTION_TYPES:
        p.append(f"extraction.type {cfg.extraction_type!r} is planned but not implemented yet "
                 f"(implemented: {sorted(IMPLEMENTED_EXTRACTION_TYPES)})")
    if cfg.extraction_type == "api" and not cfg.base_url.startswith("https://"):
        p.append("request.base_url is required for api extraction and must use https")
    if cfg.method not in ("GET", "POST"):
        p.append("method must be GET or POST")
    if not 0 < cfg.timeout_seconds <= 120:
        p.append("timeout_seconds must be in (0, 120]")

    # entities
    ids = [e.id for e in cfg.entities]
    if len(ids) < cfg.min_entities:
        p.append(f"need at least {cfg.min_entities} entities, got {len(ids)}")
    if len(ids) != len(set(ids)):
        p.append("entity ids must be unique")
    for e in cfg.entities:
        if not IDENTIFIER.match(e.id):
            p.append(f"entity id {e.id!r} must be a simple identifier")
    col_names = [c.name for c in cfg.entity_columns]
    if cfg.entity_id_field not in col_names:
        p.append(f"entities.columns must include the id_field {cfg.entity_id_field!r}")
    for c in cfg.entity_columns:
        if c.type not in SILVER_TYPES:
            p.append(f"entity column {c.name}: unknown type {c.type!r}")
        missing = [e.id for e in cfg.entities if c.name not in e.attributes]
        if missing:
            p.append(f"entity column {c.name!r} missing for {missing}")

    # templates, window, secrets
    for name, value in {**cfg.params, **cfg.headers}.items():
        for scope, ref in template_refs(value):
            if scope == "entity" and any(ref not in e.attributes for e in cfg.entities):
                p.append(f"template {{entity.{ref}}} in {name!r}: not an attribute of every entity")
            if scope == "window" and ref not in cfg.window_spec:
                p.append(f"template {{window.{ref}}} in {name!r}: not defined under window")
        if SECRET_LIKE.search(name) and not template_refs(value):
            p.append(f"{name!r} looks like a credential; use request.auth with a secret scope, never inline values")
    for name, spec in cfg.window_spec.items():
        kind = spec.get("type", "string")
        if kind not in ("int", "date", "string", "relative_date"):
            p.append(f"window.{name}: unknown type {kind!r}")
    try:
        resolve_window(cfg, {})
    except ConfigError as exc:
        p.append(str(exc))

    # auth / pagination / retry
    auth_type = cfg.auth.get("type", "none")
    if auth_type not in ("none", "api_key"):
        p.append("auth.type must be none or api_key")
    if auth_type == "api_key":
        for k in ("in", "name", "secret_scope", "secret_key"):
            if not cfg.auth.get(k):
                p.append(f"auth.{k} is required for api_key auth")
        if cfg.auth.get("in") not in ("header", "query"):
            p.append("auth.in must be header or query")
        if "value" in cfg.auth or "token" in cfg.auth:
            p.append("auth must reference a secret scope/key, not contain the secret")
    pg = cfg.pagination
    pg_type = pg.get("type", "none")
    if pg_type not in PAGINATION_TYPES:
        p.append(f"pagination.type must be one of {sorted(PAGINATION_TYPES)}")
    required_pg = {"next_url": ["next_url_path"], "cursor": ["cursor_path", "cursor_param"],
                   "page_number": ["page_param"], "offset": ["offset_param", "limit_param", "limit"]}
    for k in required_pg.get(pg_type, []):
        if pg.get(k) in (None, ""):
            p.append(f"pagination.{k} is required for {pg_type}")
    if pg_type != "none" and not 1 <= int(pg.get("max_pages", 100)) <= 1000:
        p.append("pagination.max_pages must be 1-1000")
    if not 1 <= cfg.retry.max_attempts <= 5:
        p.append("retry.max_attempts must be 1-5 (bounded retries)")
    if cfg.retry.backoff_base_seconds < 0 or cfg.retry.backoff_cap_seconds < cfg.retry.backoff_base_seconds:
        p.append("retry backoff settings invalid")

    # response contract
    if cfg.response_format not in ("columnar", "records"):
        p.append("response.format must be columnar or records")
    if not cfg.contract:
        p.append("response.contract must list at least one field")
    for c in cfg.contract:
        if c.type not in CONTRACT_TYPES:
            p.append(f"contract field {c.name}: unknown type {c.type!r}")
        if bool(c.unit) != bool(c.unit_path):
            p.append(f"contract field {c.name}: unit and unit_path go together")
    if cfg.response_format == "columnar" and any("." in c.name for c in cfg.contract):
        p.append("columnar contract fields cannot be nested paths")

    # silver
    if cfg.numeric_strings not in ("convert", "reject"):
        p.append("silver.numeric_strings must be convert or reject")
    all_cols = cfg.silver_column_types()
    if len(all_cols) != len(cfg.entity_columns) + len(cfg.columns) + len(cfg.derived):
        p.append("silver column names must be unique across entity, typed and derived columns")
    for k in cfg.silver_keys:
        if k not in all_cols:
            p.append(f"silver key {k!r} is not a silver column")
    for k in cfg.silver_partition_by:
        if k not in all_cols:
            p.append(f"silver.partition_by {k!r} is not a silver column")
        elif all_cols[k] not in ("date", "string", "int", "bigint"):
            p.append(f"silver.partition_by {k!r} should be a low-cardinality date/string/int column")
    contract_names = {c.name for c in cfg.contract}
    for c in cfg.columns:
        if not IDENTIFIER.match(c.name):
            p.append(f"silver column {c.name!r} must be a simple identifier")
        if c.type not in SILVER_TYPES:
            p.append(f"silver column {c.name}: unknown type {c.type!r}")
        if c.source not in contract_names:
            p.append(f"silver column {c.name}: source {c.source!r} is not in response.contract")
        if c.type in ("timestamp", "date") and not c.format:
            p.append(f"silver column {c.name}: format is required for {c.type}")
        if c.min and c.max and c.min.value > c.max.value:
            p.append(f"silver column {c.name}: min > max")
        if (c.min or c.max) and c.type not in ("double", "int", "bigint"):
            p.append(f"silver column {c.name}: min/max only apply to numeric columns")
        if c.on_the_hour and c.type != "timestamp":
            p.append(f"silver column {c.name}: on_the_hour only applies to timestamps")
        if c.name in cfg.silver_keys and not c.required:
            p.append(f"silver key column {c.name} must be required")
    for d in cfg.derived:
        if d.type != "date" or all_cols.get(d.from_column) != "timestamp":
            p.append(f"derived {d.name}: only type 'date' from a timestamp column is supported")

    # gold
    if not cfg.gold_sql_file.exists():
        p.append(f"gold sql_file not found: {cfg.gold_sql_file}")
    else:
        sql = strip_sql_comments(cfg.gold_sql())
        for placeholder in ("{silver}", "{affected_keys}"):
            if placeholder not in sql:
                p.append(f"gold SQL must use the {placeholder} placeholder")
        for param in set(re.findall(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)", sql)):
            if param not in cfg.gold_params and param != "run_id":
                p.append(f"gold SQL parameter :{param} is not defined in gold.params")
    for k in cfg.gold_keys:
        if k not in all_cols:
            p.append(f"gold key {k!r} must be a silver column (it scopes the recompute)")
    if "run_id" in cfg.gold_params:
        p.append("gold.params may not define the reserved name run_id")

    # governance
    if cfg.classification not in CLASSIFICATIONS:
        p.append(f"governance.classification must be one of {sorted(CLASSIFICATIONS)}")
    if not cfg.data_owner or cfg.data_owner == "unassigned":
        p.append("governance.data_owner is required")
    for layer, days in cfg.retention_days.items():
        if layer not in ("raw", "silver", "gold"):
            p.append(f"governance.retention_days.{layer}: layer must be raw, silver or gold")
        elif days < 1:
            p.append(f"governance.retention_days.{layer} must be >= 1 day")

    # fault injection
    if cfg.fault_default_target and cfg.fault_default_target not in ids:
        p.append(f"fault_injection.default_target {cfg.fault_default_target!r} is not an entity")
    for name, sc in cfg.fault_scenarios.items():
        action = sc.get("action")
        if action not in FAULT_ACTIONS:
            p.append(f"fault scenario {name}: unknown action {action!r}")
        elif action == "override_param" and sc.get("param") not in cfg.params:
            p.append(f"fault scenario {name}: param {sc.get('param')!r} is not a request param")
        elif action in ("set_value", "set_null", "drop_field") and sc.get("field") not in contract_names:
            p.append(f"fault scenario {name}: field {sc.get('field')!r} is not in the contract")

    if p:
        raise ConfigError(f"{cfg.source_name}: " + "; ".join(p))


# ======================================================================== window
def resolve_window(cfg: SourceConfig, overrides: dict[str, Any] | None, today: date | None = None) -> dict[str, Any]:
    """Resolve window values (with run-time overrides) and validate them."""
    today = today or date.today()
    overrides = overrides or {}
    unknown = set(overrides) - set(cfg.window_spec)
    problems = [f"window override {k!r} is not defined in the source window" for k in sorted(unknown)]
    resolved: dict[str, Any] = {}
    for name, spec in cfg.window_spec.items():
        kind = spec.get("type", "string")
        value = overrides.get(name, spec.get("value"))
        if kind == "relative_date" and name not in overrides:
            value = (today + timedelta(days=int(spec.get("offset_days", 0)))).isoformat()
        if value in (None, ""):
            resolved[name] = None
            continue
        try:
            if kind == "int":
                value = int(value)
                if "min" in spec and value < spec["min"] or "max" in spec and value > spec["max"]:
                    problems.append(f"window.{name}={value} outside [{spec.get('min')}, {spec.get('max')}]")
            elif kind in ("date", "relative_date"):
                value = date.fromisoformat(str(value)).isoformat()
            else:
                value = str(value)
        except (TypeError, ValueError):
            problems.append(f"window.{name}={value!r} is not a valid {kind}")
        resolved[name] = value
    if problems:
        raise ConfigError("; ".join(problems))
    return resolved


def strip_sql_comments(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def is_ignored(path: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatch.fnmatchcase(path, pat) for pat in patterns)


def with_changes(cfg: SourceConfig, **changes: Any) -> SourceConfig:
    """Validated copy with fields replaced (tests)."""
    updated = replace(cfg, **changes)
    validate_source(updated)
    return updated
