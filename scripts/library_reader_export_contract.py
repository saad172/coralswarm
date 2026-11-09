"""The Library reader's sealed export contract.

`library_read_model_diff --describe` prints the compiled reader's descriptor
and `--export-sql` prints its twelve `reader_program` records. The release
build (deploy.yml, "Validate generated Library verdicts") feeds that export to
`scripts/check_verdict_once.py`, which validates it here before inspecting the
rendered SQL. The hash is informational: nothing at boot compares it any more.
"""
import hashlib
import json
from pathlib import Path
import re
import subprocess


class ContractError(Exception):
    pass


def require(condition, category):
    if not condition:
        raise ContractError(category)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def valid_sha(value, length=40):
    return isinstance(value, str) and re.fullmatch("[0-9a-f]{" + str(length) + "}", value) is not None


# Compare canonical bytes: Python numeric equality would accept True as 1 or
# 12000.0 as 12000 and silently change the exact sealed wire representation.
PROGRAM_CONTRACT = {
    "version": 4, "transaction": "read_only_repeatable_read",
    "stages": ["guarded_clean_model_attempt", "conditional_same_snapshot_dirty_model_attempt", "conditional_unchanged_canonical_live"],
    "dirty_routing": "successful_needs_dirty_only_same_snapshot", "listing_definition": 3,
    "change_budget": {"distinct_items": 512, "distinct_nodes": 256, "object_keys": 512, "object_rows": 4096,
                      "object_edges": 16384, "node_raw_edges": 4096, "item_inputs": 16384,
                      "overflow_counts": "bounded_prefix_lower_bounds", "relevance": "old_or_new_image_in_scope",
                      "worker_heartbeat_seconds": 300, "backlog_alarm_seconds": 900},
    "hub": {"serve": "stale_nodes_exact_listing", "stale_seconds": 30, "covers": ["dirty_hub", "raw_edge_overflow"], "removals": "live"},
    "fallback": "successful_known_reason_only", "errors": "never_fall_through",
    "wall_ms": 12000, "offline_wall_ms": 120000,
    "wall_start": "before_internal_render", "wall_end": "final_json_and_timeout_restore",
    "statement_timeout": "remaining_program_wall_capped_by_original_setting", "cleanup_wall_ms": 1000,
    "bind_types": ["uuid[]", "jsonb", "uuid", "uuid", "uuid", "text", "text", "timestamptz", "uuid", "timestamptz", "uuid", "bigint", "bigint", "uuid", "bigint", "timestamptz"],
    "query_settings": {"statement_timeout": "12s", "plan_cache_mode": "force_custom_plan", "jit": "off", "work_mem": "64MB", "max_parallel_workers_per_gather": "0"},
}


def validate_descriptor(data):
    require(isinstance(data, dict) and set(data) == {"source_sha", "model_sql_sha256", "definition_version", "context_version", "program_version"}, "diff_descriptor_fields")
    require(valid_sha(data["source_sha"]) and valid_sha(data["model_sql_sha256"], 64) and
            all(type(data[key]) is int and data[key] == version for key, version in
                (("definition_version", 3), ("context_version", 2), ("program_version", 4))), "diff_descriptor_contract")


def describe(binary):
    try:
        result = subprocess.run([str(Path(binary).resolve()), "--describe"], capture_output=True,
                                text=True, timeout=15)
        data = json.loads(result.stdout)
    except (OSError, ValueError, subprocess.TimeoutExpired):
        raise ContractError("diff_descriptor_failed") from None
    require(result.returncode == 0, "diff_descriptor_failed")
    validate_descriptor(data)
    return data


def sql_export(binary, descriptor):
    validate_descriptor(descriptor)
    try:
        result = subprocess.run([str(Path(binary).resolve()), "--export-sql"], capture_output=True, text=True, timeout=15)
        records = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    except (OSError, ValueError, subprocess.TimeoutExpired):
        raise ContractError("sql_export_failed") from None
    require(result.returncode == 0, "sql_export_failed")
    return validate_sql_export_records(records, descriptor)


def validate_sql_export_records(records, descriptor):
    """The native export gate admits exactly the sealed program family."""
    validate_descriptor(descriptor)
    expected = [(mode, filtered, strategy) for mode in ("all_retained", "current") for filtered in (False, True) for strategy in ("empty", "short", "indexed")]
    require(len(records) == len(expected), "sql_export_twelve_variants_required")
    hasher, variants = hashlib.sha256(), []
    contract_bytes = canonical(PROGRAM_CONTRACT)
    for row, identity in zip(records, expected):
        require(isinstance(row, dict) and set(row) == set(descriptor) | {"kind", "outcome_versions", "filtered", "text_strategy", "program", "bind_types", "query_settings", "page_limit_bind"}, "sql_export_fields")
        require(canonical({key: row[key] for key in descriptor}) == canonical(descriptor) and row["kind"] == "reader_program" and
                type(row["filtered"]) is bool and (row["outcome_versions"], row["filtered"], row["text_strategy"]) == identity,
                "sql_export_source_or_variant")
        require(row["bind_types"] == PROGRAM_CONTRACT["bind_types"] and row["query_settings"] == PROGRAM_CONTRACT["query_settings"] and
                row["page_limit_bind"] == "requested_page_size_plus_one", "sql_export_contract")
        program = row["program"]
        require(isinstance(program, dict) and set(program) == {"contract", "clean_model_attempt_sql", "dirty_model_attempt_sql", "conditional_live_sql"}, "sql_export_program_fields")
        require(canonical(program["contract"]) == contract_bytes, "sql_export_program_contract")
        parts = [contract_bytes]
        for stage in ("clean_model_attempt_sql", "dirty_model_attempt_sql", "conditional_live_sql"):
            require(isinstance(program[stage], str), "sql_export_program_sql")
            try:
                encoded = program[stage].encode("utf-8")
            except UnicodeError:
                raise ContractError("sql_export_program_sql") from None
            require(0 < len(encoded) <= 1_000_000, "sql_export_program_sql")
            parts.append(encoded)
        for part in parts:
            hasher.update(part)
            hasher.update(b"\0")
        variants.append({"outcome_versions": identity[0], "filtered": identity[1], "text_strategy": identity[2],
                         "contract_sha256": hashlib.sha256(parts[0]).hexdigest(),
                         "clean_model_attempt_sql_sha256": hashlib.sha256(parts[1]).hexdigest(),
                         "dirty_model_attempt_sql_sha256": hashlib.sha256(parts[2]).hexdigest(),
                         "conditional_live_sql_sha256": hashlib.sha256(parts[3]).hexdigest()})
    require(hasher.hexdigest() == descriptor["model_sql_sha256"], "sql_export_definition_hash")
    return {"variant_count": 12, "program_version": descriptor["program_version"], "descriptor": descriptor,
            "query_settings": records[0]["query_settings"], "variants": variants}
