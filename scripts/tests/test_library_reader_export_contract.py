"""The reader export contract that scripts/check_verdict_once.py applies to
the release build's `library_read_model_diff --export-sql`."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("reader_export_contract", Path(__file__).parents[1] / "library_reader_export_contract.py")
r = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(r)
DESCRIPTOR = {"source_sha": "a" * 40, "model_sql_sha256": "b" * 64, "definition_version": 3, "context_version": 2, "program_version": 4}


class SqlExportTests(unittest.TestCase):
    @staticmethod
    def export_fixture(descriptor):
        descriptor = {**copy.deepcopy(descriptor), "definition_version": 3, "context_version": 2, "program_version": 4}
        # Independent expected wire contract, not the implementation constant.
        contract = {
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
        rows = []
        for mode in ("all_retained", "current"):
            for filtered in (False, True):
                for strategy in ("empty", "short", "indexed"):
                    program = {"contract": copy.deepcopy(contract), "clean_model_attempt_sql": "SELECT " + str(len(rows)),
                               "dirty_model_attempt_sql": "SELECT 'dirty "+ str(len(rows)) + "'",
                               "conditional_live_sql": "SELECT 'canonical é " + str(len(rows)) + "'"}
                    rows.append({"kind": "reader_program", **descriptor, "outcome_versions": mode, "filtered": filtered, "text_strategy": strategy, "program": program,
                                 "bind_types": copy.deepcopy(contract["bind_types"]), "query_settings": copy.deepcopy(contract["query_settings"]),
                                 "page_limit_bind": "requested_page_size_plus_one"})
        SqlExportTests.rehash(descriptor, rows)
        return descriptor, rows

    @staticmethod
    def rehash(descriptor, rows):
        hasher = r.hashlib.sha256()
        for row in rows:
            program = row["program"]
            contract_bytes = json.dumps(program["contract"], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
            for part in (contract_bytes, program["clean_model_attempt_sql"].encode(), program["dirty_model_attempt_sql"].encode(), program["conditional_live_sql"].encode()):
                hasher.update(part + b"\0")
        descriptor["model_sql_sha256"] = hasher.hexdigest()
        for row in rows:
            row["model_sql_sha256"] = descriptor["model_sql_sha256"]

    def test_final_twelve_variants_roundtrip_and_old_eight_are_rejected(self):
        descriptor, rows = self.export_fixture(DESCRIPTOR)
        def run(data):
            return subprocess.CompletedProcess([], 0, "\n".join(json.dumps(row) for row in data), "")
        with patch.object(r.subprocess, "run", return_value=run(rows)):
            result = r.sql_export("/fake/bin", descriptor)
            self.assertEqual(result["variant_count"], 12)
            self.assertEqual([(row["outcome_versions"], row["filtered"], row["text_strategy"]) for row in result["variants"]],
                             [(mode, filtered, strategy) for mode in ("all_retained", "current") for filtered in (False, True)
                              for strategy in ("empty", "short", "indexed")])
        legacy_eight = [row for row in rows if row["text_strategy"] != "empty"]
        with patch.object(r.subprocess, "run", return_value=run(legacy_eight)), self.assertRaisesRegex(r.ContractError, "twelve_variants_required"):
            r.sql_export("/fake/bin", descriptor)
        rows[-1]["program"]["clean_model_attempt_sql"] += " changed"
        with patch.object(r.subprocess, "run", return_value=run(rows)), self.assertRaisesRegex(r.ContractError, "definition_hash"):
            r.sql_export("/fake/bin", descriptor)

    def test_missing_duplicate_or_mislabelled_empty_and_short_exports_fail(self):
        descriptor, rows = self.export_fixture(DESCRIPTOR)
        missing, duplicate, swapped = copy.deepcopy(rows), copy.deepcopy(rows), copy.deepcopy(rows)
        missing.pop(1)
        duplicate[1] = copy.deepcopy(duplicate[0])
        swapped[0]["text_strategy"], swapped[1]["text_strategy"] = "short", "empty"
        for records in (missing, duplicate, swapped):
            child = subprocess.CompletedProcess([], 0, "\n".join(json.dumps(row) for row in records), "")
            with self.subTest(records=len(records)), patch.object(r.subprocess, "run", return_value=child), self.assertRaises(r.ContractError):
                r.sql_export("/fake/bin", descriptor)

    def test_nonempty_short_sql_bytes_are_included_independently_of_empty(self):
        descriptor, rows = self.export_fixture(DESCRIPTOR)
        self.assertEqual(len(rows), 12)
        self.assertNotEqual(rows[0]["program"]["clean_model_attempt_sql"], rows[1]["program"]["clean_model_attempt_sql"])
        rows[1]["program"]["clean_model_attempt_sql"] = rows[0]["program"]["clean_model_attempt_sql"]
        child = subprocess.CompletedProcess([], 0, "\n".join(json.dumps(row) for row in rows), "")
        with patch.object(r.subprocess, "run", return_value=child), self.assertRaisesRegex(r.ContractError, "definition_hash"):
            r.sql_export("/fake/bin", descriptor)

    def test_ordered_twelve_sql_hash_requires_nul_separators(self):
        descriptor, rows = self.export_fixture(DESCRIPTOR)
        descriptor["model_sql_sha256"] = r.hashlib.sha256(b"".join(part for row in rows for part in (r.canonical(row["program"]["contract"]), row["program"]["clean_model_attempt_sql"].encode(), row["program"]["dirty_model_attempt_sql"].encode(), row["program"]["conditional_live_sql"].encode()))).hexdigest()
        for row in rows:
            row["model_sql_sha256"] = descriptor["model_sql_sha256"]
        child = subprocess.CompletedProcess([], 0, "\n".join(json.dumps(row) for row in rows), "")
        with patch.object(r.subprocess, "run", return_value=child), self.assertRaisesRegex(r.ContractError, "definition_hash"):
            r.sql_export("/fake/bin", descriptor)


class ProgramCompatibilityTests(unittest.TestCase):
    @staticmethod
    def child(rows):
        return subprocess.CompletedProcess([], 0, "\n".join(json.dumps(row) for row in rows), "")

    def test_descriptor_requires_supported_exact_integer_program_and_cache_versions(self):
        descriptor, _ = SqlExportTests.export_fixture(DESCRIPTOR)
        with patch.object(r.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps(descriptor), "")):
            self.assertEqual(r.describe("/fake/bin"), descriptor)
        for key, supported in (("definition_version", 3), ("context_version", 2), ("program_version", 4)):
            for invalid in (None, True, float(supported), 0, supported + 1):
                bad = {**descriptor, key: invalid}
                if invalid is None:
                    del bad[key]
                with self.subTest(key=key, invalid=invalid), patch.object(r.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps(bad), "")), self.assertRaises(r.ContractError):
                    r.describe("/fake/bin")
        with patch.object(r.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps({**descriptor, "extra": 1}), "")), self.assertRaisesRegex(r.ContractError, "descriptor_fields"):
            r.describe("/fake/bin")

    def test_program_summary_binds_contract_and_each_distinct_stage(self):
        descriptor, rows = SqlExportTests.export_fixture(DESCRIPTOR)
        with patch.object(r.subprocess, "run", return_value=self.child(rows)):
            result = r.sql_export("/fake/bin", descriptor)
        self.assertEqual(result["program_version"], 4)
        first = result["variants"][0]
        self.assertEqual(first["contract_sha256"], r.hashlib.sha256(r.canonical(rows[0]["program"]["contract"])).hexdigest())
        for stage in ("clean_model_attempt_sql", "dirty_model_attempt_sql", "conditional_live_sql"):
            self.assertEqual(first[stage + "_sha256"], r.hashlib.sha256(rows[0]["program"][stage].encode()).hexdigest())
        self.assertNotEqual(first["clean_model_attempt_sql_sha256"], first["conditional_live_sql_sha256"])

    def test_each_stage_byte_tampering_fails_family_hash(self):
        for stage in ("clean_model_attempt_sql", "dirty_model_attempt_sql", "conditional_live_sql"):
            descriptor, rows = SqlExportTests.export_fixture(DESCRIPTOR)
            rows[7]["program"][stage] += " changed"
            with self.subTest(stage=stage), patch.object(r.subprocess, "run", return_value=self.child(rows)), self.assertRaisesRegex(r.ContractError, "definition_hash"):
                r.sql_export("/fake/bin", descriptor)

    def test_self_consistent_unsupported_contract_cannot_raise_limits_or_change_sequence(self):
        mutations = {"wall_ms": 24000, "offline_wall_ms": 240000, "cleanup_wall_ms": 2000,
                     "transaction": "read_committed", "stages": ["conditional_unchanged_canonical_live", "one_statement_model_attempt"],
                     "errors": "retry_live", "fallback": "any_error", "wall_start": "after_render", "wall_end": "before_restore",
                     "statement_timeout": "per_stage", "version": True}
        for key, value in mutations.items():
            descriptor, rows = SqlExportTests.export_fixture(DESCRIPTOR)
            for row in rows:
                row["program"]["contract"][key] = value
            SqlExportTests.rehash(descriptor, rows)
            with self.subTest(key=key), patch.object(r.subprocess, "run", return_value=self.child(rows)), self.assertRaisesRegex(r.ContractError, "program_contract"):
                r.sql_export("/fake/bin", descriptor)

    def test_dirty_routing_listing_definition_change_budget_and_order_cannot_be_relabelled(self):
        descriptor, rows = SqlExportTests.export_fixture(DESCRIPTOR)
        contract = rows[0]["program"]["contract"]
        changes = [lambda c: c.update(dirty_routing="always_dirty"), lambda c: c.pop("dirty_routing"),
                   lambda c: c.update(listing_definition=2), lambda c: c.update(listing_definition=3.0),
                   lambda c: c.pop("listing_definition"), lambda c: c["stages"].reverse(),
                   lambda c: c["stages"].pop(1), lambda c: c.update(stages=["one_statement_model_attempt", "conditional_unchanged_canonical_live"])]
        for field in contract["change_budget"]:
            changes.extend((lambda c, key=field: c["change_budget"].pop(key),
                            lambda c, key=field: c["change_budget"].update({key: "unsupported"})))
        changes.extend((lambda c: c["change_budget"].update(distinct_items=1024),
                        lambda c: c["change_budget"].update(distinct_items=512.0),
                        lambda c: c["change_budget"].update(worker_heartbeat_seconds=60),
                        lambda c: c["change_budget"].update(combined_limit=128),
                        lambda c: c["change_budget"].update(extra=1)))
        for field in contract["hub"]:
            changes.extend((lambda c, key=field: c["hub"].pop(key),
                            lambda c, key=field: c["hub"].update({key: "unsupported"})))
        changes.extend((lambda c: c["hub"].update(stale_seconds=31), lambda c: c.pop("hub"),
                        lambda c: c["hub"].update(serve="stale_everything")))
        for change in changes:
            descriptor, rows = SqlExportTests.export_fixture(DESCRIPTOR)
            for row in rows:
                change(row["program"]["contract"])
            SqlExportTests.rehash(descriptor, rows)
            with patch.object(r.subprocess, "run", return_value=self.child(rows)), self.assertRaisesRegex(r.ContractError, "program_contract"):
                r.sql_export("/fake/bin", descriptor)

    def test_equal_numeric_float_contract_and_nested_bind_or_setting_drift_fail(self):
        for mutate in (lambda c: c.update(wall_ms=12000.0), lambda c: c["bind_types"].reverse(),
                       lambda c: c["query_settings"].update(jit="on"), lambda c: c.update(extra="unknown")):
            descriptor, rows = SqlExportTests.export_fixture(DESCRIPTOR)
            for row in rows:
                mutate(row["program"]["contract"])
            SqlExportTests.rehash(descriptor, rows)
            with patch.object(r.subprocess, "run", return_value=self.child(rows)), self.assertRaisesRegex(r.ContractError, "program_contract"):
                r.sql_export("/fake/bin", descriptor)

    def test_missing_extra_empty_and_nonstring_program_fields_fail(self):
        changes = [lambda p: p.update(extra="unknown"),
                   lambda p: p.update(model_attempt_sql=p.pop("clean_model_attempt_sql"))]
        for stage in ("clean_model_attempt_sql", "dirty_model_attempt_sql", "conditional_live_sql"):
            changes.extend((lambda p, key=stage: p.pop(key), lambda p, key=stage: p.update({key: ""}),
                            lambda p, key=stage: p.update({key: None}), lambda p, key=stage: p.update({key: 123})))
        for change in changes:
            descriptor, rows = SqlExportTests.export_fixture(DESCRIPTOR)
            change(rows[0]["program"])
            with patch.object(r.subprocess, "run", return_value=self.child(rows)), self.assertRaises(r.ContractError):
                r.sql_export("/fake/bin", descriptor)

    def test_complete_legacy_program_one_definition_two_descriptor_and_export_are_rejected(self):
        descriptor, rows = SqlExportTests.export_fixture(DESCRIPTOR)
        descriptor.update(definition_version=2, program_version=1)
        hasher = r.hashlib.sha256()
        for row in rows:
            row.update(definition_version=2, program_version=1)
            program = row["program"]
            program["model_attempt_sql"] = program.pop("clean_model_attempt_sql")
            del program["dirty_model_attempt_sql"]
            program["contract"].update(version=1, stages=["one_statement_model_attempt", "conditional_unchanged_canonical_live"])
            for field in ("dirty_routing", "listing_definition", "change_budget"):
                del program["contract"][field]
            for part in (r.canonical(program["contract"]), program["model_attempt_sql"].encode(), program["conditional_live_sql"].encode()):
                hasher.update(part + b"\0")
        descriptor["model_sql_sha256"] = hasher.hexdigest()
        for row in rows:
            row["model_sql_sha256"] = hasher.hexdigest()
        with patch.object(r.subprocess, "run", return_value=self.child(rows)) as child, self.assertRaisesRegex(r.ContractError, "descriptor_contract"):
            r.sql_export("/fake/bin", descriptor)
        child.assert_not_called()
        with patch.object(r.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps(descriptor), "")), self.assertRaisesRegex(r.ContractError, "descriptor_contract"):
            r.describe("/fake/bin")

    def test_old_single_statement_export_and_undeclared_fields_are_rejected(self):
        for old in (True, False):
            descriptor, rows = SqlExportTests.export_fixture(DESCRIPTOR)
            if old:
                for row in rows:
                    row["kind"] = "sql_definition"
                    row["sql"] = row.pop("program")["clean_model_attempt_sql"]
            else:
                rows[0]["sql"] = "unexpected"
            with patch.object(r.subprocess, "run", return_value=self.child(rows)), self.assertRaises(r.ContractError):
                r.sql_export("/fake/bin", descriptor)

    def test_contract_and_stage_separators_are_each_part_of_the_hash(self):
        for missing in (0, 1, 2, 3):
            descriptor, rows = SqlExportTests.export_fixture(DESCRIPTOR)
            hasher = r.hashlib.sha256()
            for row in rows:
                p = row["program"]
                for index, part in enumerate((r.canonical(p["contract"]), p["clean_model_attempt_sql"].encode(), p["dirty_model_attempt_sql"].encode(), p["conditional_live_sql"].encode())):
                    hasher.update(part + (b"" if index == missing else b"\0"))
            descriptor["model_sql_sha256"] = hasher.hexdigest()
            for row in rows:
                row["model_sql_sha256"] = hasher.hexdigest()
            with patch.object(r.subprocess, "run", return_value=self.child(rows)), self.assertRaisesRegex(r.ContractError, "definition_hash"):
                r.sql_export("/fake/bin", descriptor)

    def test_row_versions_and_outer_bind_or_settings_cannot_drift(self):
        for mutate in (lambda row: row.update(program_version=True), lambda row: row.update(program_version=1.0),
                       lambda row: row.update(source_sha="f" * 40), lambda row: row["bind_types"].reverse(),
                       lambda row: row["query_settings"].update(statement_timeout="24s")):
            descriptor, rows = SqlExportTests.export_fixture(DESCRIPTOR)
            mutate(rows[0])
            with patch.object(r.subprocess, "run", return_value=self.child(rows)), self.assertRaises(r.ContractError):
                r.sql_export("/fake/bin", descriptor)
