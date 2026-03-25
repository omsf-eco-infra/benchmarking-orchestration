from __future__ import annotations

import json
from io import BytesIO
from unittest.mock import MagicMock, patch

from benchmarking_orchestration.analysis import (
    RunRecord,
    fetch_and_analyze_results,
    print_results_table,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_manifest(
    launch_task_id: str = "us-east-1:g5.xlarge:ami-12345678:aaaaaaaa-0000-0000-0000-000000000001",
    started_at: str = "2024-01-15T10:00:00Z",
    completed_at: str = "2024-01-15T10:30:00Z",
    output_s3_key: str = "runs/2024-01-15/abc/output/md_benchmark.out",
    json_parse_ok: bool = True,
    benchmark_kind: str | None = None,
) -> bytes:
    manifest = {
        "schema_version": 1,
        "bench_task_id": f"bench:{launch_task_id}",
        "launch_task_id": launch_task_id,
        "s3_bucket": "my-bucket",
        "s3_prefix": "runs/2024-01-15/abc",
        "input": {
            "source_name": "ross_dodecahedron_jacs.json",
            "s3_key": "runs/2024-01-15/abc/input/ross_dodecahedron_jacs.json",
            "sha256": "x",
        },
        "output": {
            "source_name": "md_benchmark.out",
            "s3_key": output_s3_key,
            "sha256": "y",
            "json_parse_ok": json_parse_ok,
            "top_level_keys_match_input": True,
            "validation_message": None,
        },
        "timestamps": {
            "started_at_utc": started_at,
            "completed_at_utc": completed_at,
        },
    }
    if benchmark_kind is not None:
        manifest["benchmark_kind"] = benchmark_kind
    return json.dumps(manifest).encode("utf-8")


def _make_s3_mock(
    manifest_bytes: bytes, output_bytes: bytes | None = None
) -> MagicMock:
    """Build a minimal mock boto3 S3 client."""
    s3 = MagicMock()

    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "runs/2024-01-15/abc/manifest.json"}]}
    ]
    s3.get_paginator.return_value = paginator

    def _get_object(Bucket, Key):  # noqa: N803
        if Key.endswith("manifest.json"):
            return {"Body": BytesIO(manifest_bytes)}
        if output_bytes is not None:
            return {"Body": BytesIO(output_bytes)}
        raise Exception("Key not found")

    s3.get_object.side_effect = _get_object
    return s3


# ---------------------------------------------------------------------------
# fetch_and_analyze_results tests
# ---------------------------------------------------------------------------


def test_manifest_parsing_extracts_correct_instance_type():
    task_id = "us-east-1:g5.xlarge:ami-12345678:aaaaaaaa-0000-0000-0000-000000000001"
    manifest_bytes = _make_manifest(launch_task_id=task_id)
    output_bytes = json.dumps({"system_a": 42.0}).encode("utf-8")
    s3 = _make_s3_mock(manifest_bytes, output_bytes)

    with patch("benchmarking_orchestration.analysis.boto3") as mock_boto3:
        mock_boto3.client.return_value = s3
        records = fetch_and_analyze_results("my-bucket")

    assert len(records) == 1
    assert records[0].instance_type == "g5.xlarge"
    assert records[0].region == "us-east-1"


def test_wall_clock_computation():
    task_id = "us-east-1:g5.xlarge:ami-12345678:aaaaaaaa-0000-0000-0000-000000000001"
    manifest_bytes = _make_manifest(
        launch_task_id=task_id,
        started_at="2024-01-15T10:00:00Z",
        completed_at="2024-01-15T10:30:00Z",
    )
    output_bytes = json.dumps({"system_a": 42.0}).encode("utf-8")
    s3 = _make_s3_mock(manifest_bytes, output_bytes)

    with patch("benchmarking_orchestration.analysis.boto3") as mock_boto3:
        mock_boto3.client.return_value = s3
        records = fetch_and_analyze_results("my-bucket")

    assert records[0].wall_seconds == 1800.0


def test_ns_per_day_averaging_multiple_systems():
    task_id = "us-east-1:g5.xlarge:ami-12345678:aaaaaaaa-0000-0000-0000-000000000001"
    manifest_bytes = _make_manifest(launch_task_id=task_id)
    output_bytes = json.dumps({"system_a": 40.0, "system_b": 60.0}).encode("utf-8")
    s3 = _make_s3_mock(manifest_bytes, output_bytes)

    with patch("benchmarking_orchestration.analysis.boto3") as mock_boto3:
        mock_boto3.client.return_value = s3
        records = fetch_and_analyze_results("my-bucket")

    assert records[0].output_valid is True
    assert records[0].ns_per_day_by_system == {"system_a": 40.0, "system_b": 60.0}


def test_multiple_runs_per_instance_type():
    task_id_1 = "us-east-1:g5.xlarge:ami-12345678:aaaaaaaa-0000-0000-0000-000000000001"
    task_id_2 = "us-east-1:g5.xlarge:ami-12345678:aaaaaaaa-0000-0000-0000-000000000002"
    manifest1 = _make_manifest(
        launch_task_id=task_id_1, output_s3_key="runs/abc/output/md_benchmark.out"
    )
    manifest2 = _make_manifest(
        launch_task_id=task_id_2, output_s3_key="runs/def/output/md_benchmark.out"
    )

    s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "runs/2024-01-15/abc/manifest.json"},
                {"Key": "runs/2024-01-15/def/manifest.json"},
            ]
        }
    ]
    s3.get_paginator.return_value = paginator

    call_count = {"n": 0}

    def _get_object(Bucket, Key):  # noqa: N803
        if Key.endswith("manifest.json"):
            call_count["n"] += 1
            return {"Body": BytesIO(manifest1 if call_count["n"] == 1 else manifest2)}
        return {"Body": BytesIO(json.dumps({"system_a": 50.0}).encode("utf-8"))}

    s3.get_object.side_effect = _get_object

    with patch("benchmarking_orchestration.analysis.boto3") as mock_boto3:
        mock_boto3.client.return_value = s3
        records = fetch_and_analyze_results("my-bucket")

    assert len(records) == 2
    assert all(r.instance_type == "g5.xlarge" for r in records)


def test_missing_output_json_handled_gracefully():
    task_id = "us-east-1:g5.xlarge:ami-12345678:aaaaaaaa-0000-0000-0000-000000000001"
    manifest_bytes = _make_manifest(launch_task_id=task_id, json_parse_ok=False)
    s3 = _make_s3_mock(manifest_bytes, output_bytes=None)

    with patch("benchmarking_orchestration.analysis.boto3") as mock_boto3:
        mock_boto3.client.return_value = s3
        records = fetch_and_analyze_results("my-bucket")

    assert len(records) == 1
    assert records[0].output_valid is False
    assert records[0].ns_per_day_by_system == {}


def test_manifest_parses_rbfe_benchmark_kind():
    task_id = "us-east-1:g5.xlarge:ami-12345678:aaaaaaaa-0000-0000-0000-000000000001"
    manifest_bytes = _make_manifest(launch_task_id=task_id, benchmark_kind="rbfe")
    output_bytes = json.dumps({"system_a": 42.0}).encode("utf-8")
    s3 = _make_s3_mock(manifest_bytes, output_bytes)

    with patch("benchmarking_orchestration.analysis.boto3") as mock_boto3:
        mock_boto3.client.return_value = s3
        records = fetch_and_analyze_results("my-bucket")

    assert len(records) == 1
    assert records[0].benchmark_kind == "rbfe"


def test_invalid_output_json_handled_gracefully():
    task_id = "us-east-1:g5.xlarge:ami-12345678:aaaaaaaa-0000-0000-0000-000000000001"
    manifest_bytes = _make_manifest(launch_task_id=task_id, json_parse_ok=True)

    s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "runs/2024-01-15/abc/manifest.json"}]}
    ]
    s3.get_paginator.return_value = paginator

    def _get_object(Bucket, Key):  # noqa: N803
        if Key.endswith("manifest.json"):
            return {"Body": BytesIO(manifest_bytes)}
        # Output fetch raises
        raise Exception("S3 error fetching output")

    s3.get_object.side_effect = _get_object

    with patch("benchmarking_orchestration.analysis.boto3") as mock_boto3:
        mock_boto3.client.return_value = s3
        records = fetch_and_analyze_results("my-bucket")

    assert records[0].output_valid is False


# ---------------------------------------------------------------------------
# Cost estimation tests
# ---------------------------------------------------------------------------


def _make_pricing_response(price_usd: str) -> dict:
    """Build a minimal AWS Pricing API response."""
    return {
        "PriceList": [
            json.dumps(
                {
                    "terms": {
                        "OnDemand": {
                            "term1": {
                                "priceDimensions": {
                                    "dim1": {"pricePerUnit": {"USD": price_usd}}
                                }
                            }
                        }
                    }
                }
            )
        ]
    }


def test_cost_estimation_with_mocked_pricing_api(capsys):
    records = [
        RunRecord(
            instance_type="g5.xlarge",
            region="us-east-1",
            wall_seconds=3600.0,
            ns_per_day_by_system={"system_a": 50.0},
            output_valid=True,
        )
    ]

    pricing = MagicMock()
    pricing.get_products.return_value = _make_pricing_response("1.006")

    with patch("benchmarking_orchestration.analysis.boto3") as mock_boto3:
        mock_boto3.client.return_value = pricing
        print_results_table(records)

    captured = capsys.readouterr()
    assert "$1.0060" in captured.out
    assert "g5.xlarge" in captured.out


def test_cost_estimation_na_when_pricing_api_returns_no_result(capsys):
    records = [
        RunRecord(
            instance_type="g5.xlarge",
            region="us-east-1",
            wall_seconds=3600.0,
            ns_per_day_by_system={"system_a": 50.0},
            output_valid=True,
        )
    ]

    pricing = MagicMock()
    pricing.get_products.return_value = {"PriceList": []}

    with patch("benchmarking_orchestration.analysis.boto3") as mock_boto3:
        mock_boto3.client.return_value = pricing
        print_results_table(records)

    captured = capsys.readouterr()
    assert "N/A" in captured.out


def test_cost_estimation_na_when_pricing_api_raises(capsys):
    records = [
        RunRecord(
            instance_type="g5.xlarge",
            region="us-east-1",
            wall_seconds=3600.0,
            ns_per_day_by_system={"system_a": 50.0},
            output_valid=True,
        )
    ]

    pricing = MagicMock()
    pricing.get_products.side_effect = Exception("API error")

    with patch("benchmarking_orchestration.analysis.boto3") as mock_boto3:
        mock_boto3.client.return_value = pricing
        print_results_table(records)

    captured = capsys.readouterr()
    assert "N/A" in captured.out


# ---------------------------------------------------------------------------
# print_results_table tests
# ---------------------------------------------------------------------------


def test_print_results_table_contains_expected_strings(capsys):
    records = [
        RunRecord(
            instance_type="g5.xlarge",
            region="us-east-1",
            wall_seconds=1800.0,
            ns_per_day_by_system={"system_a": 42.0},
            output_valid=True,
        )
    ]

    pricing = MagicMock()
    pricing.get_products.return_value = _make_pricing_response("1.006")

    with patch("benchmarking_orchestration.analysis.boto3") as mock_boto3:
        mock_boto3.client.return_value = pricing
        print_results_table(records)

    captured = capsys.readouterr()
    assert "g5.xlarge" in captured.out
    assert "42.00" in captured.out
    assert "00:30:00" in captured.out


def test_print_results_table_no_records(capsys):
    with patch("benchmarking_orchestration.analysis.boto3"):
        print_results_table([])

    captured = capsys.readouterr()
    assert "No benchmark results found." in captured.out


def test_print_results_table_na_ns_per_day_when_no_valid_output(capsys):
    records = [
        RunRecord(
            instance_type="g5.xlarge",
            region="us-east-1",
            wall_seconds=3600.0,
            ns_per_day_by_system={},
            output_valid=False,
        )
    ]

    pricing = MagicMock()
    pricing.get_products.return_value = {"PriceList": []}

    with patch("benchmarking_orchestration.analysis.boto3") as mock_boto3:
        mock_boto3.client.return_value = pricing
        print_results_table(records)

    captured = capsys.readouterr()
    assert "N/A" in captured.out
