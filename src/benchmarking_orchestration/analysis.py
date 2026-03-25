from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import boto3

from .benchmark_kind import BenchmarkKind, _normalize_benchmark_kind
from .task_id import _parse_launch_task_id


@dataclass
class RunRecord:
    """A single benchmark run result extracted from an S3 manifest.

    Attributes
    ----------
    instance_type : str
        EC2 instance type used for this run.
    region : str
        AWS region where the run executed.
    wall_seconds : float
        Elapsed wall-clock time in seconds.
    ns_per_day_by_system : dict[str, float]
        Mapping of system name to ns/day performance metric.
    output_valid : bool
        Whether the output JSON was successfully parsed.
    benchmark_kind : str
        Benchmark workload kind recorded in the manifest.
    """

    instance_type: str
    region: str
    wall_seconds: float
    ns_per_day_by_system: dict[str, float] = field(default_factory=dict)
    output_valid: bool = False
    benchmark_kind: str = BenchmarkKind.MD.value


def _parse_utc_timestamp(ts: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp string into a timezone-aware datetime.

    Parameters
    ----------
    ts : str
        Timestamp string, e.g. ``"2024-01-15T12:00:00Z"``.

    Returns
    -------
    datetime
        Timezone-aware UTC datetime.
    """
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def _get_on_demand_price_usd_per_hr(
    instance_type: str,
    pricing_client: Any,
    cache: dict[str, float | None],
) -> float | None:
    """Fetch the on-demand USD/hr price for an EC2 instance type.

    Parameters
    ----------
    instance_type : str
        EC2 instance type, e.g. ``"g5.xlarge"``.
    pricing_client : Any
        Boto3 ``pricing`` client (``us-east-1`` endpoint).
    cache : dict[str, float | None]
        Mutable dict used to cache results within a single call to avoid
        redundant API calls for the same instance type.

    Returns
    -------
    float or None
        On-demand price in USD per hour, or ``None`` if unavailable.
    """
    if instance_type in cache:
        return cache[instance_type]

    try:
        response = pricing_client.get_products(
            ServiceCode="AmazonEC2",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
                {
                    "Type": "TERM_MATCH",
                    "Field": "location",
                    "Value": "US East (N. Virginia)",
                },
                {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
            ],
            MaxResults=1,
        )
        price_list = response.get("PriceList", [])
        if not price_list:
            cache[instance_type] = None
            return None

        product = json.loads(price_list[0])
        terms = product.get("terms", {}).get("OnDemand", {})
        if not terms:
            cache[instance_type] = None
            return None

        # Navigate into first term → first price dimension
        term = next(iter(terms.values()))
        price_dimensions = term.get("priceDimensions", {})
        dimension = next(iter(price_dimensions.values()))
        price_per_unit = float(dimension["pricePerUnit"]["USD"])
        cache[instance_type] = price_per_unit
        return price_per_unit
    except Exception:
        cache[instance_type] = None
        return None


def fetch_and_analyze_results(
    s3_bucket: str,
    s3_prefix: str = "runs/",
) -> list[RunRecord]:
    """Fetch all benchmark manifests from S3 and parse them into RunRecords.

    Parameters
    ----------
    s3_bucket : str
        S3 bucket containing benchmark results.
    s3_prefix : str, default="runs/"
        S3 key prefix to search under.

    Returns
    -------
    list[RunRecord]
        Parsed run records, one per successfully fetched manifest.
    """
    s3 = boto3.client("s3")
    records: list[RunRecord] = []

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)

    manifest_keys: list[str] = []
    for page in pages:
        for obj in page.get("Contents", []):
            key: str = obj["Key"]
            if key.endswith("/manifest.json"):
                manifest_keys.append(key)

    for manifest_key in manifest_keys:
        try:
            obj = s3.get_object(Bucket=s3_bucket, Key=manifest_key)
            manifest = json.loads(obj["Body"].read().decode("utf-8"))
        except Exception:
            continue

        launch_task_id = manifest.get("launch_task_id", "")
        try:
            region, instance_type, _ami_id, _cloud_init = _parse_launch_task_id(
                launch_task_id
            )
        except ValueError:
            continue

        timestamps = manifest.get("timestamps", {})
        started_str = timestamps.get("started_at_utc")
        completed_str = timestamps.get("completed_at_utc")
        if not started_str or not completed_str:
            continue

        try:
            started_at = _parse_utc_timestamp(started_str)
            completed_at = _parse_utc_timestamp(completed_str)
            wall_seconds = (completed_at - started_at).total_seconds()
        except (ValueError, TypeError):
            continue

        output_info = manifest.get("output", {})
        benchmark_kind_raw = str(manifest.get("benchmark_kind", BenchmarkKind.MD.value))
        try:
            benchmark_kind = _normalize_benchmark_kind(benchmark_kind_raw).value
        except ValueError:
            benchmark_kind = BenchmarkKind.MD.value

        json_parse_ok = output_info.get("json_parse_ok", False)
        output_s3_key = output_info.get("s3_key")

        ns_per_day_by_system: dict[str, float] = {}
        output_valid = False

        if json_parse_ok and output_s3_key:
            try:
                out_obj = s3.get_object(Bucket=s3_bucket, Key=output_s3_key)
                output_payload = json.loads(out_obj["Body"].read().decode("utf-8"))
                if isinstance(output_payload, dict):
                    ns_per_day_by_system = {
                        k: float(v)
                        for k, v in output_payload.items()
                        if isinstance(v, (int, float))
                    }
                    output_valid = True
            except Exception:
                pass

        records.append(
            RunRecord(
                instance_type=instance_type,
                region=region,
                wall_seconds=wall_seconds,
                ns_per_day_by_system=ns_per_day_by_system,
                output_valid=output_valid,
                benchmark_kind=benchmark_kind,
            )
        )

    return records


def print_results_table(records: list[RunRecord]) -> None:
    """Print a summary table of benchmark results grouped by instance type.

    Parameters
    ----------
    records : list[RunRecord]
        Parsed run records to summarize.
    """
    if not records:
        print("No benchmark results found.")
        return

    # Group records by instance type
    by_instance: dict[str, list[RunRecord]] = {}
    for record in records:
        by_instance.setdefault(record.instance_type, []).append(record)

    pricing_client = boto3.client("pricing", region_name="us-east-1")
    price_cache: dict[str, float | None] = {}

    header = f"{'Instance Type':<20} {'Runs':>5} {'Mean ns/day':>14} {'Mean Wall Time':>16} {'Est. Cost':>12}"
    print(header)
    print("-" * len(header))

    for instance_type in sorted(by_instance):
        runs = by_instance[instance_type]
        run_count = len(runs)

        # Mean wall-clock time
        mean_wall = sum(r.wall_seconds for r in runs) / run_count
        hours = int(mean_wall // 3600)
        minutes = int((mean_wall % 3600) // 60)
        seconds = int(mean_wall % 60)
        wall_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        # Mean ns/day across all systems and all runs with valid output
        valid_runs = [r for r in runs if r.output_valid and r.ns_per_day_by_system]
        if valid_runs:
            all_values = [
                v for r in valid_runs for v in r.ns_per_day_by_system.values()
            ]
            mean_ns_per_day = sum(all_values) / len(all_values)
            ns_per_day_str = f"{mean_ns_per_day:.2f}"
        else:
            ns_per_day_str = "N/A"

        # Estimated cost
        price = _get_on_demand_price_usd_per_hr(
            instance_type, pricing_client, price_cache
        )
        if price is not None:
            mean_cost = (mean_wall / 3600.0) * price
            cost_str = f"${mean_cost:.4f}"
        else:
            cost_str = "N/A"

        print(
            f"{instance_type:<20} {run_count:>5} {ns_per_day_str:>14} {wall_str:>16} {cost_str:>12}"
        )
