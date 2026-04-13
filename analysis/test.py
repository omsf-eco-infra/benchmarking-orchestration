import marimo

__generated_with = "0.22.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb
    import polars as pl
    import boto3
    import json
    import os
    import sqlalchemy as sqla

    conn = duckdb.connect()
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")

    conn.execute("""
        CREATE OR REPLACE SECRET s3_secret (
            TYPE s3,
            PROVIDER credential_chain
        );
    """)

    pricing = boto3.client("pricing", region_name="us-east-1")

    def get_ondemand_hourly_usd(
        instance_type: str, region_code: str = "us-east-1"
    ) -> float | None:
        # Pricing API filters on human-readable location, not region code
        region_name_map = {
            "us-east-1": "US East (N. Virginia)",
            "us-east-2": "US East (Ohio)",
            "us-west-1": "US West (N. California)",
            "us-west-2": "US West (Oregon)",
        }
        location = region_name_map[region_code]

        resp = pricing.get_products(
            ServiceCode="AmazonEC2",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
                {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
                {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
            ],
            MaxResults=100,
        )

        for price_str in resp["PriceList"]:
            item = json.loads(price_str)

            terms = item.get("terms", {}).get("OnDemand", {})
            for _, term in terms.items():
                price_dimensions = term.get("priceDimensions", {})
                for _, dim in price_dimensions.items():
                    unit = dim.get("unit")
                    price = dim.get("pricePerUnit", {}).get("USD")
                    if unit == "Hrs" and price is not None:
                        return float(price)

        return None

    # Example: either hardcode, or derive from a query result / existing dataframe
    instance_types = ["g4dn.xlarge", "g5.xlarge", "g6e.xlarge"]

    prices_df = pl.DataFrame(
        [
            {
                "instance_type": it,
                "price_per_hour": get_ondemand_hourly_usd(it, "us-east-1"),
            }
            for it in instance_types
        ]
    )

    conn.register("instance_prices_df", prices_df)
    conn.execute(
        "CREATE OR REPLACE TEMP VIEW instance_prices AS SELECT * FROM instance_prices_df"
    )
    turso_database_url = os.getenv("TURSO_DATABASE_URL")
    turso_auth_token = os.getenv("TURSO_AUTH_TOKEN")
    libsql = sqla.create_engine(
        f"sqlite+{turso_database_url}?secure=true",
        connect_args={"auth_token": turso_auth_token},
    )

    prices_df
    return conn, mo


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Ranking
    """)
    return


@app.cell(hide_code=True)
def _(conn):
    conn.execute(
        r"""
        CREATE OR REPLACE TEMP VIEW md_outputs_raw AS
        SELECT
            regexp_extract(filename, 'runs/([0-9]{4}-[0-9]{2}-[0-9]{2})/', 1) AS run_date,
            regexp_extract(filename, 'runs/([^/]+)/([^/]+)/output/md_benchmark\.out$', 2) AS run_id,
            bace,
            p38,
            jnk1,
            cdk2,
            ptp1b,
            tyk2,
            mcl1,
            thrombin
        FROM read_json(
            's3://benchmark-bucket-omsf-2026/runs/2026-03-*/**/output/md_benchmark.out'
        );
        """
    )

    conn.execute(
        r"""
        CREATE OR REPLACE TEMP VIEW md_manifests_raw AS
        SELECT
            regexp_extract(filename, 'runs/([^/]+)/([^/]+)/manifest\.json$', 2) AS run_id,
            split_part(bench_task_id, ':', 3) AS instance_type,
            split_part(bench_task_id, ':', 4) AS ami
        FROM read_json(
            's3://benchmark-bucket-omsf-2026/runs/2026-03-*/**/manifest.json'
        );
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TEMP VIEW md_metrics_long AS
        SELECT
            run_date,
            run_id,
            regexp_replace(metric_name, '_ns_per_day$', '') AS benchmark,
            ns_per_day
        FROM (
            SELECT
                run_date,
                run_id,
                bace AS bace_ns_per_day,
                p38 AS p38_ns_per_day,
                jnk1 AS jnk1_ns_per_day,
                cdk2 AS cdk2_ns_per_day,
                ptp1b AS ptp1b_ns_per_day,
                tyk2 AS tyk2_ns_per_day,
                mcl1 AS mcl1_ns_per_day,
                thrombin AS thrombin_ns_per_day
            FROM md_outputs_raw
        )
        UNPIVOT (
            ns_per_day FOR metric_name IN (
                bace_ns_per_day,
                p38_ns_per_day,
                jnk1_ns_per_day,
                cdk2_ns_per_day,
                ptp1b_ns_per_day,
                tyk2_ns_per_day,
                mcl1_ns_per_day,
                thrombin_ns_per_day
            )
        );
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE benchmark_costs AS
        SELECT
            'md' AS benchmark_type,
            l.run_date,
            l.run_id,
            l.benchmark,
            'ns_per_day' AS metric_name,
            l.ns_per_day AS metric_value,
            m.instance_type,
            m.ami,
            p.price_per_hour,
            (p.price_per_hour * 24.0) / NULLIF(l.ns_per_day, 0) AS dollars_per_ns,
            l.ns_per_day / NULLIF((p.price_per_hour * 24.0), 0) AS ns_per_dollar
        FROM md_metrics_long l
        JOIN md_manifests_raw m USING (run_id)
        LEFT JOIN instance_prices p USING (instance_type);
        """
    )
    return


@app.cell(hide_code=True)
def _(benchmark_costs, conn, mo):
    median_md_cost = mo.sql(
        f"""
        SELECT 
            instance_type,
            benchmark,
            median(metric_value) AS metric_value,
            median(ns_per_dollar) as ns_per_dollar
        FROM benchmark_costs
        WHERE benchmark_type = 'md' 
        GROUP BY 1, 2
        ORDER BY metric_value DESC;
        """,
        engine=conn,
    )
    return (median_md_cost,)


@app.cell
def _():
    import altair as alt

    return (alt,)


@app.cell
def _(alt, median_md_cost):

    _chart = (
        alt.Chart(median_md_cost)
        .transform_calculate(
            # round a bit so near-equal floats can tie
            ns_per_dollar_rounded="round(datum.ns_per_dollar, 2)"
        )
        .transform_window(
            rank="dense_rank(ns_per_dollar_rounded)",
            groupby=["benchmark"],
            sort=[alt.SortField("ns_per_dollar_rounded", order="descending")],
        )
        .mark_bar()
        .encode(
            x=alt.X(
                "instance_type:N",
                title="instance type",
                sort=["g4dn.xlarge", "g5.xlarge", "g6e.xlarge"],
                axis=alt.Axis(labelAngle=-45),
            ),
            y=alt.Y(
                "ns_per_dollar:Q",
                title="ns per dollar",
            ),
            color=alt.condition(
                alt.datum.rank == 1,
                alt.Color("instance_type:N", legend=None),
                alt.value("lightgray"),
            ),
            tooltip=[
                alt.Tooltip("benchmark:N"),
                alt.Tooltip("instance_type:N"),
                alt.Tooltip("ns_per_dollar:Q", format=",.2f"),
                alt.Tooltip("rank:Q", title="rank"),
            ],
        )
        .facet(
            column=alt.Column(
                "benchmark:N",
                title="benchmark",
                header=alt.Header(labelOrient="bottom"),
            )
        )
        .properties(
            height=260,
            width=120,
        )
        .configure_axis(grid=False)
    )

    _chart
    return


@app.cell
def _(conn, mo):
    df = mo.sql(
        f"""
        WITH outputs AS (
            SELECT
                filename AS output_filename,
                regexp_extract(
                    filename,
                    'runs/([^/]+)/([^/]+)/output/md_benchmark\\.out$',
                    2
                ) AS run_id,
                *
            FROM read_json(
                's3://benchmark-bucket-omsf-2026/runs/2026-03-*/**/output/md_benchmark.out'
            )
        ),
        manifests AS (
            SELECT
                filename AS manifest_filename,
                regexp_extract(
                    filename,
                    'runs/([^/]+)/([^/]+)/manifest\\.json$',
                    2
                ) AS run_id,
            	split_part(bench_task_id, ':', 4) AS ami,
            	split_part(bench_task_id, ':', 3) AS instance_type
            FROM read_json(
                's3://benchmark-bucket-omsf-2026/runs/2026-03-*/**/manifest.json'
            )
        )
        SELECT
            SPLIT_PART(m.manifest_filename, '/', -3) as date,
            o.* EXCLUDE (output_filename, run_id),
            m.* EXCLUDE (manifest_filename, run_id)
        FROM outputs o
        LEFT JOIN manifests m
            USING (run_id);
        """,
        engine=conn,
    )
    return


@app.cell(hide_code=True)
def _(conn):
    conn.execute(
        r"""
        CREATE OR REPLACE TABLE rbfe_benchmark_costs AS
        WITH manifests AS (
            SELECT
                regexp_extract(filename, 'runs/([^/]+)/([^/]+)/manifest\.json$', 2) AS run_id,
                split_part(bench_task_id, ':', 4) AS instance_type,
                split_part(bench_task_id, ':', 5) AS ami
            FROM read_json(
                's3://benchmark-bucket-omsf-2026/runs/2026-04-*/**/manifest.json'
            )
        ),
        outputs AS (
            SELECT
                regexp_extract(filename, 'runs/([0-9]{4}-[0-9]{2}-[0-9]{2})/', 1) AS run_date,
                regexp_extract(filename, 'runs/([^/]+)/([^/]+)/output/rbfe_benchmark\.out$', 2) AS run_id,
                coalesce(try_cast(bace ->> '$.rbfe' AS DOUBLE),     try_cast(bace ->> '$.complex' AS DOUBLE))     AS bace_rbfe_complex,
                coalesce(try_cast(bace ->> '$.mm' AS DOUBLE),       try_cast(bace ->> '$.solvent' AS DOUBLE))     AS bace_rbfe_solvent,
                coalesce(try_cast(p38 ->> '$.rbfe' AS DOUBLE),      try_cast(p38 ->> '$.complex' AS DOUBLE))      AS p38_rbfe_complex,
                coalesce(try_cast(p38 ->> '$.mm' AS DOUBLE),        try_cast(p38 ->> '$.solvent' AS DOUBLE))      AS p38_rbfe_solvent,
                coalesce(try_cast(jnk1 ->> '$.rbfe' AS DOUBLE),     try_cast(jnk1 ->> '$.complex' AS DOUBLE))     AS jnk1_rbfe_complex,
                coalesce(try_cast(jnk1 ->> '$.mm' AS DOUBLE),       try_cast(jnk1 ->> '$.solvent' AS DOUBLE))     AS jnk1_rbfe_solvent,
                coalesce(try_cast(cdk2 ->> '$.rbfe' AS DOUBLE),     try_cast(cdk2 ->> '$.complex' AS DOUBLE))     AS cdk2_rbfe_complex,
                coalesce(try_cast(cdk2 ->> '$.mm' AS DOUBLE),       try_cast(cdk2 ->> '$.solvent' AS DOUBLE))     AS cdk2_rbfe_solvent,
                coalesce(try_cast(ptp1b ->> '$.rbfe' AS DOUBLE),    try_cast(ptp1b ->> '$.complex' AS DOUBLE))    AS ptp1b_rbfe_complex,
                coalesce(try_cast(ptp1b ->> '$.mm' AS DOUBLE),      try_cast(ptp1b ->> '$.solvent' AS DOUBLE))    AS ptp1b_rbfe_solvent,
                coalesce(try_cast(tyk2 ->> '$.rbfe' AS DOUBLE),     try_cast(tyk2 ->> '$.complex' AS DOUBLE))     AS tyk2_rbfe_complex,
                coalesce(try_cast(tyk2 ->> '$.mm' AS DOUBLE),       try_cast(tyk2 ->> '$.solvent' AS DOUBLE))     AS tyk2_rbfe_solvent,
                coalesce(try_cast(mcl1 ->> '$.rbfe' AS DOUBLE),     try_cast(mcl1 ->> '$.complex' AS DOUBLE))     AS mcl1_rbfe_complex,
                coalesce(try_cast(mcl1 ->> '$.mm' AS DOUBLE),       try_cast(mcl1 ->> '$.solvent' AS DOUBLE))     AS mcl1_rbfe_solvent,
                coalesce(try_cast(thrombin ->> '$.rbfe' AS DOUBLE), try_cast(thrombin ->> '$.complex' AS DOUBLE)) AS thrombin_rbfe_complex,
                coalesce(try_cast(thrombin ->> '$.mm' AS DOUBLE),   try_cast(thrombin ->> '$.solvent' AS DOUBLE)) AS thrombin_rbfe_solvent
            FROM read_json(
                's3://benchmark-bucket-omsf-2026/runs/2026-04-*/**/output/rbfe_benchmark.out'
            )
        ),
        metrics_long AS (
            SELECT
                run_date,
                run_id,
                metric_name AS benchmark_phase,
                ns_per_day
            FROM outputs
            UNPIVOT (
                ns_per_day FOR metric_name IN (
                    bace_rbfe_complex,
                    bace_rbfe_solvent,
                    p38_rbfe_complex,
                    p38_rbfe_solvent,
                    jnk1_rbfe_complex,
                    jnk1_rbfe_solvent,
                    cdk2_rbfe_complex,
                    cdk2_rbfe_solvent,
                    ptp1b_rbfe_complex,
                    ptp1b_rbfe_solvent,
                    tyk2_rbfe_complex,
                    tyk2_rbfe_solvent,
                    mcl1_rbfe_complex,
                    mcl1_rbfe_solvent,
                    thrombin_rbfe_complex,
                    thrombin_rbfe_solvent
                )
            )
        )
        SELECT
            'rbfe' AS benchmark_type,
            l.run_date,
            l.run_id,
            split_part(l.benchmark_phase, '_', 1) AS benchmark,
            split_part(l.benchmark_phase, '_', 3) AS type,
            'ns_per_day' AS metric_name,
            l.ns_per_day AS metric_value,
            m.instance_type,
            m.ami,
            p.price_per_hour,
            (p.price_per_hour * 24.0) / NULLIF(l.ns_per_day, 0) AS dollars_per_ns,
            l.ns_per_day / NULLIF(p.price_per_hour * 24.0, 0) AS ns_per_dollar
        FROM metrics_long l
        JOIN manifests m USING (run_id)
        LEFT JOIN instance_prices p USING (instance_type);
        """
    )
    return


@app.cell(hide_code=True)
def _(conn, mo):
    rbfe_benchmark_costs = mo.sql(
        f"""
        SELECT 
            instance_type,
            benchmark,
            median(metric_value) AS ns_per_day,
            median(ns_per_dollar) as ns_per_dollar
        FROM rbfe_benchmark_costs
        WHERE benchmark_type = 'rbfe' and type = 'complex'
        GROUP BY 1, 2
        ORDER BY ns_per_dollar DESC;
        """,
        engine=conn,
    )
    return (rbfe_benchmark_costs,)


@app.cell
def _(alt, rbfe_benchmark_costs):
    chart_new = (
        alt.Chart(rbfe_benchmark_costs)
        .transform_calculate(
            # round to 2 decimals so near-equal values can tie
            ns_per_day_rounded="round(datum.ns_per_day * 100) / 100"
        )
        .transform_window(
            rank="dense_rank()",
            groupby=["benchmark"],
            sort=[alt.SortField("ns_per_day_rounded", order="descending")],
        )
        .mark_bar()
        .encode(
            x=alt.X(
                "instance_type:N",
                title="instance type",
                sort=["g4dn.xlarge", "g5.xlarge", "g6e.xlarge"],
                axis=alt.Axis(labelAngle=-45),
            ),
            y=alt.Y(
                "ns_per_day:Q",
                title="ns per day",
            ),
            color=alt.condition(
                alt.datum.rank == 1,
                alt.Color("instance_type:N", legend=None),
                alt.value("lightgray"),
            ),
            tooltip=[
                alt.Tooltip("benchmark:N"),
                alt.Tooltip("instance_type:N"),
                alt.Tooltip("ns_per_day:Q", format=",.2f"),
                alt.Tooltip("rank:Q", title="rank"),
            ],
        )
        .facet(
            column=alt.Column(
                "benchmark:N",
                title="benchmark",
                header=alt.Header(labelOrient="bottom"),
            )
        )
        .properties(
            height=260,
            width=120,
        )
        .configure_axis(grid=False)
    )

    chart_new
    return


@app.cell(hide_code=True)
def _(alt, rbfe_benchmark_costs):
    chart = (
        alt.Chart(rbfe_benchmark_costs)
        .transform_calculate(
            # round to 2 decimals so near-equal values can tie
            ns_per_dollar_rounded="round(datum.ns_per_dollar * 100) / 100"
        )
        .transform_window(
            rank="dense_rank()",
            groupby=["benchmark"],
            sort=[alt.SortField("ns_per_dollar_rounded", order="descending")],
        )
        .mark_bar()
        .encode(
            x=alt.X(
                "instance_type:N",
                title="instance type",
                sort=["g4dn.xlarge", "g5.xlarge", "g6e.xlarge"],
                axis=alt.Axis(labelAngle=-45),
            ),
            y=alt.Y(
                "ns_per_dollar:Q",
                title="ns per dollar",
            ),
            color=alt.condition(
                alt.datum.rank == 1,
                alt.Color("instance_type:N", legend=None),
                alt.value("lightgray"),
            ),
            tooltip=[
                alt.Tooltip("benchmark:N"),
                alt.Tooltip("instance_type:N"),
                alt.Tooltip("ns_per_dollar:Q", format=",.2f"),
                alt.Tooltip("rank:Q", title="rank"),
            ],
        )
        .facet(
            column=alt.Column(
                "benchmark:N",
                title="RBFE Performance (ns/dollar)",
                header=alt.Header(labelOrient="bottom"),
            )
        )
        .properties(
            height=260,
            width=120,
        )
        .configure_axis(grid=False)
    )

    chart
    return


if __name__ == "__main__":
    app.run()
