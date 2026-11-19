"""Distributed reconciliation: the same algorithm and the same report as ``reconcile_rows``.

Typical use on Databricks: source = ``spark.read.jdbc(...)`` (SQL Server, Oracle, Teradata) or a
Hive table, target = the migrated Delta table. Both sides are canonicalized with identical Spark
expressions, so the comparison is type-aware without moving data to the driver. Only the
fingerprints of N buckets and the rows of mismatched buckets are collected.
Run with ``spark.sql.session.timeZone=UTC`` so timestamps compare as instants.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from decimal import Decimal

from migrationkit.normalize import NULL, SEP, Normalization, diagnose
from migrationkit.reconcile import ColumnDiff, ReconReport, _fmt, _show


def _strip_zeros(s):
    from pyspark.sql import functions as F

    stripped = F.regexp_replace(F.regexp_replace(s, "0+$", ""), "\\.$", "")
    out = F.when(s.contains("."), stripped).otherwise(s)
    return F.when(out == "-0", F.lit("0")).otherwise(out)


def canonical_expr(col: str, dtype, norm: Normalization):
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    c = F.col(f"`{col}`")
    if isinstance(dtype, T.StringType):
        e = F.rtrim(c) if norm.rtrim_strings else c
        if norm.empty_string_as_null:
            e = F.when(e == "", F.lit(None)).otherwise(e)
        if norm.case_insensitive:
            e = F.lower(e)
    elif isinstance(dtype, T.BooleanType):
        e = F.lower(c.cast("string"))
    elif isinstance(dtype, (T.ByteType, T.ShortType, T.IntegerType, T.LongType)):
        e = c.cast("string")
    elif isinstance(dtype, (T.FloatType, T.DoubleType)):
        d = norm.float_decimals
        num = _strip_zeros(F.round(c.cast("double"), d).cast(f"decimal(38,{d})").cast("string"))
        e = F.when(F.isnan(c), F.lit("NaN")).otherwise(num)
    elif isinstance(dtype, T.DecimalType):
        e = _strip_zeros(c.cast("string"))
    elif isinstance(dtype, T.TimestampType):
        unit = {"s": "second", "ms": "millisecond", "us": "microsecond"}[norm.timestamp_precision]
        e = F.date_format(F.date_trunc(unit, c), "yyyy-MM-dd HH:mm:ss.SSSSSS")
    elif isinstance(dtype, T.DateType):
        e = F.date_format(c, "yyyy-MM-dd")
    elif isinstance(dtype, T.BinaryType):
        e = F.lower(F.hex(c))
    elif isinstance(dtype, (T.StructType, T.ArrayType, T.MapType)):
        e = F.to_json(c)
    else:
        e = c.cast("string")
    return F.coalesce(e, F.lit(NULL))


def reconcile_spark(
    table: str,
    source_df,
    target_df,
    keys: Sequence[str],
    columns: Sequence[str] | None = None,
    *,
    numeric_columns: Sequence[str] = (),
    norm: Normalization | None = None,
    n_buckets: int = 256,
    sample_size: int = 5,
    diagnose_rows: int = 500,
) -> ReconReport:
    from pyspark.sql import functions as F

    norm = norm or Normalization()
    cols = [c for c in (columns or source_df.columns) if c not in keys and c not in norm.ignore_columns]

    def prep(df):
        canon = [canonical_expr(c, df.schema[c].dataType, norm).alias(f"c{i}") for i, c in enumerate(cols)]
        key = F.concat_ws(SEP, *[canonical_expr(k, df.schema[k].dataType, norm) for k in keys])
        out = df.select(key.alias("__k"), *canon)
        return (out.withColumn("__h", F.xxhash64(F.concat_ws(SEP, *[f"c{i}" for i in range(len(cols))])))
                .withColumn("__b", F.pmod(F.xxhash64("__k"), F.lit(n_buckets))))

    s, t = prep(source_df), prep(target_df)
    num_idx = {c: cols.index(c) for c in numeric_columns}

    def summarize(df):
        aggs = [F.count("*").alias("n"), F.sum(F.col("__h").cast("decimal(38,0)")).alias("h")]
        totals = df.agg(F.count("*").alias("rows"), *[
            F.sum(F.when(F.col(f"c{i}") != NULL, F.col(f"c{i}")).cast("decimal(38,10)")).alias(f"t{i}")
            for i in num_idx.values()]).first()
        buckets = {r["__b"]: (r["n"], r["h"]) for r in df.groupBy("__b").agg(*aggs).collect()}
        return buckets, totals

    sb, stot = summarize(s)
    tb, ttot = summarize(t)
    bad = sorted(b for b in set(sb) | set(tb) if sb.get(b) != tb.get(b))
    rep = ReconReport(table, stot["rows"], ttot["rows"], n_buckets, len(bad), normalization=norm.describe())
    rep.control_totals = {
        c: (_fmt(Decimal(stot[f"t{i}"] or 0)), _fmt(Decimal(ttot[f"t{i}"] or 0))) for c, i in num_idx.items()
    }
    if not bad:
        return rep

    sbad, tbad = s.where(F.col("__b").isin(bad)), t.where(F.col("__b").isin(bad))
    rep.duplicate_keys_source = sbad.groupBy("__k").count().where("count > 1").select(F.sum(F.col("count") - 1)).first()[0] or 0
    rep.duplicate_keys_target = tbad.groupBy("__k").count().where("count > 1").select(F.sum(F.col("count") - 1)).first()[0] or 0
    j = sbad.dropDuplicates(["__k"]).alias("s").join(tbad.dropDuplicates(["__k"]).alias("t"), "__k", "full_outer")
    j = j.withColumn("__state", F.when(F.col("s.__h").isNull(), "extra").when(F.col("t.__h").isNull(), "missing")
                     .when(F.col("s.__h") != F.col("t.__h"), "changed").otherwise("same")).cache()
    try:
        counts = {r["__state"]: r["count"] for r in j.groupBy("__state").count().collect()}
        rep.missing_in_target = counts.get("missing", 0)
        rep.extra_in_target = counts.get("extra", 0)
        rep.changed = counts.get("changed", 0)
        rep.samples = {
            st: [_show(r["__k"]) for r in j.where(F.col("__state") == st).select("__k").orderBy("__k")
                 .limit(sample_size).collect()]
            for st in ("missing", "extra")
        }
        changed = j.where(F.col("__state") == "changed")
        if rep.changed:
            per_col = changed.agg(*[
                F.sum(F.when(~F.col(f"s.c{i}").eqNullSafe(F.col(f"t.c{i}")), 1).otherwise(0)).alias(f"d{i}")
                for i in range(len(cols))]).first()
            sample = changed.select("__k", *[F.col(f"s.c{i}").alias(f"s{i}") for i in range(len(cols))],
                                    *[F.col(f"t.c{i}").alias(f"t{i}") for i in range(len(cols))]
                                    ).limit(diagnose_rows).collect()
            for i, c in enumerate(cols):
                if not per_col[f"d{i}"]:
                    continue
                d = ColumnDiff(c, per_col[f"d{i}"])
                causes: Counter[str] = Counter()
                for r in sample:
                    if r[f"s{i}"] != r[f"t{i}"]:
                        causes[diagnose(r[f"s{i}"], r[f"t{i}"])] += 1
                        if len(d.samples) < sample_size:
                            d.samples.append((_show(r["__k"]), r[f"s{i}"], r[f"t{i}"]))
                d.diagnosis = causes.most_common(1)[0][0] if causes else "value differs"
                rep.column_diffs[c] = d
    finally:
        j.unpersist()
    return rep
