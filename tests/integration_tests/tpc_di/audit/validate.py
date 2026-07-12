"""Audit a TPC-DI warehouse (local dir or OneLake lakehouse) — real data validation.

This is NOT a "does the table exist" smoke test. For every source→target transition it
re-derives the expected value FROM THE SOURCE FILES and asserts the finished warehouse
matches, then checks the structural invariants a correct dimensional load must hold. It is a
DuckDB port of the reference project's single-batch ``data_validation.sql`` (end-to-end
pipeline-integrity checks), adapted to our column names.

Two kinds of check:
  • Reconciliation — a value computed from the raw Batch1/2/3 source files (e.g. distinct
    customers in CustomerMgmt(NEW) + Batch2/3 Customer.txt CDC inserts) must equal (or, for
    SCD2-split facts, be covered by) the warehouse table. Because the expected value is
    DERIVED from the same source files the warehouse was built from, the audit is
    scale-factor-agnostic: point it at the sf<N> the warehouse was loaded from and it
    self-calibrates. Nothing is hard-coded per SF.
  • Invariant — a property that must hold at any scale: no NULL surrogate keys; SCD2 has no
    overlapping/zero-length windows, exactly one open version per business key, and
    IsCurrent consistent with EndDate; foreign-key surrogate keys land in a parent version
    whose date range contains the child's; enumerated columns hold only legal values.

It runs through duckrun's ``connect()`` so it works identically over a local directory and an
``abfss://`` OneLake lakehouse: duckrun registers each Delta table as a queryable view and, on
OneLake, installs the storage secret from ``ONELAKE_TOKEN`` — which also lets us ``read_csv``
the raw source files straight from ``Files/`` over ``abfss://``.

Usage:
    python validate.py --warehouse <root>/Tables --schema tpcdi --source-dir <.../sf3>
"""
from __future__ import annotations

import argparse
import os
import sys

import duckrun

# Final tables the load must produce (also the liveness summary at the top of the report).
REQUIRED = [
    "DimDate", "DimTime", "DimBroker", "DimCompany", "DimSecurity", "Financial",
    "DimCustomer", "DimAccount", "DimTrade", "Prospect",
    "FactCashBalances", "FactHoldings", "FactWatches", "FactMarketHistory",
]

# Reconciliation checks whose SOURCE value is derived purely from the raw source files
# (no dependency on a dbt-built staging table). These are the expensive ones over abfss —
# scanning tens of millions of raw rows — so scripts/summarize_seed.py precomputes them on
# local disk at generation time and the audit reads them from _seed_summary.json instead.
# The other five (DimCustomer/DimAccount via stg_customermgmt, DimCompany/DimSecurity/
# Financial via FinWire) read small/moderate Delta tables, so the audit computes them live.
RAW_SOURCE_CHECKS = {
    "DimBroker", "DimTrade", "FactCashBalances", "FactHoldings",
    "FactWatches", "FactMarketHistory", "Prospect",
}

# Column layouts of the raw source files (positional, headerless), matching macros/tpcdi.sql.
# Batch1 (historical) files have no CDC prefix; Batch2/3 (incremental) prepend cdc_flag, cdc_dsn.
_CUST_INC = ("cdc_flag VARCHAR, cdc_dsn BIGINT, c_id BIGINT, c_tax_id VARCHAR, c_st_id VARCHAR, "
             "c_l_name VARCHAR, c_f_name VARCHAR, c_m_name VARCHAR, c_gndr VARCHAR, c_tier VARCHAR, "
             "c_dob VARCHAR, c_adline1 VARCHAR, c_adline2 VARCHAR, c_zipcode VARCHAR, c_city VARCHAR, "
             "c_state_prov VARCHAR, c_ctry VARCHAR, c_ctry_1 VARCHAR, c_area_1 VARCHAR, c_local_1 VARCHAR, "
             "c_ext_1 VARCHAR, c_ctry_2 VARCHAR, c_area_2 VARCHAR, c_local_2 VARCHAR, c_ext_2 VARCHAR, "
             "c_ctry_3 VARCHAR, c_area_3 VARCHAR, c_local_3 VARCHAR, c_ext_3 VARCHAR, c_email_1 VARCHAR, "
             "c_email_2 VARCHAR, c_lcl_tx_id VARCHAR, c_nat_tx_id VARCHAR")
_ACCT_INC = ("cdc_flag VARCHAR, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT, customerid BIGINT, "
             "accountdesc VARCHAR, taxstatus TINYINT, status VARCHAR")
_TRADE_B1 = ("t_id BIGINT, t_dts VARCHAR, t_st_id VARCHAR, t_tt_id VARCHAR, t_is_cash VARCHAR, "
             "t_s_symb VARCHAR, t_qty INT, t_bid_price DOUBLE, t_ca_id BIGINT, t_exec_name VARCHAR, "
             "t_trade_price DOUBLE, t_chrg DOUBLE, t_comm DOUBLE, t_tax DOUBLE")
_TRADE_INC = ("cdc_flag VARCHAR, cdc_dsn BIGINT, tradeid BIGINT, t_dts VARCHAR, status VARCHAR, "
              "t_tt_id VARCHAR, cashflag TINYINT, t_s_symb VARCHAR, quantity INT, bidprice DOUBLE, "
              "t_ca_id BIGINT, executedby VARCHAR, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE")
_CASH_B1 = "ct_ca_id BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name VARCHAR"
_CASH_INC = "cdc_flag VARCHAR, cdc_dsn BIGINT, ct_ca_id BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name VARCHAR"
_HOLD_B1 = "hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT"
_HOLD_INC = "cdc_flag VARCHAR, cdc_dsn BIGINT, hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT"
_WATCH_B1 = "w_c_id BIGINT, w_s_symb VARCHAR, w_dts VARCHAR, w_action VARCHAR"
_WATCH_INC = "cdc_flag VARCHAR, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb VARCHAR, w_dts VARCHAR, w_action VARCHAR"
_DM_B1 = "dm_date DATE, dm_s_symb VARCHAR, dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT"
_DM_INC = "cdc_flag VARCHAR, cdc_dsn BIGINT, dm_date DATE, dm_s_symb VARCHAR, dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT"
_HR = ("employeeid VARCHAR, managerid VARCHAR, employeefirstname VARCHAR, employeelastname VARCHAR, "
       "employeemi VARCHAR, employeejobcode INT, employeebranch VARCHAR, employeeoffice VARCHAR, employeephone VARCHAR")
_PROSPECT = ("agencyid VARCHAR, lastname VARCHAR, firstname VARCHAR, middleinitial VARCHAR, gender VARCHAR, "
             "addressline1 VARCHAR, addressline2 VARCHAR, postalcode VARCHAR, city VARCHAR, state VARCHAR, "
             "country VARCHAR, phone VARCHAR, income VARCHAR, numbercars INT, numberchildren INT, "
             "maritalstatus VARCHAR, age INT, creditrating INT, ownorrentflag VARCHAR, employer VARCHAR, "
             "numbercreditcards INT, networth INT")


def _read(src: str, glob: str, cols: str, delim: str) -> str:
    """A read_csv() over source file(s), matching macros/tpcdi.sql (unquoted, backslash-free)."""
    colstr = ", ".join(f"'{c.rsplit(' ', 1)[0]}': '{c.rsplit(' ', 1)[1]}'" for c in cols.split(", "))
    d = "chr(124)" if delim == "|" else f"'{delim}'"
    return (f"read_csv('{src}/{glob}', delim => {d}, header => false, quote => '', escape => '', "
            f"nullstr => '', null_padding => true, columns => {{{colstr}}})")


def build_checks(src: str):
    """Return (reconciliations, invariants). `src` is the sf<N> source dir (local or abfss://)."""
    # --- Reconciliations: (name, source-scalar SQL, target-scalar SQL, op, description) ---
    recon = [
        ("DimBroker",
         f"(select count(*) from {_read(src, 'Batch1/HR.csv', _HR, ',')} where employeejobcode = 314)",
         'select count(*) from "DimBroker"', "==",
         "Every HR.csv broker (jobcode 314) appears in DimBroker"),

        ("DimCustomer distinct",
         "select count(distinct customerid) from ("
         "  select customerid from \"stg_customermgmt\" where actiontype = 'NEW'"
         f"  union select c_id as customerid from {_read(src, 'Batch[23]/Customer.txt', _CUST_INC, '|')} where cdc_flag = 'I')",
         'select count(distinct customerid) from "DimCustomer"', "==",
         "Every distinct customer (historical NEW + CDC inserts) appears in DimCustomer"),

        ("DimAccount distinct",
         "select count(distinct accountid) from ("
         "  select accountid from \"stg_customermgmt\" where actiontype in ('NEW','ADDACCT')"
         f"  union select accountid from {_read(src, 'Batch[23]/Account.txt', _ACCT_INC, '|')} where cdc_flag = 'I')",
         'select count(distinct accountid) from "DimAccount"', "==",
         "Every distinct account (historical + CDC inserts) appears in DimAccount"),

        ("DimCompany distinct",
         "select count(distinct try_cast(trim(substr(value, 61, 10)) as bigint)) from \"FinWire\" where rectype = 'CMP'",
         'select count(distinct companyid) from "DimCompany"', "==",
         "Every distinct FinWire CMP company (CIK) appears in DimCompany"),

        ("DimSecurity distinct",
         "select count(distinct trim(substr(value, 1, 15))) from \"FinWire\" where rectype = 'SEC'",
         'select count(distinct symbol) from "DimSecurity"', ">=",
         "Every DimSecurity symbol comes from a FinWire SEC record (target may be <= source if a security references an unknown company)"),

        ("Financial",
         "select count(*) from \"FinWire\" where rectype like 'FIN%'",
         'select count(*) from "Financial"', ">=",
         "Every FinWire FIN record that matches a company produces a Financial row"),

        ("DimTrade",
         f"select (select count(distinct t_id) from {_read(src, 'Batch1/Trade.txt', _TRADE_B1, '|')})"
         f"     + (select count(distinct tradeid) from {_read(src, 'Batch[23]/Trade.txt', _TRADE_INC, '|')} where cdc_flag = 'I')",
         'select count(*) from "DimTrade"', "==",
         "DimTrade row count matches distinct source trades (Batch1 + CDC inserts)"),

        ("FactCashBalances",
         "select count(*) from (select distinct ct_ca_id, cast(ct_dts as date) d from ("
         f"  select ct_ca_id, ct_dts from {_read(src, 'Batch1/CashTransaction.txt', _CASH_B1, '|')}"
         f"  union all select ct_ca_id, ct_dts from {_read(src, 'Batch[23]/CashTransaction.txt', _CASH_INC, '|')}))",
         'select count(*) from "FactCashBalances"', "==",
         "FactCashBalances matches source (account, date) daily pairs"),

        ("FactHoldings",
         "select (select count(*) from " + _read(src, 'Batch1/HoldingHistory.txt', _HOLD_B1, '|') + ")"
         "     + (select count(*) from " + _read(src, 'Batch[23]/HoldingHistory.txt', _HOLD_INC, '|') + ")",
         'select count(*) from "FactHoldings"', "==",
         "FactHoldings matches source HoldingHistory rows (all batches)"),

        ("FactWatches",
         "select count(*) from (select distinct w_c_id, w_s_symb from ("
         f"  select w_c_id, w_s_symb from {_read(src, 'Batch1/WatchHistory.txt', _WATCH_B1, '|')}"
         f"  union all select w_c_id, w_s_symb from {_read(src, 'Batch[23]/WatchHistory.txt', _WATCH_INC, '|')}))",
         'select count(*) from "FactWatches"', "==",
         "FactWatches matches source distinct (customer, security) watch pairs"),

        ("FactMarketHistory",
         "select (select count(*) from " + _read(src, 'Batch1/DailyMarket.txt', _DM_B1, '|') + ")"
         "     + (select count(*) from " + _read(src, 'Batch[23]/DailyMarket.txt', _DM_INC, '|') + ")",
         'select count(*) from "FactMarketHistory"', ">=",
         "No DailyMarket row is lost (target >= source; SCD2 security splits can raise it)"),

        ("Prospect",
         f"select count(distinct agencyid) from {_read(src, 'Batch3/Prospect.csv', _PROSPECT, ',')}",
         'select count(*) from "Prospect"', "==",
         "Prospect matches distinct agencyids in the latest (Batch3) snapshot"),
    ]

    # --- Invariants: (name, fail-count SQL that must be 0, description) ---
    dims_scd2 = [("DimCustomer", "customerid", "sk_customerid"),
                 ("DimAccount", "accountid", "sk_accountid"),
                 ("DimSecurity", "symbol", "sk_securityid"),
                 ("DimCompany", "companyid", "sk_companyid")]
    inv = []

    # NULL surrogate keys — a FULL/INNER join that failed to resolve would leave one NULL.
    for tbl, cols in [
        ("DimAccount", ["sk_customerid", "sk_brokerid"]),
        ("DimTrade", ["sk_customerid", "sk_accountid", "sk_securityid", "sk_companyid"]),
        ("FactCashBalances", ["sk_customerid", "sk_accountid"]),
        ("FactHoldings", ["sk_customerid", "sk_accountid"]),
        ("FactWatches", ["sk_customerid", "sk_securityid"]),
        ("FactMarketHistory", ["sk_securityid"]),
    ]:
        for c in cols:
            inv.append((f"{tbl}.{c} not null",
                        f'select count(*) from "{tbl}" where {c} is null',
                        "No NULL surrogate keys (every dimensional join must resolve)"))

    # SCD2 integrity.
    for tbl, bk, sk in dims_scd2:
        inv.append((f"{tbl} EndDate alignment",
                    f'select (select count(*) from "{tbl}")'
                    f' - (select count(*) from "{tbl}" a join "{tbl}" b on a.{bk} = b.{bk} and a.enddate = b.effectivedate)'
                    f" - (select count(*) from \"{tbl}\" where enddate = date '9999-12-31')",
                    "Each version's EndDate meets the next version's EffectiveDate (or is end-of-time)"))
        inv.append((f"{tbl} no overlap",
                    f'select count(*) from "{tbl}" a join "{tbl}" b'
                    f' on a.{bk} = b.{bk} and a.{sk} <> b.{sk} and a.effectivedate >= b.effectivedate and a.effectivedate < b.enddate',
                    "No two versions of the same business key have overlapping date ranges"))
        inv.append((f"{tbl} one open version",
                    f'select (select count(distinct {bk}) from "{tbl}")'
                    f" - (select count(*) from \"{tbl}\" where enddate = date '9999-12-31')",
                    "Exactly one open (EndDate 9999-12-31) version per business key"))
        inv.append((f"{tbl} no zero-length",
                    f'select count(*) from "{tbl}" where effectivedate = enddate',
                    "No zero-length versions (EffectiveDate = EndDate)"))
        inv.append((f"{tbl} distinct SKs",
                    f'select (select count(*) from "{tbl}") - (select count(distinct {sk}) from "{tbl}")',
                    "All surrogate keys are distinct"))
    for tbl, bk, sk in dims_scd2[:3]:  # IsCurrent tracked on Customer/Account/Security
        inv.append((f"{tbl} IsCurrent consistent",
                    f'select (select count(*) from "{tbl}")'
                    f" - (select count(*) from \"{tbl}\" where enddate = date '9999-12-31' and iscurrent)"
                    f" - (select count(*) from \"{tbl}\" where enddate < date '9999-12-31' and not iscurrent)",
                    "IsCurrent is true iff the version is open (EndDate = 9999-12-31)"))

    # Foreign-key surrogate keys land in a parent version whose window contains the child's.
    inv += [
        ("DimAccount.sk_customerid containment",
         'select (select count(*) from "DimAccount")'
         ' - (select count(*) from "DimAccount" a join "DimCustomer" c'
         ' on a.sk_customerid = c.sk_customerid and c.effectivedate <= a.effectivedate and a.enddate <= c.enddate)',
         "Account's customer SK points to a customer version whose dates contain the account's"),
        ("DimAccount.sk_brokerid containment",
         'select (select count(*) from "DimAccount")'
         ' - (select count(*) from "DimAccount" a join "DimBroker" b'
         ' on a.sk_brokerid = b.sk_brokerid and b.effectivedate <= a.effectivedate and a.enddate <= b.enddate)',
         "Account's broker SK points to a valid DimBroker version"),
        ("DimSecurity.sk_companyid containment",
         'select (select count(*) from "DimSecurity")'
         ' - (select count(*) from "DimSecurity" s join "DimCompany" c'
         ' on s.sk_companyid = c.sk_companyid and c.effectivedate <= s.effectivedate and s.enddate <= c.enddate)',
         "Security's company SK points to a company version whose dates contain the security's"),
    ]

    # Enumerated / formatted columns hold only legal values.
    inv += [
        ("DimCustomer Status domain",
         "select count(*) from \"DimCustomer\" where status not in ('Active','Inactive')",
         "Customer Status is Active/Inactive"),
        ("DimCustomer Gender domain",
         "select count(*) from \"DimCustomer\" where gender not in ('M','F','U')",
         "Customer Gender is M/F/U"),
        ("DimCustomer TaxID format",
         "select count(*) from \"DimCustomer\" where taxid not like '___-__-____'",
         "Customer TaxID matches nnn-nn-nnnn"),
        ("DimAccount Status domain",
         "select count(*) from \"DimAccount\" where status not in ('Active','Inactive')",
         "Account Status is Active/Inactive"),
        ("DimAccount TaxStatus domain",
         "select count(*) from \"DimAccount\" where batchid = 1 and taxstatus not in (0,1,2)",
         "Historical account TaxStatus is 0/1/2"),
        ("DimSecurity Status domain",
         "select count(*) from \"DimSecurity\" where status not in ('Active','Inactive')",
         "Security Status is Active/Inactive"),
        ("DimSecurity ExchangeID domain",
         "select count(*) from \"DimSecurity\" where exchangeid not in ('NYSE','NASDAQ','AMEX','PCX')",
         "Security ExchangeID is a known exchange"),
        ("DimSecurity Issue domain",
         "select count(*) from \"DimSecurity\" where issue not in ('COMMON','PREF_A','PREF_B','PREF_C','PREF_D')",
         "Security Issue is a known issue type"),
        ("DimCustomer LocalTaxRate valid",
         'select (select count(*) from "DimCustomer")'
         ' - (select count(*) from "DimCustomer" c join "TaxRate" t on c.localtaxratedesc = t.tx_name and c.localtaxrate = t.tx_rate)',
         "Customer local tax rate/desc pair exists in TaxRate"),
        ("DimCustomer NationalTaxRate valid",
         'select (select count(*) from "DimCustomer")'
         ' - (select count(*) from "DimCustomer" c join "TaxRate" t on c.nationaltaxratedesc = t.tx_name and c.nationaltaxrate = t.tx_rate)',
         "Customer national tax rate/desc pair exists in TaxRate"),
    ]

    return recon, inv


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", required=True, help="root_path (Delta output root; local or abfss://)")
    ap.add_argument("--schema", default="tpcdi")
    ap.add_argument("--source-dir", required=True,
                    help="the sf<N> source dir the warehouse was loaded from (local or abfss://Files/...)")
    ap.add_argument("--summary", default=None,
                    help="_seed_summary.json (local or abfss://) with precomputed source aggregates; "
                         "falls back to scanning the raw source files if absent/unreadable")
    args = ap.parse_args()

    storage_options = None
    if args.warehouse.startswith("abfss://"):
        token = os.environ.get("ONELAKE_TOKEN", "")
        if not token:
            sys.exit("ERROR: ONELAKE_TOKEN is empty — needed to audit an abfss:// warehouse")
        storage_options = {"bearer_token": token}

    conn = duckrun.connect(
        args.warehouse, storage_options=storage_options, schema=args.schema, read_only=True)
    src = args.source_dir.rstrip("/")

    # Precomputed source aggregates (from generation, on local disk) spare us the expensive
    # raw-file scans over abfss. Read the tiny JSON through the same connection (read_text
    # works local + abfss); any failure just falls back to live scans.
    precomputed = {}
    if args.summary:
        try:
            import json
            txt = conn.sql(f"select content from read_text('{args.summary}')").fetchone()[0]
            precomputed = json.loads(txt).get("source", {})
            print(f"  using precomputed source aggregates from {args.summary} "
                  f"({len(precomputed)} value(s))")
        except Exception as e:  # noqa: BLE001
            print(f"  no usable summary at {args.summary} ({str(e).splitlines()[0]}); scanning source files")

    # Liveness summary (context; a missing table also surfaces as a failed check below).
    print(f"\n  TPC-DI warehouse: {args.warehouse} (schema {args.schema})")
    print(f"  source seed:      {src}\n  {'table':<22} rows")
    print("  " + "-" * 34)
    for tbl in REQUIRED:
        try:
            n = conn.sql(f'SELECT count(*) FROM "{tbl}"').fetchone()[0]
            print(f"  {tbl:<22} {n:>12,}")
        except Exception as e:  # noqa: BLE001
            print(f"  {tbl:<22} MISSING/ERROR: {str(e).splitlines()[0]}")

    recon, inv = build_checks(src)
    failures = []

    def _scalar(sql):
        return conn.sql(sql).fetchone()[0]

    print(f"\n  Reconciliation (source files -> warehouse)   (* = source from precomputed summary)"
          f"\n  {'check':<28}{'source':>14}{'target':>14}  status")
    print("  " + "-" * 72)
    for name, ssql, tsql, op, desc in recon:
        try:
            s = precomputed[name] if name in precomputed else _scalar(ssql)
            t = _scalar(tsql)
            ok = (s == t) if op == "==" else (t >= s)
            mark = "*" if name in precomputed else " "
            status = "PASS" if ok else f"FAIL ({op}) delta={t - s:+,}"
            print(f"  {name:<27}{mark}{s:>14,}{t:>14,}  {status}")
            if not ok:
                failures.append(f"{name}: source={s:,} target={t:,} ({desc})")
        except Exception as e:  # noqa: BLE001
            print(f"  {name:<28}{'?':>14}{'?':>14}  ERROR: {str(e).splitlines()[0]}")
            failures.append(f"{name}: {str(e).splitlines()[0]}")

    print(f"\n  Integrity invariants (must be 0)\n  {'check':<40}{'violations':>12}  status")
    print("  " + "-" * 66)
    for name, fsql, desc in inv:
        try:
            f = _scalar(fsql)
            ok = (f == 0)
            print(f"  {name:<40}{f:>12,}  {'PASS' if ok else 'FAIL'}")
            if not ok:
                failures.append(f"{name}: {f:,} violations ({desc})")
        except Exception as e:  # noqa: BLE001
            print(f"  {name:<40}{'?':>12}  ERROR: {str(e).splitlines()[0]}")
            failures.append(f"{name}: {str(e).splitlines()[0]}")

    print()
    if failures:
        print(f"AUDIT FAILED ({len(failures)} problem(s)):")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print(f"AUDIT PASSED: {len(recon)} reconciliations + {len(inv)} invariants all hold.")


if __name__ == "__main__":
    main()
