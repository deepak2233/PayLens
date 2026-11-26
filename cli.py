#!/usr/bin/env python
"""PayLens one-command demo.

    python cli.py demo                 # seed SQLite + run the UPI investigation
    python cli.py demo --small         # tiny dataset (fast smoke run)
    python cli.py api                  # serve the dashboard + API
    python cli.py investigate "..."    # run an arbitrary investigation question
    python cli.py real-data --raw-dir <dir>   # real open-source data end-to-end

The demo layer needs no Kafka, Docker, or LLM key: events are produced to an
in-memory bus (the same producer/consumer contract as Kafka) and stored in
SQLite at ./paylens_demo.db.

The real-data flow ingests the checksum-verified Olist marketplace orders
(99,441 real orders) plus the Upworthy randomized A/B archive and runs the
same agent graph at day grain; see paylens/realdata/sources.py for
licenses and provenance.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

DEFAULT_QUESTION = (
    "Investigate the UPI success-rate drop over the last 24 hours. "
    "Compare with yesterday and identify affected segments."
)


def cmd_demo(args: argparse.Namespace) -> None:
    from paylens.pipeline import run_investigation, seed_database

    attempts = 2_000 if args.small else args.attempts_per_day
    exp_arm = 1_000 if args.small else args.experiment_users_per_arm

    print("== seeding demo data (in-memory bus -> SQLite) ==")
    stats = seed_database(attempts_per_day=attempts,
                          experiment_users_per_arm=exp_arm)
    print(json.dumps(stats, indent=2, default=str))

    print("\n== running investigation ==")
    result = run_investigation(args.question or DEFAULT_QUESTION)
    report = result["report"]

    print(f"\nrun_id: {result['run_id']}")
    print(f"\nFINDING\n{report.get('finding')}\n")
    print("EVIDENCE")
    for bullet in report.get("evidence", []):
        print(f"  - {bullet}")
    print("\nRECOMMENDED ACTIONS")
    for i, action in enumerate(report.get("recommended_actions", []), 1):
        print(f"  {i}. {action}")
    print(f"\nProvenance: {len(report.get('provenance', {}).get('governed_queries', []))} "
          "governed queries executed and audited; "
          f"metric version {report.get('provenance', {}).get('metric_version')}.")
    print("\nFull report JSON: paylens_demo_report.json")
    Path("paylens_demo_report.json").write_text(
        json.dumps(report, indent=2, default=str), encoding="utf-8")


def cmd_real_data(args: argparse.Namespace) -> None:
    from paylens.realdata.loader import dump_report, real_investigation

    print("== real open-source data: verify -> validate -> map -> ingest ==")
    result = real_investigation(
        Path(args.raw_dir),
        window_days=args.window_days,
        limit_orders=args.orders,
        anchor=args.anchor,
        include_upworthy=not args.no_upworthy,
    )
    report = result["report"]

    prov = report.get("data_provenance", {}).get("olist", {})
    stats = prov.get("stats", {})
    print(f"verified files: {[f['file'] for f in prov.get('verification', {}).get('files', [])]}")
    print(f"orders ingested: {stats.get('orders'):,} | events: "
          f"{stats.get('events_ingested'):,} | invalid: {stats.get('invalid')} | "
          f"duplicates: {stats.get('duplicates')} | {stats.get('duration_ms'):,} ms")
    for v in prov.get("validation", []):
        print(f"  {v['file']}: accepted {v['accepted']:,} / rejected {v['rejected']}")

    print(f"\nrun_id: {result['run_id']}")
    print(f"\nFINDING\n{report.get('finding')}\n")
    print("EVIDENCE")
    for bullet in report.get("evidence", []):
        print(f"  - {bullet}")
    print("\nRECOMMENDED ACTIONS")
    for i, action in enumerate(report.get("recommended_actions", []), 1):
        print(f"  {i}. {action}")

    out = Path(args.out)
    dump_report(result, out)
    print(f"\nFull report JSON: {out}")


def cmd_api(_args: argparse.Namespace) -> None:
    import uvicorn

    uvicorn.run("paylens.api.main:app", host="127.0.0.1", port=8000, reload=False)


def cmd_investigate(args: argparse.Namespace) -> None:
    from paylens.pipeline import run_investigation

    result = run_investigation(args.question)
    print(json.dumps(result["report"], indent=2, default=str))


def cmd_analyze(args: argparse.Namespace) -> None:
    from paylens.realdata.loader import dump_report
    from paylens.realdata.csv_upload import CsvUploadError, run_csv_investigation

    try:
        result = run_csv_investigation(args.csv, question=args.question)
    except CsvUploadError as exc:
        raise SystemExit(f"upload rejected: {exc}") from exc
    report = result["report"]
    print(f"== upload ingested: {result['upload_stats']} ==")
    print(f"\nrun_id: {result['run_id']}")
    print(f"\nFINDING\n{report.get('finding')}\n")
    print("EVIDENCE")
    for bullet in report.get("evidence", []):
        print(f"  - {bullet}")
    print("\nRECOMMENDED ACTIONS")
    for i, action in enumerate(report.get("recommended_actions", []), 1):
        print(f"  {i}. {action}")
    out = Path(args.out)
    dump_report(result, out)
    print(f"\nFull report JSON: {out}")


def main() -> None:
    parser = argparse.ArgumentParser(description="PayLens demo CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    demo = sub.add_parser("demo", help="seed data + run the reference investigation")
    demo.add_argument("--small", action="store_true", help="tiny dataset, fast run")
    demo.add_argument("--attempts-per-day", type=int, default=20_000)
    demo.add_argument("--experiment-users-per-arm", type=int, default=10_000)
    demo.add_argument("--question", type=str, default=DEFAULT_QUESTION)
    demo.set_defaults(func=cmd_demo)

    api = sub.add_parser("api", help="serve the dashboard + API on :8000")
    api.set_defaults(func=cmd_api)

    inv = sub.add_parser("investigate", help="run one investigation against existing data")
    inv.add_argument("question", type=str)
    inv.set_defaults(func=cmd_investigate)

    ana = sub.add_parser("analyze",
                         help="analyze your own CSV export (one row per payment attempt)")
    ana.add_argument("--csv", type=str, required=True,
                     help="path to the CSV file (schema in the web console's Analyze Data tab)")
    ana.add_argument("--question", type=str, default=None,
                     help="override the investigation question")
    ana.add_argument("--out", type=str, default="paylens_upload_report.json")
    ana.set_defaults(func=cmd_analyze)

    real = sub.add_parser("real-data",
                          help="real open-source data end-to-end (Olist + Upworthy)")
    real.add_argument("--raw-dir", type=str, default="../paylens_data/raw",
                      help="directory holding the verified raw CSVs")
    real.add_argument("--window-days", type=int, default=60,
                      help="investigation window in days (baseline is equal length)")
    real.add_argument("--orders", type=int, default=None,
                      help="cap on orders ingested (default: all 99,441)")
    real.add_argument("--anchor", type=str, default=None,
                      help="pin window_end, e.g. 2018-08-31T23:59:00")
    real.add_argument("--no-upworthy", action="store_true",
                      help="skip the real A/B archive readouts")
    real.add_argument("--out", type=str, default="real_data_report.json")
    real.set_defaults(func=cmd_real_data)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
