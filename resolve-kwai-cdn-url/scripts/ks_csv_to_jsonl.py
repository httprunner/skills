#!/usr/bin/env python3
"""Read CSV, extract Kuaishou CDN URLs, and write JSONL."""

import argparse
import csv
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

from ks_extract import extract_cdn_url_detail


def _write_jsonl_row(row: Dict[str, str], output) -> None:
    output.write(json.dumps(row, ensure_ascii=False))
    output.write("\n")


def _process_one(
    share_url: str,
    cookie: str,
    cookie_file: str,
    endpoints: List[str],
    timeout: int,
    jitter: Tuple[float, float],
) -> Tuple[str, str]:
    if not share_url:
        return "", "empty url"
    return extract_cdn_url_detail(
        share_url,
        cookie=cookie,
        cookie_file=cookie_file,
        endpoints=endpoints or None,
        timeout=timeout,
        jitter=jitter,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="CSV -> JSONL with Kuaishou CDN URLs")
    parser.add_argument("csv", help="Input CSV path")
    parser.add_argument("--url-col", default="URL", help="Column name that holds the Kuaishou URL")
    parser.add_argument("--cdn-col", default="CDNURL", help="Output column name for CDN URL")
    parser.add_argument(
        "--error-col",
        default="",
        help="Optional output column for error message (e.g., error_msg). Leave empty to disable.",
    )
    parser.add_argument("--cookie", default=None, help="Raw Cookie header value")
    parser.add_argument("--cookie-file", default=None, help="Path to a cookie.txt file")
    parser.add_argument(
        "--endpoint",
        action="append",
        default=None,
        help="GraphQL endpoint override (can be provided multiple times)",
    )
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep between requests (seconds)")
    parser.add_argument("--workers", type=int, default=1, help="Concurrent workers")
    parser.add_argument("--progress-every", type=int, default=50, help="Progress log interval")
    parser.add_argument(
        "--jitter",
        default="0,0",
        help="Sleep per request as min,max seconds (e.g., 1,3)",
    )
    parser.add_argument("--timeout", type=int, default=10, help="Request timeout (seconds)")
    parser.add_argument("--output", default="-", help="Output JSONL path or '-' for stdout")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume by skipping lines already present in output file (JSONL only)",
    )
    args = parser.parse_args()

    resume_offset = 0
    output_mode = "w"
    if args.output != "-" and args.resume:
        try:
            with open(args.output, "r", encoding="utf-8") as existing:
                resume_offset = sum(1 for _ in existing)
            output_mode = "a"
        except FileNotFoundError:
            resume_offset = 0
            output_mode = "w"

    output = sys.stdout if args.output == "-" else open(args.output, output_mode, encoding="utf-8")
    jitter_vals = (0.0, 0.0)
    try:
        parts = [p.strip() for p in args.jitter.split(",")]
        if len(parts) == 2:
            jitter_vals = (float(parts[0]), float(parts[1]))
    except Exception:
        jitter_vals = (0.0, 0.0)

    with open(args.csv, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or args.url_col not in reader.fieldnames:
            sys.stderr.write(
                f"Missing URL column '{args.url_col}'. Available: {reader.fieldnames}\n"
            )
            return 2

        rows = list(reader)
        if resume_offset:
            rows = rows[resume_offset:]
        total = len(rows)
        completed = 0

        if args.workers <= 1:
            for row in rows:
                share_url = row.get(args.url_col, "")
                cdn_url, err = _process_one(
                    share_url,
                    cookie=args.cookie,
                    cookie_file=args.cookie_file,
                    endpoints=args.endpoint or [],
                    timeout=args.timeout,
                    jitter=jitter_vals,
                )
                row[args.cdn_col] = cdn_url or ""
                if args.error_col:
                    row[args.error_col] = err
                _write_jsonl_row(row, output)
                completed += 1
                if args.progress_every and completed % args.progress_every == 0:
                    sys.stderr.write(
                        f"progress: {resume_offset + completed}/{resume_offset + total}\n"
                    )
                if args.sleep:
                    time.sleep(args.sleep)
        else:
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                future_map = {}
                for idx, row in enumerate(rows):
                    share_url = row.get(args.url_col, "")
                    future = executor.submit(
                        _process_one,
                        share_url,
                        args.cookie,
                        args.cookie_file,
                        args.endpoint or [],
                        args.timeout,
                        jitter_vals,
                    )
                    future_map[future] = idx

                results: List[Tuple[str, str]] = [("", "")] * total
                ready: Dict[int, Tuple[str, str]] = {}
                next_idx = 0
                for future in as_completed(future_map):
                    idx = future_map[future]
                    try:
                        results[idx] = future.result()
                    except Exception as exc:
                        results[idx] = ("", str(exc))
                    ready[idx] = results[idx]
                    completed += 1
                    if args.progress_every and completed % args.progress_every == 0:
                        sys.stderr.write(
                            f"progress: {resume_offset + completed}/{resume_offset + total}\n"
                        )

                    while next_idx in ready:
                        cdn_url, err = ready.pop(next_idx)
                        row = rows[next_idx]
                        row[args.cdn_col] = cdn_url or ""
                        if args.error_col:
                            row[args.error_col] = err
                        _write_jsonl_row(row, output)
                        next_idx += 1

    if output is not sys.stdout:
        output.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
