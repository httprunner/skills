#!/usr/bin/env python3
"""Read CSV, extract Kuaishou CDN URLs, and write JSONL."""

import argparse
import csv
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal
from typing import Dict, List, Set, Tuple

from kwai_extract_cdn import extract_cdn_url_detail
from kwai_common import clean_output_jsonl, load_resume_success_urls


def _write_jsonl_row(row: Dict[str, str], output) -> None:
    output.write(json.dumps(row, ensure_ascii=False))
    output.write("\n")


def _process_one(
    share_url: str,
    cookie: str,
    cookie_file: str,
    proxy: str,
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
        proxy=proxy,
        endpoints=endpoints or None,
        timeout=timeout,
        jitter=jitter,
    )


def _normalize_cdn_url(cdn_url: str, err: str) -> Tuple[str, str]:
    if not cdn_url:
        return "", err
    lower = cdn_url.strip().lower()
    if lower.startswith("https://live.kuaishou.com") or lower.startswith("http://live.kuaishou.com"):
        return "", err or "live.kuaishou.com is not a CDN url"
    return cdn_url, err


def main() -> int:
    parser = argparse.ArgumentParser(description="CSV -> JSONL with Kuaishou CDN URLs")
    parser.add_argument("csv", help="Input CSV path")
    parser.add_argument("--url-col", default="URL", help="Column name that holds the Kuaishou URL")
    parser.add_argument("--cdn-col", default="CDNURL", help="Output column name for CDN URL")
    parser.add_argument(
        "--error-col",
        default="error_msg",
        help="Output column for error message (default: error_msg). Use empty string to disable.",
    )
    parser.add_argument("--cookie", default=None, help="Raw Cookie header value")
    parser.add_argument(
        "--cookie-file",
        default=None,
        help="Path to a cookie.txt file or JSON cookie array",
    )
    parser.add_argument(
        "--proxy",
        default=None,
        help="Proxy URL, e.g. http://user:pass@host:port",
    )
    parser.add_argument(
        "--endpoint",
        action="append",
        default=None,
        help="GraphQL endpoint override (can be provided multiple times)",
    )
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep between requests (seconds)")
    parser.add_argument("--workers", type=int, default=1, help="Concurrent workers")
    parser.add_argument("--progress-every", type=int, default=10, help="Progress log interval")
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
        help="Skip URLs already resolved in output JSONL (CDNURL set, error empty)",
    )
    args = parser.parse_args()

    output_mode = "w"
    resume_urls: Set[str] = set()
    if args.resume:
        if args.output == "-":
            sys.stderr.write("--resume requires --output file\n")
            return 2
        kept, removed = clean_output_jsonl(
            args.output,
            cdn_field=args.cdn_col,
            error_field=args.error_col or None,
        )
        if removed:
            sys.stderr.write(f"resume: removed {removed} failed rows\n")
        resume_urls = load_resume_success_urls(
            args.output,
            args.url_col,
            cdn_field=args.cdn_col,
            error_field=args.error_col or None,
        )
        output_mode = "a"

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
        total = len(rows)
        skipped = 0
        if resume_urls:
            rows = [
                row
                for row in rows
                if row.get(args.url_col, "").strip() not in resume_urls
            ]
            skipped = total - len(rows)
            if skipped:
                sys.stderr.write(f"resume: skipped {skipped} already resolved\n")
        if not rows:
            sys.stderr.write("resume: all rows already resolved\n")
            return 0
        total_remaining = len(rows)
        completed = skipped
        success = 0

        if args.workers <= 1:
            try:
                for row in rows:
                    share_url = row.get(args.url_col, "")
                    cdn_url, err = _process_one(
                        share_url,
                        cookie=args.cookie,
                        cookie_file=args.cookie_file,
                        proxy=args.proxy,
                        endpoints=args.endpoint or [],
                        timeout=args.timeout,
                        jitter=jitter_vals,
                    )
                    cdn_url, err = _normalize_cdn_url(cdn_url, err)
                    row[args.cdn_col] = cdn_url or ""
                    if args.error_col:
                        row[args.error_col] = err
                    _write_jsonl_row(row, output)
                    completed += 1
                    if cdn_url:
                        success += 1
                    if args.progress_every and completed % args.progress_every == 0:
                        sys.stderr.write(
                            f"progress: {completed}/{total} success: {success}\n"
                        )
                    if args.sleep:
                        time.sleep(args.sleep)
            except KeyboardInterrupt:
                sys.stderr.write("Interrupted by user, exiting gracefully.\n")
                return 130
        else:
            executor = ThreadPoolExecutor(max_workers=args.workers)
            try:
                future_map = {}
                for idx, row in enumerate(rows):
                    share_url = row.get(args.url_col, "")
                    future = executor.submit(
                        _process_one,
                        share_url,
                        args.cookie,
                        args.cookie_file,
                        args.proxy,
                        args.endpoint or [],
                        args.timeout,
                        jitter_vals,
                    )
                    future_map[future] = idx

                results: List[Tuple[str, str]] = [("", "")] * total_remaining
                ready: Dict[int, Tuple[str, str]] = {}
                next_idx = 0
                for future in as_completed(future_map):
                    idx = future_map[future]
                    try:
                        results[idx] = _normalize_cdn_url(*future.result())
                    except Exception as exc:
                        results[idx] = ("", str(exc))
                    ready[idx] = results[idx]
                    completed += 1
                    if results[idx][0]:
                        success += 1
                    if args.progress_every and completed % args.progress_every == 0:
                        sys.stderr.write(
                            f"progress: {completed}/{total} success: {success}\n"
                        )

                    while next_idx in ready:
                        cdn_url, err = ready.pop(next_idx)
                        row = rows[next_idx]
                        row[args.cdn_col] = cdn_url or ""
                        if args.error_col:
                            row[args.error_col] = err
                        _write_jsonl_row(row, output)
                        next_idx += 1
            except KeyboardInterrupt:
                sys.stderr.write("Interrupted by user, exiting gracefully.\n")
                signal.signal(signal.SIGINT, signal.SIG_IGN)
                executor.shutdown(wait=False, cancel_futures=True)
                return 130
            finally:
                executor.shutdown(wait=False, cancel_futures=True)

    if output is not sys.stdout:
        output.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
