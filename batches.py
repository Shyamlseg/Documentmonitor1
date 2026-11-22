
import argparse
import csv
import hashlib
import html
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

csv.field_size_limit(2 ** 31 - 1)

PLAN_DIR = Path('batch_plan')
MANIFEST = PLAN_DIR / 'manifest.json'
WORK_DIR = Path('chunk_work')
DIAG_DIR = Path('chunk_diag')

OUTPUT_FIELDS = ['company', 'document_title', 'document_title_source', 'document_url']
DIFF_FIELDS = ['date', 'company', 'document_title', 'document_url']
KNOWN_FIELDS = ['first_seen_date', 'company', 'document_title', 'document_url', 'source_run_mode']
ISSUE_FIELDS = ['date', 'run_mode', 'url_file', 'source_url', 'issue_type', 'status_code',
                'documents_captured', 'error_message']
URL_STATUS_FIELDS = ['url', 'last_working_date', 'last_working_docs_count', 'latest_status_code',
                     'latest_issue', 'url_file']
SUMMARY_FIELDS = ['run_number', 'date', 'run_mode', 'url_file', 'total_urls_processed',
                  'total_documents_captured', 'new_diff_records', 'issue_count',
                  'success_zero_docs_count', 'fetch_failed_status_count', 'fetch_error_count',
                  'browser_fallback_enabled', 'output_file', 'raw_file', 'issues_file']
CHUNK_STATUS_FIELDS = ['chunk', 'status', 'sources', 'first_source', 'reason']

# P1 and P2/P3 share the production files in the repository root (as before);
# P4/P5 keeps its own files in p4_p5/.
SHARED = dict(output='output.csv', known='known_documents.csv', diff='diff.csv',
              status='url_status.csv', summary='run_summary_master.csv')
GROUPS = {
    'p1': dict(SHARED, label='P1', source='documents_p1.csv', issues='capture_issues.csv'),
    'p2_p3': dict(SHARED, label='P2/P3', source='documents_p2p3.csv',
                  issues='capture_issues_p2_p3_latest.csv'),
    'p4_p5': dict(label='P4/P5', source='p4_p5/documents_p4_p5.csv',
                  output='p4_p5/output_p4_p5.csv', known='p4_p5/known_documents_p4_p5.csv',
                  diff='diff_p4_p5.csv', status='p4_p5/url_status_p4_p5.csv',
                  summary='p4_p5/run_summary_p4_p5.csv', issues='p4_p5/capture_issues_p4_p5.csv'),
}
for _key, _cfg in GROUPS.items():
    _cfg['strategy'] = f"source_strategies_{Path(_cfg['source']).stem}.json"
    _cfg['chunk_status'] = f'chunk_status/{_key}_chunk_status.csv'


# ---------------------------------------------------------------- helpers
def read_csv(path):
    """(fieldnames, rows); (None, []) when the file is missing or empty."""
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        return None, []
    with path.open(newline='', encoding='utf-8-sig') as stream:
        reader = csv.DictReader(stream)
        rows = list(reader)
        return reader.fieldnames, rows


def write_csv(path, fields, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore', restval='')
        writer.writeheader()
        writer.writerows(rows)


def normalize_url_key(url):
    parsed = urlparse(url or '')
    return parsed._replace(scheme=parsed.scheme.lower(), netloc=parsed.netloc.lower(),
                           fragment='').geturl()


def parse_source(raw):
    raw = html.unescape((raw or '').strip())
    for prefix in ('fallback/', 'fallback:'):
        if raw.lower().startswith(prefix):
            return raw[len(prefix):].strip(), True
    return raw, False


def dedupe_rows(rows, key_field):
    final, seen = [], set()
    for row in rows:
        key = normalize_url_key(row.get(key_field, ''))
        if not key or key in seen:
            continue
        seen.add(key)
        final.append(row)
    return final


def to_int(value, default=0):
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return default


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def run_id():
    return os.environ.get('GITHUB_RUN_ID', 'local')


def github_output(**values):
    target = os.environ.get('GITHUB_OUTPUT')
    if target:
        with open(target, 'a', encoding='utf-8') as stream:
            for key, value in values.items():
                stream.write(f'{key}={value}\n')


def step_summary(text):
    target = os.environ.get('GITHUB_STEP_SUMMARY')
    if target:
        with open(target, 'a', encoding='utf-8') as stream:
            stream.write(text)


def load_manifest(group):
    manifest = json.loads(MANIFEST.read_text())
    if manifest['run_id'] != run_id():
        raise SystemExit('Batch plan belongs to another workflow run')
    if manifest['group'] != group:
        raise SystemExit(f"Batch plan is for group {manifest['group']}, not {group}")
    return manifest


# ---------------------------------------------------------------- plan
def load_sources(group):
    path = Path(GROUPS[group]['source'])
    if not path.exists():
        raise SystemExit(f'Link file not found: {path}')
    fields, rows = read_csv(path)
    if 'source_url' not in (fields or []):
        raise SystemExit(f'Missing source_url header in {path}')
    found = {}
    for row in rows:
        url, forced = parse_source(row.get('source_url'))
        if not url:
            continue
        if url in found:
            found[url]['forced'] = found[url]['forced'] or forced
        else:
            found[url] = {'url': url, 'forced': forced}
    return list(found.values())


def warn_cross_group_duplicates(group, sources):
    mine = {normalize_url_key(s['url']) for s in sources}
    for other, cfg in GROUPS.items():
        if other == group or not Path(cfg['source']).exists():
            continue
        _, rows = read_csv(cfg['source'])
        theirs = {normalize_url_key(parse_source(r.get('source_url'))[0]) for r in rows}
        shared = sorted(k for k in mine & theirs if k)
        if shared:
            print(f"::warning::{len(shared)} link(s) appear in both {group} and {other}; "
                  f"each should live in one group only. First: {shared[:3]}")


def plan(group, size, workers):
    if RUN_MODE() not in ('full', 'seed'):
        raise SystemExit(f"run_mode must be 'full' or 'seed', not {RUN_MODE()!r}")
    if size < 1:
        raise SystemExit('Chunk size must be at least 1')
    sources = load_sources(group)
    if not sources:
        raise SystemExit(f"No links in {GROUPS[group]['source']}")
    warn_cross_group_duplicates(group, sources)
    count = math.ceil(len(sources) / size)
    if count > 9999:
        raise SystemExit(f'{count} chunks is too many; increase the chunk size')
    shutil.rmtree(PLAN_DIR, ignore_errors=True)
    PLAN_DIR.mkdir(parents=True)
    manifest = {'run_id': run_id(), 'group': group, 'source_count': len(sources), 'chunks': {}}
    total = len(sources)
    for i in range(count):
        # Contiguous slices keep neighbouring links (usually the same site) in one chunk.
        part = sources[i * total // count:(i + 1) * total // count]
        cid = f'{i + 1:04}'
        path = PLAN_DIR / f'chunk_{cid}.csv'
        write_csv(path, ['source_url'], [
            {'source_url': ('fallback:' if s['forced'] else '') + s['url']} for s in part])
        manifest['chunks'][cid] = {'sources': part, 'input_sha256': sha256(path)}
    MANIFEST.write_text(json.dumps(manifest, indent=2))
    workers = max(1, min(workers, count))
    github_output(workers=json.dumps(list(range(1, workers + 1))), total=count, sources=total)
    print(f'{GROUPS[group]["label"]}: {total} links in {count} chunks of about {math.ceil(total / count)}, '
          f'{workers} workers')


def RUN_MODE():
    return os.environ.get('RUN_MODE', 'full').strip().lower()


# ---------------------------------------------------------------- run one chunk
def chunk_paths(cid):
    d = WORK_DIR / f'chunk_{cid}'
    return d, {'output': d / 'output.csv', 'issues': d / 'issues.csv', 'diff': d / 'diff.csv',
               'known': d / 'known.csv', 'summary': d / 'summary.csv', 'status': d / 'status.csv',
               'strategy': d / 'strategy.json'}


def copy_baseline(src, dest, fields):
    src = Path(src)
    if src.exists() and src.stat().st_size > 0:
        Path(dest).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dest)
    else:
        write_csv(dest, fields, [])


def run_scraper(env, timeout_min):
    process = subprocess.Popen([sys.executable, 'scrape.py'], env=env, start_new_session=True)
    try:
        code = process.wait(timeout=timeout_min * 60)
        reason = f'scrape.py exited with code {code}'
    except subprocess.TimeoutExpired:
        code, reason = 1, f'scrape.py stopped after {timeout_min:g} minutes (chunk time limit)'
    finally:
        try:  # also removes any browser processes left behind
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        process.wait()
    return code, reason


def finish_chunk(cfg, entry, p):
    _, summaries = read_csv(p['summary'])
    if len(summaries) != 1 or to_int(summaries[0].get('total_urls_processed'), -1) != len(entry['sources']):
        raise ValueError('chunk summary does not match the planned source count')
    for kind in ('status', 'issues'):
        fields, rows = read_csv(p[kind])
        if not fields:
            raise ValueError(f'{kind} file is missing or empty')
        if 'url_file' in fields:
            for row in rows:
                row['url_file'] = cfg['source']
        write_csv(p[kind], fields, rows)
    # Upload only NEW history; the merge keeps the production history.
    for kind in ('known', 'diff'):
        _, baseline = read_csv(cfg[kind])
        existing = {row.get('document_url') for row in baseline}
        fields, rows = read_csv(p[kind])
        if not fields:
            raise ValueError(f'{kind} file is missing or empty')
        write_csv(p[kind], fields, [r for r in rows if r.get('document_url') not in existing])
    fields, _ = read_csv(p['output'])
    if not fields:
        raise ValueError('output file is missing or empty')
    json.loads(p['strategy'].read_text())


def fail_chunk(d, cid, reason):
    shutil.rmtree(d, ignore_errors=True)  # do not upload big baseline copies for failed chunks
    d.mkdir(parents=True)
    (d / 'failed.json').write_text(json.dumps({'chunk': cid, 'run_id': run_id(), 'reason': reason}))


def run_chunk(group, cid, timeout_min):
    started = time.monotonic()
    manifest = load_manifest(group)
    entry = manifest['chunks'][cid]
    cfg = GROUPS[group]
    d, p = chunk_paths(cid)
    try:
        chunk_file = PLAN_DIR / f'chunk_{cid}.csv'
        if sha256(chunk_file) != entry['input_sha256']:
            raise ValueError('chunk file differs from the plan')
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True)
        diag = DIAG_DIR / f'chunk_{cid}'
        shutil.rmtree(diag, ignore_errors=True)
        diag.mkdir(parents=True)
        copy_baseline(cfg['known'], p['known'], KNOWN_FIELDS)
        copy_baseline(cfg['diff'], p['diff'], DIFF_FIELDS)
        urls = {s['url'] for s in entry['sources']}
        learned = {}
        strategy_path = Path(cfg['strategy'])
        if strategy_path.exists():
            try:
                learned = {u: v for u, v in json.loads(strategy_path.read_text()).items() if u in urls}
            except ValueError:
                learned = {}
        p['strategy'].write_text(json.dumps(learned))
        env = dict(os.environ,
                   TARGET_URL_FILE=str(chunk_file), OUTPUT_FILE=str(p['output']),
                   ISSUES_FILE=str(p['issues']), DIFF_FILE=str(p['diff']),
                   KNOWN_DOCUMENTS_FILE=str(p['known']), RUN_SUMMARY_FILE=str(p['summary']),
                   URL_STATUS_FILE=str(p['status']), SOURCE_STRATEGY_FILE=str(p['strategy']),
                   RAW_FILE=str(diag / 'raw_links.csv'),
                   BROWSER_TIMINGS_FILE=str(diag / 'browser_timings.jsonl'))
        code, reason = run_scraper(env, timeout_min)
        if code != 0:
            raise RuntimeError(reason)
        finish_chunk(cfg, entry, p)
        (d / 'completion.json').write_text(json.dumps({
            'chunk': cid, 'group': group, 'run_id': manifest['run_id'],
            'input_sha256': entry['input_sha256'], 'processed': len(entry['sources'])}))
    except Exception as error:  # a failed chunk must never stop the worker
        reason = f'{type(error).__name__}: {error}'
        fail_chunk(d, cid, reason)
        print(f'CHUNK {cid} FAILED after {time.monotonic() - started:.0f}s: {reason}', flush=True)
        return 1
    print(f'CHUNK {cid} COMPLETED sources={len(entry["sources"])} '
          f'seconds={time.monotonic() - started:.0f}', flush=True)
    return 0


# ---------------------------------------------------------------- merge
def validate_chunk(d, entry, manifest):
    marker = json.loads((d / 'completion.json').read_text())
    if (marker.get('run_id') != manifest['run_id'] or
            marker.get('input_sha256') != entry['input_sha256'] or
            marker.get('processed') != len(entry['sources'])):
        raise ValueError('completion marker does not match the plan')
    for name in ('output', 'issues', 'diff', 'known', 'summary', 'status'):
        fields, _ = read_csv(d / f'{name}.csv')
        if not fields:
            raise ValueError(f'{name}.csv is missing or empty')
    json.loads((d / 'strategy.json').read_text())


def collect_chunks(manifest, root):
    completed, rows = {}, []
    for cid, entry in manifest['chunks'].items():
        state = 'MISSING'
        reason = 'no output found (chunk was never claimed, or its worker stopped before finishing it)'
        for d in sorted(p for p in root.rglob(f'chunk_{cid}') if p.is_dir()):
            if (d / 'completion.json').exists():
                try:
                    validate_chunk(d, entry, manifest)
                    completed[cid] = d
                    state, reason = 'COMPLETED', ''
                    break
                except (ValueError, OSError) as error:
                    state, reason = 'FAILED', f'invalid output: {error}'
            elif (d / 'failed.json').exists():
                state = 'FAILED'
                try:
                    reason = json.loads((d / 'failed.json').read_text()).get('reason', 'unknown')
                except ValueError:
                    reason = 'unreadable failure marker'
        rows.append({'chunk': cid, 'status': state, 'sources': len(entry['sources']),
                     'first_source': entry['sources'][0]['url'], 'reason': reason})
    return completed, rows


def merge(group, root, strict):
    cfg = GROUPS[group]
    manifest = load_manifest(group)
    root = Path(root)
    completed, status_rows = collect_chunks(manifest, root)
    write_csv(cfg['chunk_status'], CHUNK_STATUS_FIELDS, status_rows)
    total = len(manifest['chunks'])
    bad = [r for r in status_rows if r['status'] != 'COMPLETED']
    print(f'{cfg["label"]}: {len(completed)} of {total} chunks completed, {len(bad)} not completed')
    lines = [f'\n## {cfg["label"]} chunks\n\n| Status | Chunks |\n|---|---|\n']
    for state in ('COMPLETED', 'FAILED', 'MISSING'):
        lines.append(f'| {state} | {sum(1 for r in status_rows if r["status"] == state)} |\n')
    if bad:
        lines.append('\n**Chunks not completed (their sources keep their old rows):**\n\n')
        for r in bad[:60]:
            lines.append(f'- chunk {r["chunk"]} ({r["sources"]} links, first {r["first_source"]}): '
                         f'{r["status"]} - {r["reason"]}\n')
        for r in bad[:20]:
            print(f'::warning::chunk {r["chunk"]} {r["status"]}: {r["reason"]}')
    step_summary(''.join(lines))
    if not completed:
        raise SystemExit('No chunk finished; nothing to publish')
    if strict and bad:
        raise SystemExit(f'{len(bad)} chunk(s) did not finish and require_all_chunks is on; nothing published')

    done_sources = [s for cid in completed for s in manifest['chunks'][cid]['sources']]
    done_keys = {normalize_url_key(s['url']) for s in done_sources}

    def chunk_rows(name):
        rows = []
        for cid in sorted(completed):
            rows.extend(read_csv(completed[cid] / f'{name}.csv')[1])
        return rows

    # output: replace only the rows of sources that finished
    _, existing = read_csv(cfg['output'])
    kept = [r for r in existing if normalize_url_key(r.get('company', '')) not in done_keys]
    output_rows = dedupe_rows(kept + chunk_rows('output'), 'document_url')
    write_csv(cfg['output'], OUTPUT_FIELDS, output_rows)

    # known documents and diff: production history plus new rows
    known_rows = dedupe_rows(read_csv(cfg['known'])[1] + chunk_rows('known'), 'document_url')
    write_csv(cfg['known'], KNOWN_FIELDS, known_rows)
    diff_rows = dedupe_rows(read_csv(cfg['diff'])[1] + chunk_rows('diff'), 'document_url')
    write_csv(cfg['diff'], DIFF_FIELDS, diff_rows)

    # issues: latest run only
    issue_rows = chunk_rows('issues')
    write_csv(cfg['issues'], ISSUE_FIELDS, issue_rows)

    # url status: keep history, update from this run
    status_by_key = {}
    for row in read_csv(cfg['status'])[1]:
        if row.get('url'):
            status_by_key[normalize_url_key(row['url'])] = {f: row.get(f, '') for f in URL_STATUS_FIELDS}
    for row in chunk_rows('status'):
        url = row.get('url', '')
        if not url:
            continue
        key = normalize_url_key(url)
        current = status_by_key.get(key, {f: '' for f in URL_STATUS_FIELDS})
        current.update(url=url, latest_status_code=row.get('latest_status_code', ''),
                       latest_issue=row.get('latest_issue', ''),
                       url_file=row.get('url_file', current.get('url_file', '')))
        if row.get('last_working_date'):
            current['last_working_date'] = row['last_working_date']
            current['last_working_docs_count'] = row.get('last_working_docs_count', '')
        status_by_key[key] = current
    status_list = sorted(status_by_key.values(), key=lambda r: (
        r.get('latest_status_code', ''), r.get('url_file', ''), r.get('url', '')))
    write_csv(cfg['status'], URL_STATUS_FIELDS, status_list)

    # run summary: one total row for the whole group run
    summary_rows = read_csv(cfg['summary'])[1]
    previous = max((to_int(r.get('run_number')) for r in summary_rows), default=0)
    new = chunk_rows('summary')
    if new:
        def total(field):
            return sum(to_int(r.get(field)) for r in new)
        summary_rows.append({
            'run_number': str(previous + 1),
            'date': max((r.get('date', '') for r in new), default=''),
            'run_mode': next((r['run_mode'] for r in new if r.get('run_mode')), ''),
            'url_file': f'{group}_total',
            'total_urls_processed': total('total_urls_processed'),
            'total_documents_captured': total('total_documents_captured'),
            'new_diff_records': total('new_diff_records'),
            'issue_count': total('issue_count'),
            'success_zero_docs_count': total('success_zero_docs_count'),
            'fetch_failed_status_count': total('fetch_failed_status_count'),
            'fetch_error_count': total('fetch_error_count'),
            'browser_fallback_enabled': 'true' if any(
                str(r.get('browser_fallback_enabled', '')).strip().lower() == 'true' for r in new) else 'false',
            'output_file': cfg['output'], 'raw_file': '', 'issues_file': cfg['issues']})
    write_csv(cfg['summary'], SUMMARY_FIELDS, summary_rows)

    # learned browser strategies: update only the sources that finished
    strategy_path = Path(cfg['strategy'])
    try:
        saved = json.loads(strategy_path.read_text()) if strategy_path.exists() else {}
    except ValueError:
        saved = {}
    for cid, d in completed.items():
        learned = json.loads((d / 'strategy.json').read_text())
        for s in manifest['chunks'][cid]['sources']:
            if s['url'] in learned:
                saved[s['url']] = learned[s['url']]
    strategy_path.write_text(json.dumps(saved, indent=2, sort_keys=True))

    print(f'Final output rows: {len(output_rows)}')
    print(f'Final known rows: {len(known_rows)}')
    print(f'Final diff rows: {len(diff_rows)}')
    print(f'Issues rows: {len(issue_rows)}')
    print(f'URL status rows: {len(status_list)}')
    print(f'Summary rows added: {1 if new else 0}')


# ---------------------------------------------------------------- scan / paths
SECRET_PATTERNS = [
    ('Alibaba AccessKey ID', re.compile(r'LTAI[0-9A-Za-z]{12,}')),
    ('Alibaba OSSAccessKeyId parameter', re.compile(r'(?i)OSSAccessKeyId=')),
    ('Alibaba AccessKeyId parameter', re.compile(r'(?i)AccessKeyId=')),
    ('Alibaba AccessKeySecret parameter', re.compile(r'(?i)AccessKeySecret=')),
    ('Alibaba Security Token parameter', re.compile(r'(?i)(x-oss-security-token|security-token)=')),
    ('Signed URL Signature parameter', re.compile(r'(?i)Signature=')),
]


def redact(text):
    text = str(text or '')
    text = re.sub(r'LTAI[0-9A-Za-z]{12,}', '[REDACTED_ALIBABA_ACCESS_KEY_ID]', text)
    return re.sub(r'(?i)(OSSAccessKeyId|AccessKeyId|AccessKeySecret|Signature|x-oss-security-token|'
                  r'security-token)=([^,&\s]+)', r'\1=[REDACTED]', text)


def scan(group):
    cfg = GROUPS[group]
    files = [cfg['output'], cfg['known'], cfg['issues'], cfg['status'], cfg['diff']]
    if group == 'p4_p5':
        files.append('p4_p5/document_canonical_keys_p4_p5.csv')
    found = False
    for name in files:
        path = Path(name)
        if not path.exists():
            continue
        with path.open(newline='', encoding='utf-8', errors='replace') as stream:
            for number, row in enumerate(csv.DictReader(stream), start=2):
                text = ' | '.join(str(v) for v in row.values())
                hits = [label for label, pattern in SECRET_PATTERNS if pattern.search(text)]
                if not hits:
                    continue
                found = True
                print('SECRET-LIKE PATTERN FOUND')
                print(f'File: {path}')
                print(f'Row number: {number}')
                print(f'Matched pattern(s): {", ".join(hits)}')
                print(f"Company/source URL: {row.get('company') or row.get('source_url') or row.get('url') or ''}")
                print(f"Document title: {row.get('document_title') or row.get('text') or ''}")
                if row.get('document_url') or row.get('url'):
                    print(f"Document URL redacted: {redact(row.get('document_url') or row.get('url'))}")
                if row.get('error_message'):
                    print(f"Error message redacted: {redact(row['error_message'])}")
                print('---')
    if found:
        print('One or more secret-like patterns were found. Stop before commit/push.')
        sys.exit(1)
    print('No Alibaba secret-like patterns found in generated CSV files.')


def publish_paths(group):
    cfg = GROUPS[group]
    names = [cfg[k] for k in ('output', 'known', 'diff', 'status', 'summary', 'issues',
                              'strategy', 'chunk_status')]
    print('\n'.join(n for n in names if Path(n).exists()))


# ---------------------------------------------------------------- cli
def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='action', required=True)
    for name in ('plan', 'run', 'merge', 'scan', 'paths'):
        p = sub.add_parser(name)
        p.add_argument('--group', required=True, choices=sorted(GROUPS))
        if name == 'plan':
            p.add_argument('--size', type=int, default=25)
            p.add_argument('--workers', type=int, default=15)
        if name == 'run':
            p.add_argument('--chunk', required=True)
            p.add_argument('--timeout-min', type=float, default=60)
        if name == 'merge':
            p.add_argument('--root', default='worker_outputs')
            p.add_argument('--strict', action='store_true')
    args = parser.parse_args()
    if args.action == 'plan':
        plan(args.group, args.size, args.workers)
    elif args.action == 'run':
        sys.exit(run_chunk(args.group, args.chunk, args.timeout_min))
    elif args.action == 'merge':
        merge(args.group, args.root, args.strict)
    elif args.action == 'scan':
        scan(args.group)
    else:
        publish_paths(args.group)


if __name__ == '__main__':
    main()
