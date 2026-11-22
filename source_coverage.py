
import argparse
import csv
import html
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

csv.field_size_limit(2 ** 31 - 1)

GROUPS = {
    'p1': dict(source='documents_p1.csv', output='output.csv', known='known_documents.csv',
               status='url_status.csv', issues='capture_issues.csv'),
    'p2_p3': dict(source='documents_p2p3.csv', output='output.csv', known='known_documents.csv',
                  status='url_status.csv', issues='capture_issues_p2_p3_latest.csv'),
    'p4_p5': dict(source='p4_p5/documents_p4_p5.csv', output='p4_p5/output_p4_p5.csv',
                  known='p4_p5/known_documents_p4_p5.csv', status='p4_p5/url_status_p4_p5.csv',
                  issues='p4_p5/capture_issues_p4_p5.csv'),
}

RESULTS = {
    'HAS_DOCUMENTS': 'Documents are listed under this link.',
    'DOCS_LISTED_UNDER_ANOTHER_LINK': 'Page worked and found documents, but the same PDFs are already '
                                      'listed under another link (the scraper keeps each PDF once).',
    'PAGE_LOADED_NO_DOCUMENTS': 'Page loaded but no document links were found.',
    'FETCH_FAILED': 'Page could not be fetched (blocked, not found or error).',
    'BROWSER_INCOMPLETE': 'Browser capture timed out or was incomplete and nothing was captured.',
    'NOT_IN_STATUS_FILE': 'No record at all for this link: it was probably not processed in the last run.',
    'UNCLEAR': 'Has issue rows or no documents, but the files do not say why.',
}


def clean(raw):
    raw = html.unescape((raw or '').strip())
    for prefix in ('fallback/', 'fallback:'):
        if raw.lower().startswith(prefix):
            return raw[len(prefix):].strip()
    return raw


def read_rows(path):
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return None
    with p.open(newline='', encoding='utf-8-sig', errors='replace') as stream:
        return list(csv.DictReader(stream))


def is_2xx(code):
    try:
        return 200 <= int(float(str(code).strip())) < 300
    except ValueError:
        return False


def classify(docs_out, docs_known, status, issue_types):
    if docs_out or docs_known:
        return 'HAS_DOCUMENTS'
    code = (status or {}).get('latest_status_code', '')
    latest_issue = ((status or {}).get('latest_issue') or '').upper()
    types = [t.upper() for t in issue_types]
    if status is None and not issue_types:
        return 'NOT_IN_STATUS_FILE'
    if any(t.startswith('FETCH') for t in types) or latest_issue.startswith('FETCH') or \
            (code and str(code).strip().isdigit() and not is_2xx(code)):
        return 'FETCH_FAILED'
    if any(t.startswith('SUCCESS_ZERO_DOCS') for t in types) or latest_issue.startswith('SUCCESS_ZERO_DOCS'):
        return 'PAGE_LOADED_NO_DOCUMENTS'
    if any(t.startswith('BROWSER') for t in types) or latest_issue.startswith('BROWSER'):
        return 'BROWSER_INCOMPLETE'
    try:
        found = int(float((status or {}).get('last_working_docs_count') or 0))
    except ValueError:
        found = 0
    if found > 0 and is_2xx(code):
        return 'DOCS_LISTED_UNDER_ANOTHER_LINK'
    return 'UNCLEAR'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--groups', default='p1,p2_p3')
    ap.add_argument('--out', default='source_coverage.csv')
    args = ap.parse_args()
    groups = [g.strip() for g in args.groups.split(',') if g.strip()]
    for g in groups:
        if g not in GROUPS:
            sys.exit(f'Unknown group: {g}')

    rows_out, lines = [], []
    seen_in = defaultdict(list)
    cache = {}

    def load(path):
        if path not in cache:
            cache[path] = read_rows(path)
        return cache[path]

    for g in groups:
        cfg = GROUPS[g]
        links = load(cfg['source'])
        if links is None:
            lines.append(f'- {g}: link file {cfg["source"]} not found, skipped')
            continue
        sources = []
        for r in links:
            url = clean(r.get('source_url'))
            if url and url not in sources:
                sources.append(url)
        for url in sources:
            seen_in[url].append(g)

        out_count, known_count = Counter(), Counter()
        for r in load(cfg['output']) or []:
            out_count[clean(r.get('company'))] += 1
        for r in load(cfg['known']) or []:
            known_count[clean(r.get('company'))] += 1
        status = {clean(r.get('url')): r for r in load(cfg['status']) or []}
        issue_rows, issue_types = Counter(), defaultdict(Counter)
        for r in load(cfg['issues']) or []:
            s = clean(r.get('source_url'))
            issue_rows[s] += 1
            issue_types[s][(r.get('issue_type') or '').strip()] += 1

        for url in sources:
            st = status.get(url)
            types = list(issue_types[url])
            result = classify(out_count[url], known_count[url], st, types)
            rows_out.append({
                'group': g, 'source_url': url,
                'documents_in_output': out_count[url], 'documents_in_known': known_count[url],
                'latest_status_code': (st or {}).get('latest_status_code', ''),
                'latest_issue': (st or {}).get('latest_issue', ''),
                'last_working_docs_count': (st or {}).get('last_working_docs_count', ''),
                'issue_rows': issue_rows[url],
                'issue_types': '; '.join(f'{t} x{n}' for t, n in issue_types[url].most_common()),
                'result': result, 'result_explained': RESULTS[result]})

    both = sum(1 for v in seen_in.values() if len(v) > 1)
    with open(args.out, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()) if rows_out else ['group'])
        w.writeheader()
        w.writerows(rows_out)

    out = ['## Link coverage report', '']
    out += lines
    for g in groups:
        mine = [r for r in rows_out if r['group'] == g]
        if not mine:
            continue
        n_issue_links = sum(1 for r in mine if r['issue_rows'])
        n_issue_rows = sum(r['issue_rows'] for r in mine)
        known_links = sum(1 for r in mine if r['documents_in_known'])
        out += [f'### {g}', '',
                f'- Links in the link file: **{len(mine)}**',
                f'- Links with at least one document in known_documents.csv: **{known_links}**',
                f'- Issue rows in the issues file: **{n_issue_rows}**, spread over **{n_issue_links}** links',
                '', '| Result | Links |', '|---|---|']
        for result, n in Counter(r['result'] for r in mine).most_common():
            out.append(f'| {result} | {n} |')
        out.append('')
    if len(groups) > 1:
        out.append(f'Links that appear in more than one group file: **{both}**\n')
    kn = load('known_documents.csv')
    if kn is not None and any(g in ('p1', 'p2_p3') for g in groups):
        out.append(f'known_documents.csv overall: **{len(kn)}** rows, '
                   f'**{len({clean(r.get("company")) for r in kn})}** different links, '
                   f'**{len({r.get("document_url") for r in kn})}** different documents.\n')
    out.append('Result meanings:\n')
    for k, v in RESULTS.items():
        out.append(f'- `{k}`: {v}')
    text = '\n'.join(out)
    print(text)
    target = os.environ.get('GITHUB_STEP_SUMMARY')
    if target:
        with open(target, 'a', encoding='utf-8') as f:
            f.write(text + '\n')


if __name__ == '__main__':
    main()
