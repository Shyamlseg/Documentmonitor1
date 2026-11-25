
import argparse
import os
import re
import sys
import tempfile

EXTENSIONS = ('.csv', '.json', '.jsonl')
SKIP_DIRS = {'.git', 'node_modules', '.venv', 'venv', '__pycache__', '.github'}

# Query keys that carry the signature or credentials.
SIGNING_KEY = re.compile(
    r'^(?:x-goog-(?!expires$|date$)[a-z0-9-]+|x-amz-(?!expires$|date$)[a-z0-9-]+|signature|oss-?access-?key-?id|'
    r'access-?key-?id|access-?key-?secret|x-oss-(?!expires$)[a-z0-9-]+|security-?token|googleaccessid|sig)$', re.I)
# Keys that only make sense together with a signature, so they go too when one was removed.
COMPANION_KEY = re.compile(r'^(?:x-goog-expires|x-goog-date|x-amz-expires|x-amz-date|expires)$', re.I)
URL = re.compile(r'https?://(?:&amp;|[^\s"\'<>,;|\\])+', re.I)
ALIBABA_KEY = re.compile(r'\bLTAI[A-Za-z0-9]{12,32}\b')
# Cheap test so ordinary lines are skipped without any regex work.
QUICK = re.compile(r'x-goog-|x-amz-|signature=|accesskey|accesskeyid|security-token|googleaccessid|x-oss-|[?&]sig=|LTAI', re.I)


def clean_url(url):
    """Return (new_url, number_of_parameters_removed)."""
    if '?' not in url:
        return url, 0
    base, _, rest = url.partition('?')
    fragment = ''
    if '#' in rest:
        rest, _, frag = rest.partition('#')
        fragment = '#' + frag
    sep = '&amp;' if '&amp;' in rest else '&'
    parts = rest.split(sep)
    has_signature = any(SIGNING_KEY.match(p.split('=', 1)[0]) for p in parts if p)
    if not has_signature:
        return url, 0
    kept, removed = [], 0
    for part in parts:
        key = part.split('=', 1)[0]
        if part and (SIGNING_KEY.match(key) or COMPANION_KEY.match(key)):
            removed += 1
        else:
            kept.append(part)
    new = base + ('?' + sep.join(kept) if kept else '') + fragment
    return new, removed


def clean_text(text):
    stats = {'urls': 0, 'params': 0, 'keys': 0}

    def repl(match):
        new, removed = clean_url(match.group(0))
        if removed:
            stats['urls'] += 1
            stats['params'] += removed
        return new

    out = URL.sub(repl, text)
    out, n = ALIBABA_KEY.subn('[REMOVED]', out)
    stats['keys'] += n
    return out, stats


def process_file(path):
    total = {'urls': 0, 'params': 0, 'keys': 0}
    changed = False
    handle, tmp = tempfile.mkstemp(prefix='.redact_', dir=os.path.dirname(os.path.abspath(path)))
    try:
        with open(path, encoding='utf-8-sig', errors='surrogateescape', newline='') as src, \
                os.fdopen(handle, 'w', encoding='utf-8', errors='surrogateescape', newline='') as dst:
            bom = open(path, 'rb').read(3) == b'\xef\xbb\xbf'
            if bom:
                dst.write('\ufeff')
            for line in src:
                if QUICK.search(line):
                    new, st = clean_text(line)
                    if new != line:
                        changed = True
                        for k in total:
                            total[k] += st[k]
                    line = new
                dst.write(line)
        if changed:
            os.replace(tmp, path)
        else:
            os.remove(tmp)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return changed, total


def iter_files(roots):
    seen = set()
    for root in roots:
        for folder, dirs, files in os.walk(root):
            dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
            for name in files:
                if name.lower().endswith(EXTENSIONS) and not name.startswith('.redact_'):
                    full = os.path.normpath(os.path.join(folder, name))
                    if full not in seen:
                        seen.add(full)
                        yield full


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--root', action='append', default=None, help='Folder to scan (repeatable). Default: .')
    args = parser.parse_args()
    grand = {'files': 0, 'urls': 0, 'params': 0, 'keys': 0}
    scanned = 0
    for path in iter_files(args.root or ['.']):
        scanned += 1
        try:
            changed, st = process_file(path)
        except OSError as exc:
            print('::warning::could not clean %s (%s)' % (path, exc))
            continue
        if changed:
            grand['files'] += 1
            for k in ('urls', 'params', 'keys'):
                grand[k] += st[k]
            print('cleaned %-60s links=%d signing parameters=%d key ids=%d' % (path, st['urls'], st['params'], st['keys']))
    print('Checked %d file(s).' % scanned)
    if grand['files']:
        print('::warning::Removed signed-link credentials from %d file(s): %d link(s), %d signing parameter(s), '
              '%d key id(s). The links now point to the plain file address.' %
              (grand['files'], grand['urls'], grand['params'], grand['keys']))
    else:
        print('No signed links found.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
