"""Extract exactly one archived snapshot and verify its SHA-256."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import zipfile

ROOT = Path(__file__).resolve().parents[1]
MEMBER = ('BIGDATA_NHOM_3/data/raw/paysim/ingest_date=2026-10-23/'
          'run_20261023T180622Z_c45ac87c/paysim_data.csv')


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--zip', required=True, type=Path)
    args = parser.parse_args()
    cfg = json.loads((ROOT / 'config/config.json').read_text(encoding='utf-8'))
    target = ROOT / cfg['input']
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if sha256(target) != cfg['expected_sha256']:
            raise RuntimeError('Existing raw has a different hash; no file overwritten.')
        print('Raw already exists and hash matches:', target)
        return
    temporary = target.with_suffix('.csv.partial')
    with zipfile.ZipFile(args.zip) as archive, archive.open(MEMBER) as source:
        with temporary.open('xb') as destination:
            shutil.copyfileobj(source, destination, length=8 * 1024 * 1024)
    actual = sha256(temporary)
    if actual != cfg['expected_sha256']:
        raise RuntimeError(f'Snapshot hash mismatch: {actual}')
    temporary.rename(target)
    (target.parent / 'checkpoint3_input.json').write_text(json.dumps({
        'archive': str(args.zip.resolve()), 'member': MEMBER,
        'local_path': str(target.resolve()), 'sha256': actual,
        'bytes': target.stat().st_size,
        'note': 'Relocation record for CP3; original CP2 manifests remain unchanged.'
    }, ensure_ascii=False, indent=2), encoding='utf-8')
    print('Raw prepared:', target)


if __name__ == '__main__':
    main()
