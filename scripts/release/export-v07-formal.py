#!/usr/bin/env python3
"""Export only the 14 public formal inputs at an exact Git commit."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import yaml

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('repository',type=Path)
parser.add_argument('commit')
parser.add_argument('output',type=Path)
args=parser.parse_args()
if re.fullmatch('[0-9a-f]{40}',args.commit) is None:
    raise SystemExit('full immutable commit required')
def git(*params):
    return subprocess.check_output(['git','-C',str(args.repository),*params],timeout=30)
if git('rev-parse',args.commit+'^{commit}').decode().strip()!=args.commit:
    raise SystemExit('commit unavailable')
compose=yaml.safe_load(git('show',args.commit+':formal/chirps-durable/compose.yml'))
rows=compose['services']['suite']['environment']['CHIRPS_DURABLE_MODELS'].splitlines()
files=['formal/compose.yml','formal/chirps-durable/compose.yml']
for row in rows:
    files.extend('formal/'+path for path in row.split('|')[1:4])
if len(files)!=14 or len(set(files))!=14:
    raise SystemExit('unexpected source inventory')
args.output.mkdir()
inputs={}
for path in files:
    if '..' in Path(path).parts or Path(path).is_absolute():
        raise SystemExit('unsafe source input')
    raw=git('show',args.commit+':'+path)
    destination=args.output/path
    destination.parent.mkdir(parents=True,exist_ok=True)
    destination.write_bytes(raw)
    inputs[path]=hashlib.sha256(raw).hexdigest()
(args.output/'source.json').write_text(json.dumps({'source_commit':args.commit,'inputs':inputs},indent=2)+'\n')
print(args.output)
