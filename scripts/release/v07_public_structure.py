#!/usr/bin/env python3
"""Read-only public E2E source inventory; not complete RELEASE-STRUCTURE."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import re
import sys
import tomllib

sys.dont_write_bytecode = True
from v07_e2e_evidence import TARGETS

COMPANIONS = {
    'durable_server_faults': 'durable_send',
    'durable_retention_window': 'durable_shutdown',
    'durable_owner': 'durable_creation',
    'durable_checkpoint_recovery': 'durable_checkpoint',
    'durable_retention': 'durable_poll',
    'durable_redelivery': 'durable_delivery',
    'durable_capacity': 'durable_compaction',
    'durable_bootstrap_security': 'durable_diagnostics',
}
LANE_COMPANIONS = {'production': tuple(COMPANIONS), 'fault': ('durable_server_faults',)}
SCHEMA_PATH = 'docs/release/v0.7.0-evidence-schema.json'


def require(value, message):
    if not value:
        raise ValueError(message)


def regular(root, relative):
    path = root / relative
    require(path.is_file() and not any(p.is_symlink() for p in (path, *path.parents)),
            f'missing or symlinked public source: {relative}')
    require(path.resolve().is_relative_to(root.resolve()), 'source path escapes checkout')
    return path


def rust_tokens(text):
    """Only tokenize file-module syntax; never interpret a string as Rust code."""
    tokens = []
    i = 0
    while i < len(text):
        if text[i].isspace():
            i += 1
        elif text.startswith('//', i):
            end = text.find('\n', i)
            i = len(text) if end < 0 else end
        elif text.startswith('/*', i):
            depth = 1
            i += 2
            while depth and i < len(text):
                if text.startswith('/*', i): depth += 1; i += 2
                elif text.startswith('*/', i): depth -= 1; i += 2
                else: i += 1
            require(depth == 0, 'unterminated Rust comment')
        elif raw := re.match(r'(?:br|cr|r)(#*)"', text[i:]):
            start = i + raw.end()
            end = text.find('"' + raw[1], start)
            require(end >= 0, 'unterminated raw string')
            tokens.append(('string', text[start:end]))
            i = end + 1 + len(raw[1])
        elif text[i] == '"':
            start = i + 1
            i = start
            while i < len(text) and text[i] != '"':
                i += 2 if text[i] == '\\' else 1
            require(i < len(text), 'unterminated string')
            tokens.append(('string', text[start:i]))
            i += 1
        elif char := re.match(r"'(?:[^'\\\n]|\\(?:[^\n]|u\{[0-9a-fA-F_]+\}))'", text[i:]):
            i += char.end()
        elif word := re.match(r'[a-zA-Z_][a-zA-Z_0-9]*', text[i:]):
            tokens.append(('code', word[0]))
            i += word.end()
        else:
            tokens.append(('code', text[i]))
            i += 1
    return tokens


def module_paths(path):
    tokens = rust_tokens(path.read_text())
    result = []
    depth = 0
    for i, token in enumerate(tokens):
        if token == ('code', '{'): depth += 1
        elif token == ('code', '}'): depth -= 1
        if token != ('code', 'mod') or i + 2 >= len(tokens) or tokens[i + 2] != ('code', ';'):
            continue
        require(depth == 0, 'nested external module syntax is unsupported')
        kind, name = tokens[i + 1]
        require(kind == 'code' and re.fullmatch('[a-z_][a-z_0-9]*', name), 'unsupported module name')
        previous = i - 1 if i and tokens[i - 1] == ('code', 'pub') else i
        require(not previous or tokens[previous - 1] != ('code', ')'), 'unsupported file-module visibility')
        override = None
        if previous and tokens[previous - 1] == ('code', ']'):
            attribute = tokens[max(0, previous - 6):previous]
            require(len(attribute) == 6 and attribute[:4] == [('code', '#'), ('code', '['), ('code', 'path'), ('code', '=')]
                    and attribute[4][0] == 'string', 'unsupported file-module attribute')
            override = attribute[4][1]
            require(re.fullmatch(r'[a-zA-Z_0-9./-]+', override), 'unsupported module path literal')
            require(previous < 7 or tokens[previous - 7] != ('code', ']'), 'multiple file-module attributes are unsupported')
        result.append((name, override or f'{name}.rs'))
    return result


def validate_sources(root, strict_lanes=('production', 'fault')):
    root = Path(root).resolve(strict=True)
    manifest = tomllib.loads(regular(root, 'tests/e2e/Cargo.toml').read_text())
    require(manifest.get('package', {}).get('autotests') is False, 'E2E autotests must be false')
    require(set(TARGETS) == {'production', 'fault'} and len(TARGETS['production']) == 10
            and len(TARGETS['fault']) == 4, 'strict lane allowlist must contain production 10/fault 4')
    for names in TARGETS.values():
        require(len(set(names)) == len(names), 'duplicate strict lane target')
    primaries = set().union(*map(set, TARGETS.values()))
    entries = manifest.get('test', [])
    names = [entry.get('name') for entry in entries]
    require(len(names) == len(set(names)), 'duplicate Cargo test target')
    declared = {entry.get('name'): entry.get('path') for entry in entries}
    for name in primaries & set(declared):
        require(declared[name] == f'tests/{name}.rs', f'wrong primary path: {name}')
    require(not (set(COMPANIONS) & set(declared)), 'companion must not be a standalone Cargo test')
    for name, relative in declared.items():
        require(isinstance(name, str) and isinstance(relative, str), 'explicit Cargo target/path required')
        regular(root, f'tests/e2e/{relative}')
        if name.startswith('durable_'):
            require(name in primaries, f'unknown primary target: {name}')
    source_root = root / 'tests/e2e/tests'
    files = {p.relative_to(root / 'tests/e2e').as_posix(): p for p in source_root.rglob('*.rs')}
    for relative, path in files.items():
        regular(root, f'tests/e2e/{relative}')
        if path.stem.startswith('durable_'):
            require(path.stem in primaries | set(COMPANIONS), f'unknown durable source: {path.stem}')
    edges = {}
    for relative, path in files.items():
        edges[relative] = {}
        for name, child in module_paths(path):
            child_path = path.parent / child
            require(not Path(child).is_absolute() and '..' not in Path(child).parts,
                    f'module path escapes source: {name}')
            target = child_path.relative_to(root / 'tests/e2e').as_posix()
            require(target in files, f'missing module source: {name}')
            edges[relative][name] = target
    for companion, owner in COMPANIONS.items():
        if f'tests/{companion}.rs' in files:
            require(edges.get(f'tests/{owner}.rs', {}).get(companion) == f'tests/{companion}.rs',
                    f'companion is not imported by primary: {companion}')
    seen = set()
    def visit(relative):
        if relative in seen:
            return
        seen.add(relative)
        for target in edges.get(relative, {}).values():
            visit(target)
    for relative in declared.values():
        visit(relative)
    require(set(files) <= seen, 'orphan E2E source: ' + ', '.join(sorted(set(files) - seen)))
    for lane in strict_lanes:
        for name in (*TARGETS[lane], *LANE_COMPANIONS[lane]):
            require(f'tests/{name}.rs' in files, f'strict {lane} missing source: {name}')
        for name in TARGETS[lane]:
            require(declared.get(name) == f'tests/{name}.rs', f'strict {lane} missing allowlisted Cargo target: {name}')
    return declared


def validate_schema(root):
    schema = json.loads(regular(Path(root).resolve(), SCHEMA_PATH).read_text())
    # Reuse the production schema contract checker, without running evidence
    # collection or importing private specification inputs.
    import importlib.util
    spec = importlib.util.spec_from_file_location('structure_schema', Path(__file__).with_name('verify-v0.7-evidence.py'))
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    verifier.validate_schema_contract(schema)


def validate_manifests(root):
    import importlib.util
    spec = importlib.util.spec_from_file_location('structure_manifests', Path(__file__).with_name('prepare-v07-iggy-source.py'))
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    def read(relative):
        path = regular(Path(root).resolve(), relative)
        require(path.stat().st_size <= verifier.MAX_INPUT, 'source manifest size limit exceeded')
        with path.open('rb') as stream:
            value = stream.read(verifier.MAX_INPUT + 1)
        require(len(value) <= verifier.MAX_INPUT, 'source manifest size limit exceeded')
        return value
    commit = tomllib.loads(read(verifier.MANIFEST).decode())['source']['commit']
    verifier.validate_source_contract(read, commit)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, required=True)
    parser.add_argument('--lane', choices=tuple(TARGETS))
    parser.add_argument('--mode', choices=('strict', 'materialized', 'target', 'perf-fixture'))
    parser.add_argument('--target', default='')
    args = parser.parse_args()
    try:
        if args.lane is None:
            require(args.mode is None and not args.target, 'lane required for selection')
            validate_sources(args.source_root)
            validate_schema(args.source_root)
            validate_manifests(args.source_root)
            print('v0.7 public source/schema subset validated; complete RELEASE-STRUCTURE not established')
            return
        require(args.mode is not None, 'lane selection mode required')
        declared = validate_sources(args.source_root, (args.lane,) if args.mode == 'strict' else ())
        targets = TARGETS[args.lane]
        if args.mode == 'target':
            require(args.target in targets and args.target in declared, 'unknown or undeclared lane target')
            selected = (args.target,)
        elif args.mode == 'materialized':
            selected = tuple(name for name in targets if name in declared)
            require(selected, 'materialized lane has no primary targets')
        else:
            require(args.mode != 'perf-fixture' or args.lane == 'production', 'fixture requires production')
            selected = targets
        print('\n'.join(selected))
    except (ValueError, OSError, KeyError, TypeError) as error:
        parser.exit(1, f'public structure rejected: {error}\n')


if __name__ == '__main__':
    main()
