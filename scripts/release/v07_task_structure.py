#!/usr/bin/env python3
"""Read and verify exact private task bytes without disclosing their contents."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

MAX_INPUT_BYTES = 4 * 1024 * 1024
MAX_TASKS = 10000
MAX_REFERENCES = 100000
FIELDS = ('Role', 'Task', 'Restrictions', 'Leverage', 'Requirements', 'Validation', 'Success', 'Instructions')
ID = re.compile(r'[0-9]+(?:\.[0-9]+)*')
HEADER = re.compile(r'^- \[([ xX-])\] (\S+)(?:\s+.+)?$')
PROMPT = re.compile(r'\s+- _Prompt:\s*(.*?)_\s*$')


class TaskStructureError(ValueError):
    """Only fixed codes and validated numeric identifiers enter diagnostics."""
    def __init__(self, code, line=None, task_id=None):
        text = 'task-structure: ' + code
        if task_id is not None:
            text += ' task=' + task_id
        if line is not None:
            text += ' line=' + str(line)
        super().__init__(text)


def task_id(value, line):
    if (len(value) > 64 or ID.fullmatch(value) is None
            or len(value.split('.')) > 8
            or any(part != str(int(part)) for part in value.split('.'))):
        raise TaskStructureError('invalid-task-id', line)
    return value


def read_id(text, position, line):
    match = ID.match(text, position)
    if match is None:
        raise TaskStructureError('invalid-reference', line)
    value = task_id(match.group(), line)
    end = match.end()
    if end < len(text):
        suffix = text[end]
        if suffix.isalnum() or suffix == '_':
            raise TaskStructureError('invalid-reference', line)
        # A sentence-ending period is punctuation; a malformed dotted ID is not.
        if suffix == '.' and end + 1 < len(text) and not (text[end + 1].isspace() or text[end + 1] in ')]},;:'):
            raise TaskStructureError('invalid-reference', line)
    return value, end


def references(line_text, line_number):
    # Inline code/bold do not exempt a textual Task reference from validation.
    text = line_text.replace('`', '').replace('*', '')
    for marker in re.finditer(r'\bTasks?\s+(?=[0-9+-])', text):
        position = marker.end()
        while True:
            first, position = read_id(text, position, line_number)
            values = [first]
            tail = re.match(r'\s*[-–]\s*', text[position:])
            if tail:
                last, position = read_id(text, position + tail.end(), line_number)
                start_parts, end_parts = first.split('.'), last.split('.')
                if start_parts[:-1] != end_parts[:-1]:
                    raise TaskStructureError('invalid-reference-range', line_number)
                low, high = int(start_parts[-1]), int(end_parts[-1])
                if high < low or high - low >= MAX_TASKS:
                    raise TaskStructureError('invalid-reference-range', line_number)
                prefix = '.'.join(start_parts[:-1])
                values = [(prefix + '.' if prefix else '') + str(n) for n in range(low, high + 1)]
            yield from values
            separator = re.match(r'\s*(?:/|,\s*(?:and\s+)?|and\s+)\s*(?=[0-9+-])', text[position:])
            if separator is None:
                break
            position += separator.end()


def verify_bytes(raw, expected_sha256):
    if re.fullmatch(r'[0-9a-f]{64}', expected_sha256) is None:
        raise TaskStructureError('invalid-expected-digest')
    if len(raw) > MAX_INPUT_BYTES:
        raise TaskStructureError('input-too-large')
    actual = hashlib.sha256(raw).hexdigest()
    if actual != expected_sha256:
        raise TaskStructureError('digest-mismatch')
    try:
        lines = raw.decode('utf-8').splitlines()
    except UnicodeDecodeError:
        raise TaskStructureError('invalid-utf8') from None
    tasks, prompts = {}, {}
    owner = None
    for number, line in enumerate(lines, 1):
        if line.startswith('- ['):
            match = HEADER.fullmatch(line)
            if match is None:
                raise TaskStructureError('invalid-task-header', number)
            owner = task_id(match[2], number)
            if owner in tasks:
                raise TaskStructureError('duplicate-task-id', number, owner)
            tasks[owner] = number
            if len(tasks) > MAX_TASKS:
                raise TaskStructureError('too-many-tasks', number)
        if '_Prompt:' not in line:
            continue
        if owner is None:
            raise TaskStructureError('orphan-prompt', number)
        if owner in prompts:
            raise TaskStructureError('duplicate-prompt', number, owner)
        match = PROMPT.fullmatch(line)
        if match is None:
            raise TaskStructureError('invalid-prompt-container', number, owner)
        parts = match[1].split('|')
        # The optional introductory prose precedes the first Role field.
        roles = list(re.finditer(r'\bRole:', parts[0]))
        if (len(parts) != len(FIELDS) or len(roles) != 1
                or re.search(r'\b[A-Z][A-Za-z]*:', parts[0][:roles[0].start()])):
            raise TaskStructureError('invalid-prompt-fields', number, owner)
        parts[0] = parts[0][roles[0].start():]
        for part, expected in zip(parts, FIELDS):
            field = re.fullmatch(r'\s*([A-Za-z]+):\s*(\S.*?)\s*', part)
            if field is None or field[1] != expected:
                raise TaskStructureError('invalid-prompt-fields', number, owner)
        prompts[owner] = number
    if not tasks:
        raise TaskStructureError('no-tasks')
    for identifier, number in tasks.items():
        if identifier not in prompts:
            raise TaskStructureError('missing-prompt', number, identifier)
    reference_count = 0
    for number, line in enumerate(lines, 1):
        for identifier in references(line, number):
            if identifier not in tasks:
                raise TaskStructureError('unknown-reference', number, identifier)
            reference_count += 1
            if reference_count > MAX_REFERENCES:
                raise TaskStructureError('too-many-references', number)
    return {'schema': 'chirps.task-structure/v1', 'source_sha256': actual,
            'task_count': len(tasks), 'prompt_count': len(prompts),
            'reference_count': reference_count, 'result': 'pass'}


def verify_task_structure(path, expected_sha256):
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NONBLOCK', 0))
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                raise TaskStructureError('input-not-regular')
            if info.st_size > MAX_INPUT_BYTES:
                raise TaskStructureError('input-too-large')
            with os.fdopen(fd, 'rb', closefd=False) as stream:
                raw = stream.read(MAX_INPUT_BYTES + 1)
        finally:
            os.close(fd)
    except TaskStructureError:
        raise
    except (OSError, ValueError):
        raise TaskStructureError('input-unreadable') from None
    return verify_bytes(raw, expected_sha256)


class SafeParser(argparse.ArgumentParser):
    def error(self, message):
        # argparse's original message may contain a private path or supplied text.
        raise TaskStructureError('invalid-arguments')


def main(argv=None):
    parser = SafeParser(prog='v07-task-structure', description=__doc__)
    parser.add_argument('--tasks-file', required=True, type=Path)
    parser.add_argument('--expected-sha256', required=True)
    try:
        args = parser.parse_args(argv)
        result = verify_task_structure(args.tasks_file, args.expected_sha256)
    except TaskStructureError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
