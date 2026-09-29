#!/usr/bin/env python3
"""Synthetic verifier regressions; these are never runtime qualification evidence."""
import copy
import hashlib
import json
from pathlib import Path
import struct
import tempfile
import unittest

import v07_official_evidence as v


def fixture(public=False):
    config = b'[system.message_deduplication]\nenabled = false\n'
    source = '00000000000040008000000000000001'
    target = '00000000000040008000000000000002'
    expected = []
    key = b'official-interoperability'
    for index, payload in enumerate(v.PAYLOADS):
        message = f'000000000000400080000000000000{index + 3:02x}'
        prefix = (struct.pack('>H', 1) + bytes.fromhex(message + source + target)
            + struct.pack('>QII', 1, 0, len(key)) + key + struct.pack('>Q', len(payload)) + payload)
        canonical = prefix + hashlib.sha256(payload).digest() + hashlib.sha256(v.DOMAIN + prefix).digest()
        expected.append(dict(message_id=message, source=source, target=target, generation=1,
            partition=0, ordering_key_hex=key.hex(), payload_hex=payload.hex(), canonical_hex=canonical.hex()))
    observation = dict(boundary='BrokerAccepted', strong_preflight='Unavailable' if public else 'Unsupported', strong_receipt=False,
        startup_config_hex=config.hex(), startup_config_sha256=v.sha(config), stream_id=1, topic_id=1,
        partition_id=0, expected=expected, observed=[dict(row, offset=i) for i, row in enumerate(expected)])
    if not public:
        observation.update(local_correlation_uuid=source, local_correlation_is_broker_identity=False)
    return observation


class OfficialTests(unittest.TestCase):
    def test_both_observation_scopes(self):
        for public in (False, True):
            v.verify_observation(fixture(public), public=public)

    def test_missing_duplicate_or_reordered_readback_rejected(self):
        for mode in ('missing', 'duplicate', 'reorder', 'extra'):
            value = fixture()
            observed = value['observed']
            if mode == 'missing': observed.pop()
            elif mode == 'duplicate': observed[1] = copy.deepcopy(observed[0])
            elif mode == 'reorder': observed.reverse()
            else: observed.append(copy.deepcopy(observed[0]))
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                v.verify_observation(value)

    def test_corrupted_matching_envelopes_are_independently_rejected(self):
        for field, bad in [('canonical_hex', '00'), ('payload_hex', 'ff'), ('message_id', '0'*32),
                           ('generation', True), ('partition', 2), ('ordering_key_hex', 'ff')]:
            value = fixture()
            value['expected'][0][field] = value['observed'][0][field] = bad
            with self.subTest(field=field), self.assertRaises(ValueError):
                v.verify_observation(value)

    def test_readback_identity_bytes_and_offset_are_all_required(self):
        for field, bad in [('message_id', '0'*32), ('canonical_hex', '00'), ('payload_hex', ''),
                           ('source', '0'*32), ('target', '0'*32), ('generation', True), ('offset', True), ('offset', 8)]:
            value = fixture()
            value['observed'][0][field] = bad
            with self.subTest(field=field), self.assertRaises(ValueError):
                v.verify_observation(value)

    def test_no_strong_claim_or_fake_broker_identity(self):
        for field, bad in [('boundary', 'OsSyncedAccepted'), ('strong_preflight', 'Other'),
                           ('strong_receipt', 0), ('local_correlation_is_broker_identity', True)]:
            value = fixture()
            value[field] = bad
            with self.subTest(field=field), self.assertRaises(ValueError):
                v.verify_observation(value)

    def test_startup_config_has_real_explicit_false_and_matching_bytes(self):
        for config in (b'', b'[system.message_deduplication]\nenabled = true\n',
                       b'[system.message_deduplication]\nenabled = "false"\n',
                       b'[system.message_deduplication]\nenabled = false\nenabled = false\n'):
            value = fixture()
            value['startup_config_hex'] = config.hex()
            value['startup_config_sha256'] = v.sha(config)
            with self.subTest(config=config), self.assertRaises(ValueError):
                v.verify_observation(value)
        value = fixture()
        value['startup_config_sha256'] = '0'*64
        with self.assertRaises(ValueError): v.verify_observation(value)

    def test_report_source_binary_manifest_and_public_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = dict(schema='chirps-official-devbaseline-v1', artifact_kind='official-development-interoperability',
                publishable=False, profile='dev', source_commit=v.BASELINE, source_tree=v.TREE,
                cargo_lock_sha256=v.LOCK, source_clean=True, binary_size=5, binary_sha256='c'*64)
            raw = json.dumps(manifest).encode()
            (root/'server-manifest.json').write_bytes(raw)
            report = dict(schema='chirps.v0.7.official-interoperability/v1', source_commit='a'*40,
                api_surface='DevelopmentAppendConnection', public_durable_config_validated=False,
                official_manifest_sha256=v.sha(raw), server_binary_sha256='c'*64, server_source_commit=v.BASELINE,
                client_binary_sha256='b'*64, observation=fixture(), cleanup=dict(graceful=True,forced=False), result='pass')
            path = root/'report.json'
            path.write_text(json.dumps(report))
            v.verify_report(path, 'a'*40, 'b'*64)
            with self.assertRaises(ValueError): v.verify_report(path,'a'*40,'b'*64,require_public=True)
            for field, bad in [('source_commit','d'*40), ('client_binary_sha256','d'*64),
                               ('official_manifest_sha256','d'*64), ('server_source_commit','d'*40),
                               ('cleanup',dict(graceful=1,forced=False)), ('public_durable_config_validated',True)]:
                changed = dict(report, **{field:bad})
                path.write_text(json.dumps(changed))
                with self.subTest(field=field), self.assertRaises(ValueError): v.verify_report(path,'a'*40,'b'*64)
            report.update(schema='chirps.v0.7.official-interoperability/v2', api_surface='DurableConfig',
                public_durable_config_validated=True, observation=fixture(True))
            path.write_text(json.dumps(report))
            v.verify_report(path,'a'*40,'b'*64,require_public=True)
            manifest['source_clean'] = False
            raw = json.dumps(manifest).encode()
            (root/'server-manifest.json').write_bytes(raw)
            report['official_manifest_sha256'] = v.sha(raw)
            path.write_text(json.dumps(report))
            with self.assertRaises(ValueError): v.verify_report(path,'a'*40,'b'*64,require_public=True)


if __name__ == '__main__':
    unittest.main()
