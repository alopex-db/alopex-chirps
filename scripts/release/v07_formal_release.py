#!/usr/bin/env python3
"""Require full checker, catalog and exact-Git refinement evidence together."""
from pathlib import Path
from v07_formal_evidence import artifact, read_report, verify_formal_report
from v07_formal_catalog import verify_catalog_report
from v07_formal_refinements import verify_refinements


def verify_release_models(source_root,iggy_root,report_path,source_commit,iggy_commit):
    report_path=Path(report_path).resolve();root=report_path.parent
    report=read_report(report_path)
    fields={'schema','source_commit','iggy_commit','raw','catalog','refinements'}
    if set(report)!=fields or report['schema']!='chirps.formal-release/v1':
        raise ValueError('formal release composite schema differs')
    if report['source_commit']!=source_commit or report['iggy_commit']!=iggy_commit:
        raise ValueError('formal release candidate binding differs')
    paths={}
    for kind in ('raw','catalog','refinements'):
        ref=report[kind]
        if not isinstance(ref,dict) or set(ref)!={'path','sha256'}:
            raise ValueError('formal release reference shape differs')
        artifact(root,ref['path'],ref['sha256'])
        paths[kind]=root/ref['path']
    if len(set(paths.values()))!=3:raise ValueError('formal release references alias')
    raw=verify_formal_report(Path(source_root),paths['raw'],source_commit)
    catalog=verify_catalog_report(Path(source_root),paths['catalog'],source_commit)
    expected=verify_refinements(Path(source_root),source_commit,Path(iggy_root),iggy_commit)
    recorded=read_report(paths['refinements'])
    if recorded!=expected:raise ValueError('formal refinement report differs from exact Git objects')
    return {'status':'pass','source_commit':source_commit,'iggy_commit':iggy_commit,
            'checker_jobs':len(raw['jobs']),'catalog_probes':catalog['probes'],
            'refinement_references':len(expected['references'])}


def package_release_models(source_root,iggy_root,source_commit,iggy_commit,raw,catalog,refinements,output):
    """Create an exclusive composite only after all three real gates succeed."""
    import hashlib
    import json
    import tempfile
    output=Path(output).resolve()
    if output.exists():raise FileExistsError(output)
    report=dict(schema='chirps.formal-release/v1',source_commit=source_commit,iggy_commit=iggy_commit)
    for kind,path in (('raw',raw),('catalog',catalog),('refinements',refinements)):
        path=Path(path).resolve();name=path.relative_to(output.parent).as_posix()
        report[kind]={'path':name,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
    with tempfile.NamedTemporaryFile(mode='w',suffix='.json',prefix='.formal-package-',dir=output.parent) as temporary:
        json.dump(report,temporary,indent=2);temporary.write('\n');temporary.flush()
        verify_release_models(source_root,iggy_root,Path(temporary.name),source_commit,iggy_commit)
        with output.open('x') as stream:
            json.dump(report,stream,indent=2);stream.write('\n')
    return output


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('source-root','iggy-root','raw','catalog','refinements','output'):
        parser.add_argument('--'+name,required=True,type=Path)
    for name in ('source-commit','iggy-commit'):parser.add_argument('--'+name,required=True)
    args=parser.parse_args()
    print(package_release_models(args.source_root,args.iggy_root,args.source_commit,args.iggy_commit,args.raw,args.catalog,args.refinements,args.output))
