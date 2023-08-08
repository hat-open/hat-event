from .pymodules import *  # NOQA

from pathlib import Path
import sys

from hat import json
from hat import sbs
from hat.doit import common
from hat.doit.c import get_task_clang_format
from hat.doit.docs import (build_sphinx,
                           build_pdoc)
from hat.doit.py import (get_task_build_wheel,
                         get_task_run_pytest,
                         get_task_run_pip_compile,
                         run_flake8,
                         get_py_versions)

from .pymodules import py_limited_api

from . import pymodules


__all__ = ['task_clean_all',
           'task_build',
           'task_check',
           'task_test',
           'task_docs',
           'task_json_schema_repo',
           'task_sbs_repo',
           'task_peru',
           'task_format',
           'task_pip_compile',
           *pymodules.__all__]


build_dir = Path('build')
src_py_dir = Path('src_py')
src_c_dir = Path('src_c')
pytest_dir = Path('test_pytest')
docs_dir = Path('docs')
schemas_json_dir = Path('schemas_json')
schemas_sbs_dir = Path('schemas_sbs')

build_py_dir = build_dir / 'py'
build_docs_dir = build_dir / 'docs'

json_schema_repo_path = src_py_dir / 'hat/event/common/json_schema_repo.json'
sbs_repo_path = src_py_dir / 'hat/event/common/sbs_repo.json'


def task_clean_all():
    """Clean all"""
    return {'actions': [(common.rm_rf, [
        build_dir,
        json_schema_repo_path,
        sbs_repo_path,
        *(src_py_dir /
          'hat/event/common/subscription').glob('_csubscription.*')
        ])]}


def task_build():
    """Build"""
    return get_task_build_wheel(
        src_dir=src_py_dir,
        build_dir=build_py_dir,
        scripts={
            'hat-event-server': 'hat.event.server.main:main',
            'hat-event-lmdb-manager': 'hat.event.server.backends.lmdb.manager.main:main'},  # NOQA
        py_versions=get_py_versions(py_limited_api),
        py_limited_api=py_limited_api,
        platform=common.target_platform,
        has_ext_modules=True,
        task_dep=['json_schema_repo',
                  'sbs_repo',
                  'pymodules'])


def task_check():
    """Check with flake8"""
    return {'actions': [(run_flake8, [src_py_dir]),
                        (run_flake8, [pytest_dir])]}


def task_test():
    """Test"""
    return get_task_run_pytest(task_dep=['json_schema_repo',
                                         'sbs_repo',
                                         'pymodules'])


def task_docs():
    """Docs"""

    def build():
        build_sphinx(src_dir=docs_dir,
                     dst_dir=build_docs_dir,
                     project='hat-event',
                     extensions=['sphinx.ext.graphviz',
                                 'sphinxcontrib.plantuml',
                                 'sphinxcontrib.programoutput'])
        build_pdoc(module='hat.event',
                   dst_dir=build_docs_dir / 'py_api')

    return {'actions': [build],
            'task_dep': ['json_schema_repo',
                         'sbs_repo',
                         'pymodules']}


def task_json_schema_repo():
    """Generate JSON Schema Repository"""
    src_paths = list(schemas_json_dir.rglob('*.yaml'))

    def generate():
        repo = json.SchemaRepository(*src_paths)
        data = repo.to_json()
        json.encode_file(data, json_schema_repo_path, indent=None)

    return {'actions': [generate],
            'file_dep': src_paths,
            'targets': [json_schema_repo_path]}


def task_sbs_repo():
    """Generate SBS repository"""
    src_paths = list(schemas_sbs_dir.rglob('*.sbs'))

    def generate():
        repo = sbs.Repository(*src_paths)
        data = repo.to_json()
        json.encode_file(data, sbs_repo_path, indent=None)

    return {'actions': [generate],
            'file_dep': src_paths,
            'targets': [sbs_repo_path]}


def task_peru():
    """Peru"""
    return {'actions': [f'{sys.executable} -m peru sync']}


def task_format():
    """Format"""
    yield from get_task_clang_format([*src_c_dir.rglob('*.c'),
                                      *src_c_dir.rglob('*.h')])


def task_pip_compile():
    """Run pip-compile"""
    return get_task_run_pip_compile()
