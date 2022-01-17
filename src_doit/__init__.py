from pathlib import Path
import sys

from hat import json
from hat import sbs
from hat.doit import common
from hat.doit.py import (build_wheel,
                         run_pytest,
                         run_flake8)
from hat.doit.docs import (SphinxOutputType,
                           build_sphinx,
                           build_pdoc)

from .csubscription import *  # NOQA
from . import csubscription


__all__ = ['task_clean_all',
           'task_build',
           'task_check',
           'task_test',
           'task_docs',
           'task_json_schema_repo',
           'task_sbs_repo',
           'task_deps',
           'task_format',
           *csubscription.__all__]


build_dir = Path('build')
src_py_dir = Path('src_py')
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
        *(src_py_dir / 'hat/event/common').glob('_csubscription.*')])]}


def task_build():
    """Build"""

    def build():
        build_wheel(
            src_dir=src_py_dir,
            dst_dir=build_py_dir,
            name='hat-event',
            description='Hat event',
            url='https://github.com/hat-open/hat-event',
            license=common.License.APACHE2,
            packages=['hat'],
            console_scripts=['hat-event = hat.event.server.main:main'])

    return {'actions': [build],
            'task_dep': ['json_schema_repo',
                         'sbs_repo',
                         'csubscription']}


def task_check():
    """Check with flake8"""
    return {'actions': [(run_flake8, [src_py_dir]),
                        (run_flake8, [pytest_dir])]}


def task_test():
    """Test"""
    return {'actions': [lambda args: run_pytest(pytest_dir, *(args or []))],
            'pos_arg': 'args',
            'task_dep': ['json_schema_repo',
                         'sbs_repo',
                         'csubscription']}


def task_docs():
    """Docs"""
    return {'actions': [(build_sphinx, [SphinxOutputType.HTML,
                                        docs_dir,
                                        build_docs_dir]),
                        (build_pdoc, ['hat.event',
                                      build_docs_dir / 'py_api'])],
            'task_dep': ['json_schema_repo',
                         'sbs_repo',
                         'csubscription']}


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


def task_deps():
    """Dependencies"""
    return {'actions': [f'{sys.executable} -m peru sync']}


def task_format():
    """Format"""
    files = [*Path('src_c').rglob('*.c'),
             *Path('src_c').rglob('*.h')]
    for f in files:
        yield {'name': str(f),
               'actions': [f'clang-format -style=file -i {f}'],
               'file_dep': [f]}
