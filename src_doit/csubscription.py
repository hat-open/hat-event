from pathlib import Path

from hat.doit import common
from hat.doit.c import (get_py_ext_suffix,
                        get_py_c_flags,
                        get_py_ld_flags,
                        get_py_ld_libs,
                        CBuild)


__all__ = ['task_csubscription',
           'task_csubscription_obj',
           'task_csubscription_dep',
           'task_csubscription_cleanup']


# py_limited_api = next(iter(common.PyVersion))
py_limited_api = None

build_dir = Path('build')
deps_dir = Path('deps')
src_c_dir = Path('src_c')
src_py_dir = Path('src_py')

py_ext_suffix = get_py_ext_suffix(py_limited_api=py_limited_api)
csubscription_path = (
    src_py_dir /
    'hat/event/common/_csubscription').with_suffix(py_ext_suffix)


def task_csubscription():
    """Build csubscription"""
    yield from _build.get_task_lib(csubscription_path)


def task_csubscription_obj():
    """Build csubscription .o files"""
    yield from _build.get_task_objs()


def task_csubscription_dep():
    """Build csubscription .d files"""
    yield from _build.get_task_deps()


def task_csubscription_cleanup():
    """Cleanup csubscription"""
    return {'actions': [_cleanup]}


def _cleanup():
    for path in csubscription_path.parent.glob('_csubscription.*'):
        if path == csubscription_path:
            continue
        common.rm_rf(path)


def _get_c_flags():
    yield from get_py_c_flags(py_limited_api=py_limited_api)
    yield f"-I{deps_dir / 'hat-util/src_c'}"
    yield '-DMODULE_NAME="_csubscription"'
    yield '-fPIC'
    yield '-O2'
    # yield '-O0'
    # yield '-ggdb'


def _get_ld_flags():
    yield from get_py_ld_flags(py_limited_api=py_limited_api)


def _get_ld_libs():
    yield from get_py_ld_libs(py_limited_api=py_limited_api)


_build = CBuild(src_paths=[*(src_c_dir / 'py/_csubscription').rglob('*.c'),
                           deps_dir / 'hat-util/src_c/hat/py_allocator.c',
                           deps_dir / 'hat-util/src_c/hat/ht.c'],
                build_dir=(build_dir / 'csubscription' /
                           f'{common.target_platform.name.lower()}_'
                           f'{common.target_py_version.name.lower()}'),
                c_flags=list(_get_c_flags()),
                ld_flags=list(_get_ld_flags()),
                ld_libs=list(_get_ld_libs()),
                task_dep=['deps',
                          'csubscription_cleanup'])
