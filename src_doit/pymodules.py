from pathlib import Path

from hat.doit import common
from hat.doit.c import (get_py_ext_suffix,
                        get_py_c_flags,
                        get_py_ld_flags,
                        get_py_ld_libs,
                        CBuild)


__all__ = ['task_pymodules',
           'task_pymodules_subscription',
           'task_pymodules_subscription_obj',
           'task_pymodules_subscription_dep',
           'task_pymodules_subscription_cleanup']


py_limited_api = None
# py_limited_api = common.PyVersion.CP310  # PyUnicode_AsUTF8AndSize
py_ext_suffix = get_py_ext_suffix(py_limited_api=py_limited_api)

build_dir = Path('build')
peru_dir = Path('peru')
src_c_dir = Path('src_c')
src_py_dir = Path('src_py')

pymodules_build_dir = build_dir / 'pymodules'

subscription_path = (src_py_dir /
                     'hat/event/common/subscription/_csubscription'
                     ).with_suffix(py_ext_suffix)
subscription_src_paths = [src_c_dir / 'py/subscription/_csubscription.c',
                          peru_dir / 'hat-util/src_c/hat/py_allocator.c',
                          peru_dir / 'hat-util/src_c/hat/ht.c']
subscription_build_dir = (pymodules_build_dir / 'subscription' /
                          f'{common.target_platform.name.lower()}_'
                          f'{common.target_py_version.name.lower()}')
subscription_c_flags = [*get_py_c_flags(py_limited_api=py_limited_api),
                        f"-I{peru_dir / 'hat-util/src_c'}",
                        '-fPIC',
                        '-O2',
                        # '-O0',
                        # '-ggdb'
                        ]
subscription_ld_flags = [*get_py_ld_flags(py_limited_api=py_limited_api)]
subscription_ld_libs = [*get_py_ld_libs(py_limited_api=py_limited_api)]

subscription_build = CBuild(src_paths=subscription_src_paths,
                            build_dir=subscription_build_dir,
                            c_flags=subscription_c_flags,
                            ld_flags=subscription_ld_flags,
                            ld_libs=subscription_ld_libs,
                            task_dep=['pymodules_subscription_cleanup',
                                      'peru'])


def task_pymodules():
    """Build pymodules"""
    return {'actions': None,
            'task_dep': ['pymodules_subscription']}


def task_pymodules_subscription():
    """Build pymodules subscription"""
    yield from subscription_build.get_task_lib(subscription_path)


def task_pymodules_subscription_obj():
    """Build pymodules subscription .o files"""
    yield from subscription_build.get_task_objs()


def task_pymodules_subscription_dep():
    """Build pymodules subscription .d files"""
    yield from subscription_build.get_task_deps()


def task_pymodules_subscription_cleanup():
    """Cleanup pymodules subscription"""

    def cleanup():
        for path in subscription_path.parent.glob('_csubscription.*'):
            if path == subscription_path:
                continue
            common.rm_rf(path)

    return {'actions': [cleanup]}
