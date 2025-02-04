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
           'task_pymodules_subscription_cleanup',
           'task_pymodules_collection',
           'task_pymodules_collection_obj',
           'task_pymodules_collection_dep',
           'task_pymodules_collection_cleanup']


py_limited_api = next(iter(common.PyVersion))
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

collection_path = (src_py_dir / 'hat/event/common/collection/_ctree'
                   ).with_suffix(py_ext_suffix)
collection_src_paths = [src_c_dir / 'py/collection/_ctree.c',
                        peru_dir / 'hat-util/src_c/hat/py_allocator.c',
                        peru_dir / 'hat-util/src_c/hat/ht.c']
collection_build_dir = (pymodules_build_dir / 'collection' /
                        f'{common.target_platform.name.lower()}_'
                        f'{common.target_py_version.name.lower()}')
collection_c_flags = [*get_py_c_flags(py_limited_api=py_limited_api),
                      f"-I{peru_dir / 'hat-util/src_c'}",
                      '-fPIC',
                      '-O2',
                      # '-O0',
                      # '-ggdb'
                      ]
collection_ld_flags = [*get_py_ld_flags(py_limited_api=py_limited_api)]
collection_ld_libs = [*get_py_ld_libs(py_limited_api=py_limited_api)]

collection_build = CBuild(src_paths=collection_src_paths,
                          build_dir=collection_build_dir,
                          c_flags=collection_c_flags,
                          ld_flags=collection_ld_flags,
                          ld_libs=collection_ld_libs,
                          task_dep=['pymodules_collection_cleanup',
                                    'peru'])


def task_pymodules():
    """Build pymodules"""
    return {'actions': None,
            'task_dep': ['pymodules_subscription',
                         'pymodules_collection']}


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


def task_pymodules_collection():
    """Build pymodules collection"""
    yield from collection_build.get_task_lib(collection_path)


def task_pymodules_collection_obj():
    """Build pymodules collection .o files"""
    yield from collection_build.get_task_objs()


def task_pymodules_collection_dep():
    """Build pymodules collection .d files"""
    yield from collection_build.get_task_deps()


def task_pymodules_collection_cleanup():
    """Cleanup pymodules collection"""

    def cleanup():
        for path in collection_path.parent.glob('_ctree.*'):
            if path == collection_path:
                continue
            common.rm_rf(path)

    return {'actions': [cleanup]}
