#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "allocator.h"


static void *py_alloc(hat_allocator_t *a, size_t size, void *old) {
    if (size < 1) {
        PyMem_Free(old);
        return NULL;
    }
    return PyMem_Realloc(old, size);
}


hat_allocator_t py_allocator = {.ctx = NULL, .alloc = py_alloc};
