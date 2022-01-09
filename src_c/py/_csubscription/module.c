#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdbool.h>
#include <hat/ht.h>
#include "allocator.h"


typedef struct {
    bool is_leaf;
    hat_ht_t *children;
} node_t;

// clang-format off
typedef struct {
    PyObject_HEAD
    node_t root;
} Subscription;
// clang-format on


static PyObject *Subscription_new(PyTypeObject *type, PyObject *args,
                                  PyObject *kwds) {

    // TODO parse args/kwds

    Subscription *self = (Subscription *)type->tp_alloc(type, 0);
    if (!self)
        return NULL;

    // TODO allocate nodes

    return (PyObject *)self;
}

static void Subscription_dealloc(Subscription *self) {
    // TODO free nodes
    Py_TYPE(self)->tp_free((PyObject *)self);
}


static PyObject *Subscription_get_query_types(Subscription *self,
                                              PyObject *args) {
    return NULL;
}


static PyObject *Subscription_matches(Subscription *self, PyObject *args) {
    return NULL;
}


static PyObject *Subscription_union(Subscription *self, PyObject *args) {
    return NULL;
}


static PyObject *Subscription_isdisjoint(Subscription *self, PyObject *args) {
    return NULL;
}


PyMethodDef Subscription_Methods[] = {
    {.ml_name = "get_query_types",
     .ml_meth = (PyCFunction)Subscription_get_query_types,
     .ml_flags = METH_NOARGS,
     .ml_doc = "Calculate sanitized query event types"},
    {.ml_name = "matches",
     .ml_meth = (PyCFunction)Subscription_matches,
     .ml_flags = METH_O,
     .ml_doc = "Does `event_type` match subscription"},
    {.ml_name = "union",
     .ml_meth = (PyCFunction)Subscription_union,
     .ml_flags = METH_VARARGS,
     .ml_doc = "Create new subscription including event types from this and "
               "other subscriptions"},
    {.ml_name = "isdisjoint",
     .ml_meth = (PyCFunction)Subscription_isdisjoint,
     .ml_flags = METH_O,
     .ml_doc = "Return ``True`` if this subscription has no event types in "
               "common with other subscription"},
    {NULL}};


// clang-format off
PyTypeObject Subscription_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "hat.event.common._csubscription.Subscription",
    .tp_basicsize = sizeof(Subscription),
    .tp_doc = "Subscription defined by query event types",
    .tp_new = Subscription_new,
    .tp_dealloc = (destructor)Subscription_dealloc,
    .tp_methods = Subscription_Methods};
// clang-format on


PyModuleDef module_def = {.m_base = PyModuleDef_HEAD_INIT,
                          .m_name = "_csubscription"};


PyMODINIT_FUNC PyInit__csubscription() {
    if (PyType_Ready(&Subscription_Type))
        return NULL;

    PyObject *module = PyModule_Create(&module_def);
    if (!module)
        return NULL;

    Py_INCREF(&Subscription_Type);
    if (PyModule_AddObject(module, "Connection",
                           (PyObject *)&Subscription_Type)) {
        Py_DECREF(module);
        return NULL;
    }

    return module;
}
