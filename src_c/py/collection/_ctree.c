#include <Python.h>


typedef struct {
    PyObject ob_base;
} Collection;


static PyObject *Collection_new(PyTypeObject *type, PyObject *args,
                                PyObject *kwds) {
    Collection *self = (Collection *)PyType_GenericAlloc(type, 0);
    if (!self)
        return NULL;

    return (PyObject *)self;
}


static void Collection_dealloc(Collection *self) {
    PyTypeObject *tp = Py_TYPE(self);
    PyObject_Free(self);
    Py_DECREF(tp);
}


static PyObject *Collection_add(Collection *self, PyObject *args) {

    PyObject *subscription;
    PyObject *value;

    if (!PyArg_ParseTuple(args, "OO", &subscription, &value))
        return NULL;


    return NULL;
}


static PyObject *Collection_remove(Collection *self, PyObject *value) {
    return NULL;
}


static PyObject *Collection_get(Collection *self, PyObject *event_type) {
    return NULL;
}


static PyMethodDef collection_methods[] = {
    {.ml_name = "add",
     .ml_meth = (PyCFunction)Collection_add,
     .ml_flags = METH_VARARGS},
    {.ml_name = "remove",
     .ml_meth = (PyCFunction)Collection_remove,
     .ml_flags = METH_O},
    {.ml_name = "get",
     .ml_meth = (PyCFunction)Collection_get,
     .ml_flags = METH_O},
    {NULL}};

static PyType_Slot collection_type_slots[] = {
    {Py_tp_new, Collection_new},
    {Py_tp_dealloc, Collection_dealloc},
    {Py_tp_methods, collection_methods},
    {0, NULL}};

static PyType_Spec collection_type_spec = {
    .name = "hat.event.common.collection._ctree.Collection",
    .basicsize = sizeof(Collection),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HEAPTYPE,
    .slots = collection_type_slots};


static int module_exec(PyObject *module) {
    PyObject *collection_type =
        PyType_FromModuleAndSpec(module, &collection_type_spec, NULL);
    if (!collection_type)
        return -1;

    int result = PyModule_AddObjectRef(module, "Collection", collection_type);
    Py_DECREF(collection_type);
    return result;
}


static PyModuleDef_Slot module_slots[] = {{Py_mod_exec, module_exec},
                                          {0, NULL}};

static PyModuleDef module_def = {.m_base = PyModuleDef_HEAD_INIT,
                                 .m_name = "_ctree",
                                 .m_slots = module_slots};


PyMODINIT_FUNC PyInit__ctree() { return PyModuleDef_Init(&module_def); }
