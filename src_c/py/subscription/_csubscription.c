#include <Python.h>
#include <stdbool.h>
#include <hat/ht.h>
#include <hat/py_allocator.h>


typedef struct {
    bool is_leaf;
    bool has_star;
    hat_ht_t *children;
} node_t;


static void free_children(node_t *node) {
    if (!node->children)
        return;

    node_t *child;
    hat_ht_iter_t iter = NULL;
    while ((iter = hat_ht_iter_next(node->children, iter))) {
        hat_ht_iter_value(iter, (void **)&child);
        free_children(child);
        PyMem_Free(child);
    }

    hat_ht_destroy(node->children);
    node->children = NULL;
}


static int add_query_type(node_t *node, PyObject *query_type_iter) {
    if (node->has_star)
        return 0;

    PyObject *subtype = PyIter_Next(query_type_iter);
    if (!subtype) {
        node->is_leaf = true;
        return 0;
    }

    if (!PyUnicode_Check(subtype)) {
        Py_DECREF(subtype);
        PyErr_SetString(PyExc_ValueError, "invalid subtype type");
        return 1;
    }

    Py_ssize_t key_size;
    const char *key = PyUnicode_AsUTF8AndSize(subtype, &key_size);
    if (!key) {
        Py_DECREF(subtype);
        PyErr_SetString(PyExc_RuntimeError, "conversion error");
        return 1;
    }

    if (strcmp(key, "*") == 0) {
        PyObject *next = PyIter_Next(query_type_iter);
        if (next) {
            Py_DECREF(next);
            Py_DECREF(subtype);
            PyErr_SetString(PyExc_ValueError, "invalid query event type");
            return 1;
        }

        node->is_leaf = true;
        node->has_star = true;
        free_children(node);

        return 0;
    }

    if (!node->children) {
        node->children = hat_ht_create(&hat_py_allocator, 8);
        if (!node->children) {
            Py_DECREF(subtype);
            PyErr_SetString(PyExc_RuntimeError, "internal error");
            return 1;
        }
    }

    size_t node_children_count = hat_ht_count(node->children);
    if (node_children_count >= hat_ht_avg_count(node->children)) {
        if (hat_ht_resize(node->children, node_children_count * 2)) {
            Py_DECREF(subtype);
            PyErr_SetString(PyExc_RuntimeError, "internal error");
            return 1;
        }
    }

    node_t *child = hat_ht_get(node->children, (uint8_t *)key, key_size);
    if (!child) {
        child = PyMem_Malloc(sizeof(node_t));
        if (!child) {
            Py_DECREF(subtype);
            PyErr_SetString(PyExc_RuntimeError, "allocation error");
            return 1;
        }

        *child =
            (node_t){.is_leaf = false, .has_star = false, .children = NULL};
        if (hat_ht_set(node->children, (uint8_t *)key, key_size, child)) {
            PyMem_Free(child);
            Py_DECREF(subtype);
            PyErr_SetString(PyExc_RuntimeError, "internal error");
            return 1;
        }
    }
    Py_DECREF(subtype);

    return add_query_type(child, query_type_iter);
}


static int resize_children(node_t *node) {
    if (!node->children)
        return 0;

    node_t *child;
    hat_ht_iter_t iter = NULL;
    while ((iter = hat_ht_iter_next(node->children, iter))) {
        hat_ht_iter_value(iter, (void **)&child);
        if (resize_children(child))
            return 1;
    }

    return hat_ht_resize(node->children, hat_ht_count(node->children));
}


static int get_query_types(node_t *node, PyObject *prefix, PyObject *deque) {
    if (node->has_star) {
        Py_ssize_t query_type_len = PyTuple_Size(prefix) + 1;
        PyObject *query_type = PyTuple_New(query_type_len);

        for (Py_ssize_t i = 0; i < query_type_len - 1; ++i) {
            PyObject *segment = PyTuple_GetItem(prefix, i);
            Py_INCREF(segment);
            PyTuple_SetItem(query_type, i, segment);
        }

        PyObject *segment = PyUnicode_FromString("*");
        if (!segment) {
            Py_DECREF(query_type);
            return 1;
        }
        PyTuple_SetItem(query_type, query_type_len - 1, segment);

        PyObject *result =
            PyObject_CallMethod(deque, "append", "(O)", query_type);
        Py_DECREF(query_type);
        if (!result)
            return 1;

        Py_DECREF(result);
        return 0;
    }

    if (node->is_leaf && !node->has_star) {
        PyObject *result = PyObject_CallMethod(deque, "append", "(O)", prefix);
        if (!result)
            return 1;
        Py_DECREF(result);
    }

    if (!node->children)
        return 0;

    hat_ht_iter_t iter = NULL;
    while ((iter = hat_ht_iter_next(node->children, iter))) {
        size_t key_size;
        uint8_t *key;
        hat_ht_iter_key(iter, &key, &key_size);

        node_t *child;
        hat_ht_iter_value(iter, (void **)&child);

        Py_ssize_t child_prefix_len = PyTuple_Size(prefix) + 1;
        PyObject *child_prefix = PyTuple_New(child_prefix_len);
        if (!child_prefix)
            return 1;

        PyObject *segment;
        for (Py_ssize_t i = 0; i < child_prefix_len - 1; ++i) {
            segment = PyTuple_GetItem(prefix, i);
            Py_INCREF(segment);
            PyTuple_SetItem(child_prefix, i, segment);
        }
        segment = PyUnicode_DecodeUTF8((char *)key, key_size, NULL);
        if (!segment) {
            Py_DECREF(child_prefix);
            return 1;
        }
        PyTuple_SetItem(child_prefix, child_prefix_len - 1, segment);

        int err = get_query_types(child, child_prefix, deque);
        Py_DECREF(child_prefix);
        if (err)
            return 1;
    }

    return 0;
}


static bool matches(node_t *node, PyObject *event_type,
                    Py_ssize_t event_type_size, size_t event_type_index) {
    if (node->has_star)
        return true;

    if (event_type_index >= event_type_size)
        return node->is_leaf;

    if (!node->children)
        return false;

    node_t *child;

    PyObject *subtype = PyTuple_GetItem(event_type, event_type_index);
    Py_ssize_t key_size;
    const char *key = PyUnicode_AsUTF8AndSize(subtype, &key_size);
    if (!key)
        return false;
    child = hat_ht_get(node->children, (uint8_t *)key, key_size);
    if (child &&
        matches(child, event_type, event_type_size, event_type_index + 1))
        return true;

    child = hat_ht_get(node->children, (uint8_t *)"?", 1);
    if (child &&
        matches(child, event_type, event_type_size, event_type_index + 1))
        return true;

    return false;
}


static bool isdisjoint(node_t *first, node_t *second) {
    if (first->is_leaf && second->is_leaf)
        return false;

    if (first->has_star)
        return !second->is_leaf && !second->children;

    if (second->has_star)
        return !first->is_leaf && !first->children;

    if (!first->children)
        return !first->is_leaf || !second->children;

    if (!second->children)
        return !second->is_leaf || !first->children;

    node_t *first_child;
    node_t *second_child;
    hat_ht_iter_t iter = NULL;

    first_child = hat_ht_get(first->children, (uint8_t *)"?", 1);
    if (first_child) {
        while ((iter = hat_ht_iter_next(second->children, iter))) {
            hat_ht_iter_value(iter, (void **)&second_child);

            if (!isdisjoint(first_child, second_child))
                return false;
        }
    }

    second_child = hat_ht_get(second->children, (uint8_t *)"?", 1);
    if (second_child) {
        while ((iter = hat_ht_iter_next(first->children, iter))) {
            hat_ht_iter_value(iter, (void **)&first_child);

            if (!isdisjoint(first_child, second_child))
                return false;
        }
    }

    size_t key_size;
    uint8_t *key;

    while ((iter = hat_ht_iter_next(first->children, iter))) {
        hat_ht_iter_key(iter, &key, &key_size);
        if (key_size == 1 && *key == '?')
            continue;

        second_child = hat_ht_get(second->children, key, key_size);
        if (!second_child)
            continue;

        hat_ht_iter_value(iter, (void **)&first_child);
        if (!isdisjoint(first_child, second_child))
            return false;
    }

    while ((iter = hat_ht_iter_next(second->children, iter))) {
        hat_ht_iter_key(iter, &key, &key_size);
        if (key_size == 1 && *key == '?')
            continue;

        first_child = hat_ht_get(first->children, key, key_size);
        if (!first_child)
            continue;

        hat_ht_iter_value(iter, (void **)&second_child);
        if (!isdisjoint(first_child, second_child))
            return false;
    }

    return true;
}


typedef struct {
    PyObject ob_base;
    node_t root;
} Subscription;


static PyObject *Subscription_new(PyTypeObject *type, PyObject *args,
                                  PyObject *kwds) {
    PyObject *query_types;
    if (!PyArg_ParseTuple(args, "O", &query_types))
        return NULL;

    PyObject *query_types_iter = PyObject_GetIter(query_types);
    if (!query_types_iter)
        return NULL;

    Subscription *self = (Subscription *)PyType_GenericAlloc(type, 0);
    if (!self) {
        Py_DECREF(query_types_iter);
        return NULL;
    }

    self->root =
        (node_t){.is_leaf = false, .has_star = false, .children = NULL};

    while (true) {
        PyObject *query_type = PyIter_Next(query_types_iter);
        if (!query_type)
            break;

        PyObject *query_type_iter = PyObject_GetIter(query_type);
        if (!query_type_iter) {
            Py_DECREF(query_type);
            Py_DECREF(self);
            Py_DECREF(query_types_iter);
            return NULL;
        }

        int err = add_query_type(&(self->root), query_type_iter);
        Py_DECREF(query_type_iter);
        Py_DECREF(query_type);
        if (err) {
            Py_DECREF(self);
            Py_DECREF(query_types_iter);
            return NULL;
        }
    }
    Py_DECREF(query_types_iter);

    if (resize_children(&(self->root))) {
        Py_DECREF(self);
        return NULL;
    }

    return (PyObject *)self;
}


static void Subscription_dealloc(Subscription *self) {
    free_children(&(self->root));

    PyTypeObject *tp = Py_TYPE(self);
    PyObject_Free(self);
    Py_DECREF(tp);
}


static PyObject *Subscription_get_query_types(Subscription *self,
                                              PyObject *args) {
    PyObject *collections = PyImport_ImportModule("collections");
    if (!collections)
        return NULL;

    PyObject *deque_cls = PyObject_GetAttrString(collections, "deque");
    Py_DECREF(collections);
    if (!deque_cls)
        return NULL;

    PyObject *deque = PyObject_CallObject(deque_cls, NULL);
    Py_DECREF(deque_cls);
    if (!deque)
        return NULL;

    PyObject *empty_tuple = PyTuple_New(0);
    if (!empty_tuple) {
        Py_DECREF(deque);
        return NULL;
    }

    int err = get_query_types(&(self->root), empty_tuple, deque);
    Py_DECREF(empty_tuple);
    if (err) {
        Py_DECREF(deque);
        return NULL;
    }

    return deque;
}


static PyObject *Subscription_matches(Subscription *self, PyObject *args) {
    if (!PyTuple_Check(args)) {
        PyErr_SetString(PyExc_ValueError, "event_type is not tuple");
        return NULL;
    }

    Py_ssize_t args_size = PyTuple_Size(args);

    if (matches(&(self->root), args, args_size, 0))
        Py_RETURN_TRUE;

    if (PyErr_Occurred())
        return NULL;

    Py_RETURN_FALSE;
}


static PyObject *Subscription_isdisjoint(Subscription *self, PyObject *args) {
    PyTypeObject *type = Py_TYPE(self);

    if (!PyObject_TypeCheck(args, type)) {
        PyErr_SetString(PyExc_ValueError, "unsuported argument type");
        return NULL;
    }

    Subscription *other = (Subscription *)args;

    return PyBool_FromLong(isdisjoint(&(self->root), &(other->root)));
}


static PyMethodDef subscription_methods[] = {
    {.ml_name = "get_query_types",
     .ml_meth = (PyCFunction)Subscription_get_query_types,
     .ml_flags = METH_NOARGS},
    {.ml_name = "matches",
     .ml_meth = (PyCFunction)Subscription_matches,
     .ml_flags = METH_O},
    {.ml_name = "isdisjoint",
     .ml_meth = (PyCFunction)Subscription_isdisjoint,
     .ml_flags = METH_O},
    {NULL}};

static PyType_Slot subscription_type_slots[] = {
    {Py_tp_new, Subscription_new},
    {Py_tp_dealloc, Subscription_dealloc},
    {Py_tp_methods, subscription_methods},
    {0, NULL}};

static PyType_Spec subscription_type_spec = {
    .name = "hat.event.common.subscription._csubscription.Subscription",
    .basicsize = sizeof(Subscription),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HEAPTYPE,
    .slots = subscription_type_slots};


static int module_exec(PyObject *module) {
    PyObject *subscription_type =
        PyType_FromModuleAndSpec(module, &subscription_type_spec, NULL);
    if (!subscription_type)
        return -1;

    int result =
        PyModule_AddObjectRef(module, "Subscription", subscription_type);
    Py_DECREF(subscription_type);
    return result;
}


static PyModuleDef_Slot module_slots[] = {{Py_mod_exec, module_exec},
                                          {0, NULL}};

static PyModuleDef module_def = {.m_base = PyModuleDef_HEAD_INIT,
                                 .m_name = "_csubscription",
                                 .m_slots = module_slots};


PyMODINIT_FUNC PyInit__csubscription() { return PyModuleDef_Init(&module_def); }
