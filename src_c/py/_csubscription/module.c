#include <Python.h>
#include <stdbool.h>
#include <hat/ht.h>
#include <hat/py_allocator.h>


PyTypeObject Subscription_Type;

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
    if (node->children && hat_ht_get(node->children, (uint8_t *)"*", 1))
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
        free_children(node);
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

        *child = (node_t){.is_leaf = false, .children = NULL};
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
    if (node->is_leaf &&
        !(node->children && hat_ht_get(node->children, (uint8_t *)"*", 1))) {
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

        Py_ssize_t child_prefix_len = PyTuple_GET_SIZE(prefix) + 1;
        PyObject *child_prefix = PyTuple_New(child_prefix_len);
        if (!child_prefix)
            return 1;

        PyObject *segment;
        for (Py_ssize_t i = 0; i < child_prefix_len - 1; ++i) {
            segment = PyTuple_GET_ITEM(prefix, i);
            Py_INCREF(segment);
            PyTuple_SET_ITEM(child_prefix, i, segment);
        }
        segment = PyUnicode_DecodeUTF8((char *)key, key_size, NULL);
        if (!segment) {
            Py_DECREF(child_prefix);
            return 1;
        }
        PyTuple_SET_ITEM(child_prefix, child_prefix_len - 1, segment);

        int err = get_query_types(child, child_prefix, deque);
        Py_DECREF(child_prefix);
        if (err)
            return 1;
    }

    return 0;
}


static bool matches(node_t *node, PyObject *event_type,
                    size_t event_type_index) {
    if (node->children && hat_ht_get(node->children, (uint8_t *)"*", 1))
        return true;

    if (event_type_index >= PyTuple_GET_SIZE(event_type))
        return node->is_leaf;

    if (!node->children)
        return false;

    node_t *child;

    PyObject *subtype = PyTuple_GET_ITEM(event_type, event_type_index);
    Py_ssize_t key_size;
    const char *key = PyUnicode_AsUTF8AndSize(subtype, &key_size);
    if (!key)
        return false;
    child = hat_ht_get(node->children, (uint8_t *)key, key_size);
    if (child && matches(child, event_type, event_type_index + 1))
        return true;

    child = hat_ht_get(node->children, (uint8_t *)"?", 1);
    if (child && matches(child, event_type, event_type_index + 1))
        return true;

    return false;
}


static int merge_node(node_t *node, node_t *other) {
    if (other->is_leaf)
        node->is_leaf = true;

    if (!other->children)
        return 0;

    if (node->children && hat_ht_get(node->children, (uint8_t *)"*", 1))
        return 0;

    if (hat_ht_get(other->children, (uint8_t *)"*", 1))
        free_children(node);

    if (!node->children) {
        node->children = hat_ht_create(&hat_py_allocator, 8);
        if (!node->children) {
            PyErr_SetString(PyExc_RuntimeError, "internal error");
            return 1;
        }
    }

    if (hat_ht_resize(node->children, hat_ht_count(node->children) +
                                          hat_ht_count(other->children))) {
        PyErr_SetString(PyExc_RuntimeError, "internal error");
        return 1;
    }

    hat_ht_iter_t iter = NULL;
    while ((iter = hat_ht_iter_next(other->children, iter))) {
        size_t key_size;
        uint8_t *key;
        hat_ht_iter_key(iter, &key, &key_size);

        node_t *other_child;
        hat_ht_iter_value(iter, (void **)&other_child);

        node_t *node_child = hat_ht_get(node->children, key, key_size);
        if (!node_child) {
            node_child = PyMem_Malloc(sizeof(node_t));
            if (!node_child) {
                PyErr_SetString(PyExc_RuntimeError, "allocation error");
                return 1;
            }

            *node_child = (node_t){.is_leaf = false, .children = NULL};
            if (hat_ht_set(node->children, (uint8_t *)key, key_size,
                           node_child)) {
                PyMem_Free(node_child);
                PyErr_SetString(PyExc_RuntimeError, "internal error");
                return 1;
            }
        }

        if (merge_node(node_child, other_child))
            return 1;
    }

    return 0;
}


static bool isdisjoint(node_t *first, node_t *second) {
    if (first->is_leaf && second->is_leaf)
        return false;

    if (!first->children) {
        return !first->is_leaf || !second->children ||
               !hat_ht_get(second->children, (uint8_t *)"*", 1);
    }

    if (!second->children) {
        return !second->is_leaf || !first->children ||
               !hat_ht_get(first->children, (uint8_t *)"*", 1);
    }

    if (hat_ht_get(first->children, (uint8_t *)"*", 1))
        return false;

    if (hat_ht_get(second->children, (uint8_t *)"*", 1))
        return false;

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
        if (strncmp((char *)key, "?", key_size) == 0)
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
        if (strncmp((char *)key, "?", key_size) == 0)
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


static PyObject *Subscription_new(PyTypeObject *type, PyObject *args,
                                  PyObject *kwds) {
    PyObject *query_types;
    PyObject *cache_maxsize;
    if (!PyArg_ParseTupleAndKeywords(
            args, kwds, "O|O", (char *[]){"query_types", "cache_maxsize", NULL},
            &query_types, &cache_maxsize))
        return NULL;

    PyObject *query_types_iter = PyObject_GetIter(query_types);
    if (!query_types_iter)
        return NULL;

    Subscription *self = (Subscription *)type->tp_alloc(type, 0);
    if (!self) {
        Py_DECREF(query_types_iter);
        return NULL;
    }

    self->root = (node_t){.is_leaf = false, .children = NULL};

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
    Py_TYPE(self)->tp_free((PyObject *)self);
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

    if (matches(&(self->root), args, 0))
        Py_RETURN_TRUE;

    Py_RETURN_FALSE;
}


static PyObject *Subscription_union(Subscription *self, PyObject *args) {
    PyTypeObject *type = &Subscription_Type;

    if (!PyTuple_Check(args)) {
        PyErr_SetString(PyExc_ValueError, "unsuported arguments");
        return NULL;
    }

    Subscription *subscription = (Subscription *)type->tp_alloc(type, 0);
    if (!subscription)
        return NULL;

    if (merge_node(&(subscription->root), &(self->root))) {
        Py_DECREF(subscription);
        return NULL;
    }

    PyObject *args_iter = PyObject_GetIter(args);
    if (!args_iter) {
        Py_DECREF(subscription);
        return NULL;
    }

    while (true) {
        PyObject *arg = PyIter_Next(args_iter);
        if (!arg)
            break;

        if (!PyObject_TypeCheck(arg, type)) {
            Py_DECREF(arg);
            Py_DECREF(args_iter);
            Py_DECREF(subscription);
            PyErr_SetString(PyExc_ValueError, "unsuported argument type");
            return NULL;
        }

        Subscription *other = (Subscription *)arg;
        int err = merge_node(&(subscription->root), &(other->root));
        Py_DECREF(other);
        if (err) {
            Py_DECREF(args_iter);
            Py_DECREF(subscription);
            PyErr_SetString(PyExc_Exception, "union error");
            return NULL;
        }
    }
    Py_DECREF(args_iter);

    if (resize_children(&(subscription->root))) {
        Py_DECREF(subscription);
        return NULL;
    }

    return (PyObject *)subscription;
}


static PyObject *Subscription_isdisjoint(Subscription *self, PyObject *args) {
    PyTypeObject *type = &Subscription_Type;

    if (!PyObject_TypeCheck(args, type)) {
        PyErr_SetString(PyExc_ValueError, "unsuported argument type");
        return NULL;
    }

    Subscription *other = (Subscription *)args;

    if (isdisjoint(&(self->root), &(other->root)))
        Py_RETURN_TRUE;

    Py_RETURN_FALSE;
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
    if (PyModule_AddObject(module, "Subscription",
                           (PyObject *)&Subscription_Type)) {
        Py_DECREF(module);
        return NULL;
    }

    return module;
}
