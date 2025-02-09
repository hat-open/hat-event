#include <Python.h>
#include <hat/ht.h>
#include <hat/py_allocator.h>


typedef struct obj_node_t obj_node_t;
typedef struct obj_list_t obj_list_t;
typedef struct ref_t ref_t;
typedef struct tree_node_t tree_node_t;

struct obj_node_t {
    obj_node_t *prev;
    obj_node_t *next;
    PyObject *obj;
};

struct obj_list_t {
    obj_node_t *head;
};

struct ref_t {
    obj_list_t *list;
    obj_node_t *node;
};

struct tree_node_t {
    obj_list_t values;
    obj_list_t asterisk_values;
    hat_ht_t *children;
    tree_node_t *question_child;
};


static void free_obj_list(obj_list_t *list) {
    obj_node_t *node = list->head;
    while (node) {
        Py_DECREF(node->obj);

        obj_node_t *next = node->next;
        PyMem_Free(node);
        node = next;
    }
}


static void free_tree_node(tree_node_t *node) {
    free_obj_list(&(node->values));
    free_obj_list(&(node->asterisk_values));

    if (node->children) {
        hat_ht_iter_t iter = NULL;
        while ((iter = hat_ht_iter_next(node->children, iter))) {
            tree_node_t *child = hat_ht_iter_value(iter);
            free_tree_node(child);
        }

        hat_ht_destroy(node->children);
    }

    if (node->question_child)
        free_tree_node(node->question_child);

    PyMem_Free(node);
}


static tree_node_t *create_tree_node() {
    tree_node_t *result = PyMem_Malloc(sizeof(tree_node_t));
    if (!result)
        return NULL;

    *result = (tree_node_t){.values = {.head = NULL},
                            .asterisk_values = {.head = NULL},
                            .children = NULL,
                            .question_child = NULL};

    return result;
}


static int resize_ht(hat_ht_t *ht) {
    size_t avg_count = hat_ht_avg_count(ht);
    size_t count = hat_ht_count(ht);

    if (count < avg_count && count > avg_count / 4)
        return 0;

    if (count * 2 < 8)
        return 0;

    return hat_ht_resize(ht, count * 2);
}


static void ref_obj_destructor(PyObject *obj) {
    ref_t *ref = PyCapsule_GetPointer(obj, NULL);
    PyMem_Free(ref);
}


static PyObject *add_value_to_list(obj_list_t *list, PyObject *value) {
    obj_node_t *new_obj_node = NULL;
    ref_t *ref = NULL;

    new_obj_node = PyMem_Malloc(sizeof(obj_node_t));
    if (!new_obj_node)
        goto error;

    ref = PyMem_Malloc(sizeof(ref_t));
    if (!ref)
        goto error;

    PyObject *ref_obj = PyCapsule_New(ref, NULL, ref_obj_destructor);
    if (!ref_obj)
        goto error;

    Py_INCREF(value);

    *new_obj_node =
        (obj_node_t){.prev = NULL, .next = list->head, .obj = value};
    if (list->head)
        list->head->prev = new_obj_node;
    list->head = new_obj_node;

    *ref = (ref_t){.list = list, .node = new_obj_node};

    return ref_obj;

error:
    PyMem_Free(new_obj_node);
    PyMem_Free(ref);

    return NULL;
}


static PyObject *add_value_to_tree(tree_node_t *node, PyObject *query_type,
                                   PyObject *value, size_t query_type_index) {
    if (query_type_index >= PyTuple_Size(query_type))
        return add_value_to_list(&(node->values), value);

    PyObject *query_type_head = PyTuple_GetItem(query_type, query_type_index);
    if (!query_type_head)
        return NULL;

    Py_ssize_t query_type_head_len;
    const char *query_type_head_str =
        PyUnicode_AsUTF8AndSize(query_type_head, &query_type_head_len);
    if (!query_type_head_str)
        return NULL;

    if (query_type_head_len == 1 && query_type_head_str[0] == '*')
        return add_value_to_list(&(node->asterisk_values), value);

    if (query_type_head_len == 1 && query_type_head_str[0] == '?') {
        if (!node->question_child) {
            node->question_child = create_tree_node();
            if (!node->question_child)
                return NULL;
        }

        return add_value_to_tree(node->question_child, query_type, value,
                                 query_type_index + 1);
    }

    if (!node->children) {
        node->children = hat_ht_create(&hat_py_allocator, 8);
        if (!node->children)
            return NULL;
    }

    tree_node_t *child = hat_ht_get(node->children, (void *)query_type_head_str,
                                    query_type_head_len);
    if (!child) {
        child = create_tree_node();
        if (!child)
            return NULL;

        if (hat_ht_set(node->children, (void *)query_type_head_str,
                       query_type_head_len, child)) {
            free_tree_node(child);
            return NULL;
        }

        if (resize_ht(node->children))
            return NULL;
    }

    return add_value_to_tree(child, query_type, value, query_type_index + 1);
}


static int get_values(PyObject *result, tree_node_t *node, PyObject *event_type,
                      size_t event_type_index) {
    for (obj_node_t *value = node->asterisk_values.head; value;
         value = value->next)
        if (PySet_Add(result, value->obj))
            return -1;

    if (event_type_index >= PyTuple_Size(event_type)) {
        for (obj_node_t *value = node->values.head; value; value = value->next)
            if (PySet_Add(result, value->obj))
                return -1;

        return 0;
    }

    if (node->question_child) {
        if (get_values(result, node->question_child, event_type,
                       event_type_index + 1))
            return -1;
    }

    if (!node->children)
        return 0;

    PyObject *event_type_head = PyTuple_GetItem(event_type, event_type_index);
    if (!event_type_head)
        return -1;

    Py_ssize_t event_type_head_len;
    const char *event_type_head_str =
        PyUnicode_AsUTF8AndSize(event_type_head, &event_type_head_len);
    if (!event_type_head_str)
        return -1;

    tree_node_t *child = hat_ht_get(node->children, (void *)event_type_head_str,
                                    event_type_head_len);
    if (!child)
        return 0;

    return get_values(result, child, event_type, event_type_index + 1);
}


typedef struct {
    PyObject ob_base;
    tree_node_t *root;
} Collection;


static PyObject *Collection_new(PyTypeObject *type, PyObject *args,
                                PyObject *kwds) {
    Collection *self = (Collection *)PyType_GenericAlloc(type, 0);
    if (!self)
        return NULL;

    self->root = NULL;

    self->root = create_tree_node();
    if (!self->root) {
        Py_DECREF(self);
        return NULL;
    }

    return (PyObject *)self;
}


static void Collection_dealloc(Collection *self) {
    if (self->root)
        free_tree_node(self->root);

    PyTypeObject *tp = Py_TYPE(self);
    PyObject_Free(self);
    Py_DECREF(tp);
}


static PyObject *Collection_add(Collection *self, PyObject *args) {
    PyObject *query_type;
    PyObject *value;

    if (!PyArg_ParseTuple(args, "OO", &query_type, &value))
        return NULL;

    if (!PyTuple_Check(query_type)) {
        PyErr_SetString(PyExc_TypeError, "invalid query type");
        return NULL;
    }

    return add_value_to_tree(self->root, query_type, value, 0);
}


static PyObject *Collection_remove(Collection *self, PyObject *ref_obj) {
    ref_t *ref = PyCapsule_GetPointer(ref_obj, NULL);

    obj_node_t *node = ref->node;

    if (node->prev) {
        node->prev->next = node->next;

    } else {
        ref->list->head = node->next;
    }

    if (node->next)
        node->next->prev = node->prev;

    Py_DECREF(node->obj);
    PyMem_Free(node);

    return Py_NewRef(Py_None);
}


static PyObject *Collection_get(Collection *self, PyObject *event_type) {
    if (!PyTuple_Check(event_type)) {
        PyErr_SetString(PyExc_TypeError, "invalid event type");
        return NULL;
    }

    PyObject *result = PySet_New(NULL);
    if (!result)
        return NULL;

    if (get_values(result, self->root, event_type, 0)) {
        Py_DECREF(result);
        return NULL;
    }

    return result;
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
