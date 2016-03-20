/*
 * Created by Ivo Georgiev on 2/9/16.
 */

#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

/*************/
/*           */
/* Constants */
/*           */
/*************/
static const float      MEM_FILL_FACTOR                 = 0.75;
static const unsigned   MEM_EXPAND_FACTOR               = 2;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY    = 20;
static const float      MEM_POOL_STORE_FILL_FACTOR      = 0.75;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR    = 2;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY     = 40;
static const float      MEM_NODE_HEAP_FILL_FACTOR       = 0.75;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR     = 2;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY        = 40;
static const float      MEM_GAP_IX_FILL_FACTOR          = 0.75;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR        = 2;



/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _node {
    alloc_t alloc_record;
    unsigned used;
    unsigned allocated;
    struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;

typedef struct _gap {
    size_t size;
    node_pt node;
} gap_t, *gap_pt;

typedef struct _pool_mgr {
    pool_t pool;
    node_pt node_heap;
    unsigned total_nodes;
    unsigned used_nodes;
    gap_pt gap_ix;
    unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;



/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;



/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status
        _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                           size_t size,
                           node_pt node);
static alloc_status
        _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                size_t size,
                                node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr);



/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init()
{
    // ensure that it's called only once until mem_free
    // allocate the pool store with initial capacity
    // note: holds pointers only, other functions to allocate/deallocate

    if (pool_store == NULL) {
        pool_store = (pool_mgr_pt*) calloc(MEM_POOL_STORE_INIT_CAPACITY, sizeof(pool_mgr_pt));
        pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;
        pool_store_size = 0;
        return ALLOC_OK;
    }

    else {
        return ALLOC_CALLED_AGAIN;
    }
}

alloc_status mem_free()
{
    // ensure that it's called only once for each mem_init
    // make sure all pool managers have been deallocated
    // can free the pool store array
    // update static variables
    if(pool_store != NULL) {
        mem_pool_close(&pool_store[0]->pool);
    }
    else {
        return ALLOC_CALLED_AGAIN;
    }

    free(pool_store);
    pool_store = NULL;
    pool_store_size = 0;
    pool_store_capacity = 0;
    return ALLOC_OK;
}

pool_pt mem_pool_open(size_t size, alloc_policy policy)
{
    // make sure there the pool store is allocated
    if(pool_store_size == 0);
    else{
        return NULL;
    }
    if(pool_store != NULL);
    else{
        return NULL;
    }

    // expand the pool store, if necessary
    if (((float) pool_store_size / pool_store_capacity) > MEM_POOL_STORE_FILL_FACTOR && _mem_resize_pool_store() == ALLOC_FAIL) {
        return NULL;
    }

    // allocate a new mem pool mgr
    pool_mgr_pt pool_mgr = (pool_mgr_pt) malloc(sizeof(pool_mgr_t));

    // check success, on error return null
    if (pool_mgr == NULL) {
        printf("pool mgr not allocated");
        return NULL;
    }

    // allocate a new memory pool
    pool_mgr->pool.mem = (char*) malloc(size);

    // check success, on error deallocate mgr and return null
    if (pool_mgr->pool.mem == NULL)
    {
        free(pool_mgr);
        printf("mem pool not allocated");
        return NULL;
    }

    // allocate a new node heap
    pool_mgr->node_heap = (node_pt) calloc (MEM_NODE_HEAP_INIT_CAPACITY ,sizeof(node_t));

    // check success, on error deallocate mgr/pool and return null
    if (pool_mgr->node_heap == NULL)
    {
        free(pool_mgr);
        printf("node heap not allocated");
        return NULL;
    }

    // allocate a new gap index
    pool_mgr->gap_ix = (gap_pt) calloc (MEM_GAP_IX_INIT_CAPACITY, sizeof(gap_t));

    // check success, on error deallocate mgr/pool/heap and return null
    if (pool_mgr->gap_ix == NULL)
    {
        free(pool_mgr->node_heap);
        free(&pool_mgr[0].pool);
        free(pool_mgr);
        printf("gap index not allocated");
        return NULL;

    }
    // assign all the pointers and update meta data:
    //   initialize top node of node heap
    pool_mgr->node_heap[0].alloc_record.mem = pool_mgr->pool.mem;
    pool_mgr->node_heap[0].alloc_record.size = size;
    pool_mgr->node_heap[0].next = NULL;
    pool_mgr->node_heap[0].prev = NULL;
    pool_mgr->node_heap[0].used = 0;
    pool_mgr->node_heap[0].allocated = 0;

    //   initialize top node of gap index
    pool_mgr->gap_ix[0].node = pool_mgr->node_heap;
    pool_mgr->gap_ix[0].size = size;

    //   initialize pool mgr
    pool_mgr->pool.total_size = size;
    pool_mgr->pool.policy = policy;
    pool_mgr->used_nodes = 1;
    pool_mgr->pool.num_gaps = 1;
    pool_mgr->pool.alloc_size = 0;
    pool_mgr->pool.num_allocs = 0;

    //   link pool mgr to pool store
    int i = 0;
    while (pool_store[i] != NULL) {
        ++i;
    }
    pool_store[i] = pool_mgr;
    pool_store_size = 1;

    pool_mgr->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;
    pool_mgr->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;

    // return the address of the mgr, cast to (pool_pt)
    return (pool_pt) pool_mgr;
}

alloc_status mem_pool_close(pool_pt pool)
{
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_mgr = (pool_mgr_pt)pool;

    // check if this pool is allocated
    if (pool_mgr != NULL);
    else {
        return ALLOC_NOT_FREED;
    }

    // check if pool has only one gap
    if (pool->num_gaps == 1);
    else {
        return ALLOC_NOT_FREED;
    }

    // check if it has zero allocations
    if (pool->num_allocs == 0);
    else {
        return ALLOC_NOT_FREED;
    }

    // free memory pool
    free(pool);

    // free node heap
    free(pool_mgr->node_heap);

    // free gap index
    free(pool_mgr->gap_ix);

    // find mgr in pool store and set to null
    int i = 0;
    while (pool_store[i] != pool_mgr) {
        ++i;
    }
    pool_store[i] = NULL;
    return ALLOC_OK;

}

alloc_pt mem_new_alloc(pool_pt pool, size_t size) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    // variables that will be used:
    pool_mgr_pt poolMgr = (pool_mgr_pt) pool;
    node_pt newNode = NULL;
    node_pt newGap = NULL;
    int i = 0;
    int j = 0;
    int remainGap = 0;


    // check if any gaps, return null if none
    if(poolMgr->gap_ix[0].node == NULL){
        return NULL;
    }

    // expand heap node, if necessary, quit on error
    if ((poolMgr->used_nodes / poolMgr->total_nodes) > MEM_NODE_HEAP_FILL_FACTOR && _mem_resize_node_heap(poolMgr) != ALLOC_OK) {
        _mem_resize_node_heap(poolMgr);
    }

    if (poolMgr->used_nodes > poolMgr->total_nodes) {
        return NULL;
    }
    // if policy == FIRST_FIT, (node heap)
    if(poolMgr->pool.policy == FIRST_FIT){
        while((i < poolMgr->total_nodes) && (poolMgr->node_heap[i].allocated != 0 || poolMgr->node_heap[i].alloc_record.size < size)){
            ++i;
        }
        // check if node found
        if(i == poolMgr->total_nodes){
            return NULL;
        }
        newNode = &poolMgr->node_heap[i];

    }
        // if policy == BEST_FIT, (gap ix)
    else if(poolMgr->pool.policy == BEST_FIT){
        if(poolMgr->pool.num_gaps > 0) {
            while (i < poolMgr->pool.num_gaps && poolMgr->gap_ix[i + 1].size >= size) {
                if(poolMgr->gap_ix[i].size == size){
                    break;
                }
                ++i;
            }
        }
        else {
            return NULL;
        }
        newNode = poolMgr->gap_ix[i].node;
    }

    // check if node found
    if(newNode == NULL){
        return NULL;
    }

    // update metadata (num_allocs, alloc_size)
    ++(poolMgr->pool.num_allocs);
    poolMgr->pool.alloc_size += size;

    // calculate the size of the remaining gap, if any
    if(newNode->alloc_record.size - size > 0){
        remainGap = newNode->alloc_record.size - size;
    }
    _mem_remove_from_gap_ix(poolMgr, size, newNode);

    // convert gap_node to an allocation node of given size
    newNode->alloc_record.size = size;
    newNode->allocated = 1;
    newNode->used = 1;

    // adjust node heap:
    //   if remaining gap, need a new node
    if(remainGap != 0) {
        //   find an unused one in the node heap
        while (poolMgr->node_heap[j].used != 0) {
            ++j;
        }
        newGap = &poolMgr->node_heap[j];
        //   make sure one was found
        if(newGap == NULL){
            return NULL;
        }
        else {
            newGap->alloc_record.size = remainGap;
            newGap->used = 1;
            newGap->allocated = 0;
        }
        newGap->next = newNode->next;

        //   update linked list (new node right after the node for allocation)
        if(newNode->next != NULL){
            newNode->next->prev = newGap;
        }
        ++(poolMgr->used_nodes);
        newNode->next = newGap;
        newGap->prev = newNode;


        //   add to gap index
        _mem_add_to_gap_ix(poolMgr, remainGap, newGap);
    }

    // return allocation record by casting the node to (alloc_pt)
    return (alloc_pt) newNode;
}

alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc)
{
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt poolMgr = (pool_mgr_pt) pool;
    // temporary node pt to check if the node if found
    node_pt node = (node_pt) alloc;
    // node that will hold the next node from the node that will be deleted
    node_pt next = NULL;
    // prev node from deleteNode
    node_pt prev = NULL;
    // this node will be used as a temporary storage
    node_pt deleteNode = NULL;
    // find the node in the node heap
    for(int i = 0; i< poolMgr->total_nodes; ++i){
        if(node == &poolMgr->node_heap[i]){
            deleteNode = &poolMgr->node_heap[i];
            break;
        }
    }
    // make sure it's found
    if (deleteNode != NULL);
    else{
        return ALLOC_FAIL;
    }

    next = deleteNode->next;
    prev = deleteNode->prev;
    deleteNode->allocated = 0;

    // update metadata (num_allocs, alloc_size)
    poolMgr->pool.num_allocs--;
    poolMgr->pool.alloc_size -= deleteNode->alloc_record.size;

    // if the next node in the list is a gap, merge deleteNode to it
    if(deleteNode->next != NULL && deleteNode->next->allocated == 0) {
        if(_mem_remove_from_gap_ix(poolMgr, 0, next) == ALLOC_FAIL) {
            return ALLOC_FAIL;
        }
        deleteNode->alloc_record.size += next->alloc_record.size;
        //   update node as unused
        next->used = 0;
        //   update metadata (used nodes)
        --(poolMgr->used_nodes);
        //   update linked list:
        if (next->next) {
            next->next->prev = deleteNode;
            deleteNode->next = next->next;
        } else {
            deleteNode->next = NULL;
        }
        next->next = NULL;
        next->prev = NULL;
    }
    // check if the prev node in the list a gap and merges it if it is
    if(deleteNode->prev!= NULL && deleteNode->prev->allocated == 0) {
        if(_mem_remove_from_gap_ix(poolMgr, 0, prev) == ALLOC_FAIL) {
            return ALLOC_FAIL;
        }
        prev->alloc_record.size += deleteNode->alloc_record.size;
        //   update metadata (used_nodes)
        --(poolMgr->used_nodes);
        deleteNode->used = 0;
        //   update linked list
        if (deleteNode->next) {
            prev->next = deleteNode->next;
            deleteNode->next->prev = prev;
        } else {
            prev->next = NULL;
        }
        deleteNode = prev;
    }
    // check success
    if (_mem_add_to_gap_ix(poolMgr, deleteNode->alloc_record.size, deleteNode) == ALLOC_OK) {
        return ALLOC_OK;
    }
    else {
        return ALLOC_FAIL;
    }
}

void mem_inspect_pool(pool_pt pool, pool_segment_pt *segments, unsigned *num_segments)
{
    // get the mgr from the pool
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;
    node_pt currNode;
    pool_segment_pt segmentArr = (pool_segment_pt) calloc(pool_mgr->used_nodes, sizeof(pool_segment_t));
    currNode = pool_mgr->node_heap;

    // loop through the node heap and the segments array
    //    for each node, write the size and allocated in the segment
    for (int i=0; i < pool_mgr->used_nodes; ++i)
    {
        segmentArr[i].size = currNode->alloc_record.size;
        segmentArr[i].allocated = currNode->allocated;
        if (currNode->next != NULL)
            currNode = currNode->next;
    }

    // "return" the values:
    *segments = segmentArr;
    *num_segments = pool_mgr->used_nodes;
}



/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store()
{
    // check if necessary
    /*
                if (((float) pool_store_size / pool_store_capacity)
                    > MEM_POOL_STORE_FILL_FACTOR) {...}
     */
    // don't forget to update capacity variables
    if (((float) pool_store_size / pool_store_capacity) > MEM_POOL_STORE_FILL_FACTOR) {
        // reallocate pool store
        pool_store = realloc(pool_store, (sizeof(pool_store) * MEM_POOL_STORE_EXPAND_FACTOR));
        // update pool store capacity
        pool_store_capacity = pool_store_capacity * MEM_POOL_STORE_EXPAND_FACTOR;
        return ALLOC_OK;
    }
    else {
        return ALLOC_FAIL;
    }
}

static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr)
{
    // check if necessary
    /*
                if (((float) pool_mgr->used_nodes / pool_mgr->total_nodes)
                    > MEM_NODE_HEAP_FILL_FACTOR) {...}
     */
    // don't forget to update capacity variables
    if (MEM_NODE_HEAP_FILL_FACTOR > (pool_mgr->used_nodes / pool_mgr->total_nodes)){
        return ALLOC_FAIL;
    }
    else{
        pool_mgr->node_heap = realloc(pool_mgr->node_heap, sizeof(pool_mgr->node_heap) * MEM_NODE_HEAP_EXPAND_FACTOR);
        pool_mgr->total_nodes = pool_mgr->total_nodes * MEM_NODE_HEAP_EXPAND_FACTOR;

        return ALLOC_OK;
    }
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr)
{
    // check if necessary
    /*
                if (((float) pool_mgr->pool.num_gaps / pool_mgr->gap_ix_capacity)
                    > MEM_GAP_IX_FILL_FACTOR) {...}
     */
    // don't forget to update capacity variables
    if (MEM_GAP_IX_FILL_FACTOR > (pool_mgr->pool.num_gaps / pool_mgr->gap_ix_capacity)) {
        return ALLOC_FAIL;
    }
    else{
        pool_mgr->gap_ix = realloc(pool_mgr->gap_ix, sizeof(pool_mgr->gap_ix) * MEM_GAP_IX_EXPAND_FACTOR);
        pool_mgr->gap_ix_capacity = pool_mgr->gap_ix_capacity * MEM_GAP_IX_EXPAND_FACTOR;
        return ALLOC_OK;
    }
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr, size_t size, node_pt node)
{
    int i = 0;
    // expand the gap index, if necessary (call the function)
    _mem_resize_gap_ix(pool_mgr);
    while (pool_mgr->gap_ix[i].node != NULL) {
        ++i;
    }
    // update metadata (num_gaps)
    (pool_mgr->pool.num_gaps)++;
    // add the entry at the end
    pool_mgr->gap_ix[i].size = size;
    pool_mgr->gap_ix[i].node = node;

    // check success
    if (pool_mgr->gap_ix[i].node == NULL) {
        return ALLOC_FAIL;
    }
    else {
        // sort the gap index (call the function)
        _mem_sort_gap_ix(pool_mgr);
        return ALLOC_OK;
    }
}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr, size_t size, node_pt node)
{
    // find the position of the node in the gap index
    // loop from there to the end of the array:
    //    pull the entries (i.e. copy over) one position up
    //    this effectively deletes the chosen node
    int i = 0;
    while (pool_mgr->gap_ix[i].node != node) {
        ++i;
    }

    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps--;
    // zero out the element at position num_gaps!
    pool_mgr->gap_ix[i].size = 0;
    pool_mgr->gap_ix[i].node = NULL;

    return ALLOC_OK;
}

static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr)
{
    // the new entry is at the end, so "bubble it up"
    // loop from num_gaps - 1 until but not including 0:
    //    if the size of the current entry is less than the previous (u - 1)
    //    or if the sizes are the same but the current entry points to a
    //    node with a lower address of pool allocation address (mem)
    //       swap them (by copying) (remember to use a temporary variable)
    gap_t tempNode;
    int i = pool_mgr->pool.num_gaps - 1;
    while (pool_mgr->gap_ix[i].size < pool_mgr->gap_ix[i+1].size && i > 0){
        tempNode = pool_mgr->gap_ix[i];
        pool_mgr->gap_ix[i] = pool_mgr->gap_ix[i+1];
        pool_mgr->gap_ix[i+1] = tempNode;
        --i;
    }
    return ALLOC_OK;
}
