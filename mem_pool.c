//
// Created by bjgad on 3/7/2016.
//

//
// Created by BillJoshua on 2/21/2016.
//

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

//static const float      MEM_POOL_STORE_FILL_FACTOR    = MEM_FILL_FACTOR;
//static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR  = MEM_EXPAND_FACTOR;

//static const float      MEM_NODE_HEAP_FILL_FACTOR     = MEM_FILL_FACTOR;
//static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR   = MEM_EXPAND_FACTOR;

//static const float      MEM_GAP_IX_FILL_FACTOR        = MEM_FILL_FACTOR;
//static const unsigned   MEM_GAP_IX_EXPAND_FACTOR      = MEM_EXPAND_FACTOR;


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
alloc_status mem_init() {
    // ensure that it's called only once until mem_free
    // allocate the pool store with initial capacity
    // note: holds pointers only, other functions to allocate/deallocate
    if(pool_store == NULL)
    {
        pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;
        pool_store = (pool_mgr_pt *) calloc(pool_store_capacity, sizeof(pool_mgr_t));
        return  ALLOC_OK;
    }
    else
    {
        return ALLOC_CALLED_AGAIN;
    }
}

alloc_status mem_free() {
    // ensure that it's called only once for each mem_init
    // make sure all pool managers have been deallocated
    // can free the pool store array
    // update static variables
    if(!pool_store_size == 0)
    {
        free(pool_store);
        return ALLOC_OK;
    }
    else
    {
        return ALLOC_CALLED_AGAIN;
    }
}

pool_pt mem_pool_open(size_t size, alloc_policy policy)
{
    // make sure there the pool store is allocated
    // expand the pool store, if necessary

    if(pool_store == NULL)
    {
        pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;
        pool_store = (pool_mgr_pt *) calloc(pool_store_capacity, sizeof(pool_mgr_t));
    }

    // allocate a new mem pool mgr
    pool_store[0] = (pool_mgr_pt) malloc(sizeof(pool_mgr_t));
    pool_store_size++;
    // check success, on error return null
    if(pool_store[0]);
    else
    {
        printf("pool manager not allocated");
        return NULL;
    }
    // allocate a new memory pool
    pool_store[0]->pool.mem = (char *) malloc(size);

    // check success, on error deallocate mgr and return null
    if(pool_store[0]->pool.mem);
    else
    {
        printf("pool mem not allocated");
        free(pool_store[0]->pool.mem);
        return NULL;
    }

    // initialize pool
    pool_store[0]->pool.policy = policy;
    pool_store[0]->pool.alloc_size = size;
    pool_store[0]->pool.num_allocs = 0;
    pool_store[0]->pool.total_size = 0;
    pool_store[0]->pool.num_gaps = 0;

    // allocate a new node heap
    pool_store[0]->node_heap = (node_pt) calloc(MEM_NODE_HEAP_INIT_CAPACITY, sizeof(node_t));

    // check success, on error deallocate mgr/pool and return null
    if(pool_store[0]->node_heap);
    else
    {
        printf("node heap not allocated");
        free(pool_store[0]->pool.mem);
        free(pool_store[0]);
    }

    //   initialize top node of node heap
    node_pt Hnode = (node_pt) malloc(sizeof(node_t));
    pool_store[0]->used_nodes++;
    pool_store[0]->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;
    pool_store[0]->node_heap[0].alloc_record.size = size;
    Hnode->alloc_record.size = size;
    pool_store[0]->node_heap[0].allocated = 1;
    Hnode->allocated = 1;
    pool_store[0]->node_heap[0].used = 1;
    Hnode->used = 1;
    pool_store[0]->node_heap[0].next = NULL;
    Hnode->next = NULL;
    pool_store[0]->node_heap[0].prev = NULL;
    Hnode->prev = NULL;
    pool_store[0]->node_heap[0].alloc_record.mem = (char *) malloc(size);
    Hnode->alloc_record.mem = (char *) malloc(size);

    // allocate a new gap index
    pool_store[0]->pool.num_gaps++;
    pool_store[0]->gap_ix = (gap_pt) calloc(MEM_GAP_IX_INIT_CAPACITY, sizeof(gap_t));

    // check success, on error deallocate mgr/pool/heap and return null
    if(pool_store[0]->gap_ix);
    else
    {
        printf("gap index not allocated");
        free(pool_store[0]->pool.mem);
        free(pool_store[0]->node_heap);
        free(pool_store[0]);
    }

    //   initialize top node of gap index

    pool_store[0]->gap_ix[0].size = size;
    pool_store[0]->gap_ix->node = Hnode;

    /*
    pool_store[0]->gap_ix[0].size = 0;
    pool_store[0]->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;
    pool_store[0]->gap_ix[0].node->alloc_record.mem = pool_store[0]->pool.mem;
    pool_store[0]->gap_ix[0].node->alloc_record.size = pool_store[0]->pool.alloc_size;
    pool_store[0]->gap_ix[0].node->next = NULL;
    pool_store[0]->gap_ix[0].node->prev = NULL;
    pool_store[0]->gap_ix[0].node->used = 1;
    pool_store[0]->gap_ix[0].node->allocated = 1;*/

    // initialize pool store
    pool_store[0]->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;
    pool_store[0]->used_nodes = 1;

    return (pool_pt) pool_store[0];
}

alloc_status mem_pool_close(pool_pt pool) {
    // note: don't decrement pool_store_size, because it only grows

    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pt = (pool_mgr_pt) pool;
    // check if this pool is allocated
    if(pool)
    {
        // check if pool has only one gap
        if(pool->num_gaps == 1)
        {
            // check if it has zero allocations
            if(pool->num_allocs == 0)
            {
                // free memory pool
                free(pool->mem);
                pool->alloc_size = 0;

                // free node heap
                pt->node_heap->alloc_record.size = 0;
                free(pt->node_heap);

                // free gap index
                pt->gap_ix->node->alloc_record.size = 0;
                free(pt->gap_ix);

                // find mgr in pool store and set to null
                int i = 0;
                for(i; i < pool_store_size; ++i)
                {
                    if(pool_store[i] == pt)
                    {
                        pool_store[i] = NULL;
                        // free mgr
                        free(pool_store[i]);
                        return ALLOC_OK;
                    }
                }
            }
            else
            {
                return ALLOC_FAIL;
            }
        }
        else
        {
            return ALLOC_FAIL;
        }
    }

    else
    {
        printf("mem_pool_close: Pool not allocated");
        return ALLOC_FAIL;
    }
}

alloc_pt mem_new_alloc(pool_pt pool, size_t size)
{
    alloc_status newSize;
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pt = (pool_mgr_pt) pool;
    // check if any gaps, return null if none
    int i = 0;
    for(i; i < (pt->pool.num_gaps); ++i)
    {
        if(i == pt->pool.num_gaps)
        {
            printf("no gaps");
            return NULL;
        }
        if(pt->gap_ix[0].node != NULL)
        {
            break;
        }
    }
    // expand heap node, if necessary, quit on error
    if(((float) pt->used_nodes / pt->total_nodes) > MEM_NODE_HEAP_FILL_FACTOR)
    {
        newSize = _mem_resize_gap_ix(pt);
        if(newSize)
        {
            printf("newSize not 'resized'");
            return NULL;
        }
    }
    // check used nodes fewer than total nodes, quit on error
    if(pt->used_nodes >= pt->total_nodes)
    {
        printf("used nodes is not fewer than total nodes");
        return NULL;
    }
    // get a node for allocation:
    node_pt node = NULL;
    node_pt currNode = NULL;
    // if FIRST_FIT, then find the first sufficient node in the node heap
    int j = 0;
    if(pt->pool.policy == FIRST_FIT)
    {
        currNode = pt->node_heap;
        while(pt->node_heap[j].used != 0 && pt->node_heap->allocated != 0)
        {
            j++;
        }
        if(currNode == NULL)
        {
            return NULL;
        }
        node = &(pt->node_heap[j]);
    }

        // if BEST_FIT, then find the first sufficient node in the gap index
    else
    {
        if(pt->pool.num_gaps != 0)
        {
            while(j < pt->pool.num_gaps && pt->gap_ix[++j].size >= size)
            {
                ++j;
            }
        }
            // check if node found
        else
        {
            return NULL;
        }
        node = pt->gap_ix[j].node;
    }

    // update metadata (num_allocs, alloc_size)
    (pt->pool.num_allocs)++;
    pt->pool.alloc_size += size;

    // calculate the size of the remaining gap, if any
    int remainSize = 0;
    remainSize =  node->alloc_record.size - size;

    // remove node from gap index
    _mem_remove_from_gap_ix(pt, size, node);

    // convert gap_node to an allocation node of given size
    // adjust node heap:
    node->alloc_record.size = size;
    node->used = 1;
    node->allocated = 1;

    //   if remaining gap, need a new node
    node_pt newGap = (node_pt) malloc(sizeof(node_t));
    //   find an unused one in the node heap
    if(remainSize > 0)
    {
        int k = 0;
        while(pt->node_heap[k].used != 0 && pt->node_heap->allocated != 0)
        {
            k++;
        }
        newGap = &(pt->node_heap[k]);

        //   make sure one was found
        if(newGap == NULL);
            //   initialize it to a gap node
        else
        {
            newGap->alloc_record.size = remainSize;
            newGap->allocated = 0;
            newGap->used = 1;
        }
    }
    //   update metadata (used_nodes)
    pt->used_nodes++;

    //   update linked list (new node right after the node for allocation)
    newGap->prev = node;
    node->next = newGap;
    node->alloc_record.size = size;

    //   add to gap index
    _mem_add_to_gap_ix(pt, remainSize, newGap);

    //   check if successful
    // return allocation record by casting the node to (alloc_pt)
    return (alloc_pt) node;
}

alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pt = (pool_mgr_pt) pool;
    node_pt allocNode = (node_pt) alloc;

    // Call the internal function that will handle this

    if(_mem_add_to_gap_ix(pt, alloc->size, allocNode) != ALLOC_OK)
    {
        return ALLOC_FAIL;
    }
    else
    {
        pt->pool.total_size -= alloc->size;
        (pt->pool.num_allocs)--;
        return ALLOC_OK;
    }
}
void mem_inspect_pool(pool_pt pool,
                      pool_segment_pt *segments,
                      unsigned *num_segments) {
    int numSegs = 0;
    // get the mgr from the pool
    pool_mgr_pt pt = (pool_mgr_pt) pool;
    // will be used to traverse through node heap
    // allocate the segments array with size == used_nodes
    *segments = (pool_segment_pt) calloc(pt->used_nodes, sizeof(pool_segment_t));
    // check successful
    if(*segments);
    else
    {
        return;
    }
    // loop through the node heap and the segments array
    int i = 0;
    for(i; i<pt->used_nodes; i++)
    {
        if(pt->node_heap[i].used == 0);
            //    for each node, write the size and allocated in the segment
        else
        {
            (*segments)[i].allocated = pt->node_heap[i].allocated;
            (*segments)[i].size = pt->node_heap[i].alloc_record.size;
            numSegs++;
        }
    }
    *num_segments = numSegs;
    // "return" the values:
    /*
                    *segments = segs;
                    *num_segments = pool_mgr->used_nodes;
     */
}
/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store() {
    // check if necessary
    /*
                if (((float) pool_store_size / pool_store_capacity)
                    > MEM_POOL_STORE_FILL_FACTOR) {...}
     */
    // don't forget to update capacity variables
    pool_store = realloc(pool_store, MEM_EXPAND_FACTOR  * sizeof(pool_store));
    pool_store_capacity *= MEM_EXPAND_FACTOR;
    if(pool_store)
    {
        return ALLOC_OK;
    }
    else {
        return ALLOC_FAIL;
    }
}

static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr) {
    // see above

    return ALLOC_FAIL;
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr) {
    // see above
    if(pool_mgr->gap_ix_capacity < (pool_mgr->gap_ix_capacity * MEM_GAP_IX_FILL_FACTOR))
    {
        return ALLOC_FAIL;
    }
    return ALLOC_FAIL;
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                                       size_t size,
                                       node_pt node) {

    // expand the gap index, if necessary (call the function)
    // add the entry at the end
    // update metadata (num_gaps)
    // sort the gap index (call the function)
    // check success
    int i = 0;
    while(pool_mgr->gap_ix[i].node != NULL)
    {
        i++;
    }
    pool_mgr->gap_ix[i].node = node;
    pool_mgr->gap_ix[i].size = size;
    if(pool_mgr->gap_ix[i].node == NULL) {
        return ALLOC_FAIL;
    }
    else
    {return ALLOC_OK;}
}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                            size_t size,
                                            node_pt node) {
    // find the position of the node in the gap index
    int i = 0;
    int k = 0;
    for(k; k < pool_mgr->pool.num_gaps; k++)
    {
        if(pool_mgr->gap_ix[k].node == node)
        {
            i = k;
        }
    }

    pool_mgr->gap_ix[i].node = NULL;
    pool_mgr->gap_ix[i].size = 0;
    // loop from there to the end of the array:
    //    pull the entries (i.e. copy over) one position up
    //    this effectively deletes the chosen node
    // update metadata (num_gaps)
    // zero out the element at position num_gaps!
    if(pool_mgr->gap_ix[i].node == NULL) {
        return ALLOC_FAIL;
    }
    else
    { return ALLOC_OK;}
}

// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr) {
    // the new entry is at the end, so "bubble it up"
    // loop from num_gaps - 1 until but not including 0:
    //    if the size of the current entry is less than the previous (u - 1)
    //       swap them (by copying) (remember to use a temporary variable)

    return ALLOC_FAIL;
}

