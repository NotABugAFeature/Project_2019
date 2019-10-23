#include <stdint.h>

//Type definition for a tuple
typedef struct
{
    int64_t key;        //the key according to which the table will be sorted
                        //and the join operation will be perfomed
    uint64_t row_id;
}tuple;

//Type definition for a relation
typedef struct
{
    tuple *tuples;
    uint64_t num_tuples;
}relation;
#define HIST_SIZE 256
//Type definition for a histogram/cumulative histogram
typedef struct
{
    uint64_t array[HIST_SIZE];

}histogram;


//-----Result List-----
#define RESULT_LIST_BUCKET_SIZE 1048576/(2*sizeof(uint64_t))
#define ROWID_R_INDEX 0
#define ROWID_S_INDEX 1
typedef struct
{
    uint64_t row_ids[RESULT_LIST_BUCKET_SIZE][2];
    unsigned int index_to_add_next;
}result_list_bucket;

typedef struct result_list_node_tag
{
    result_list_bucket bucket;
    struct result_list_node_tag* next;
}result_list_node;

//Type definition for the result list
typedef struct
{
        result_list_node* head; //The First Node Of The List
        result_list_node* tail; //The Last Node Of The List
        int NumberOfNodes; //Counter Of The Buckets;
}result_list;

result_list* create_result_list();
void delete_result_list(result_list*);
int append_to_list(result_list*, uint64_t r_row_id, uint64_t s_row_id);
void print_result_list(result_list);
int is_empty(result_list);
int get_number_of_buckets(result_list);

//Sort Merge Join
/**
 * Sort Merge Join
 * @param relation* R
 * @param relation* S
 * @return result*
 */
result_list* SortMergeJoin(relation *relR,relation *relS);

//----Histogram----

/**
 * Goes through the relation and with binary operations creates the histogram
 * @param relation
 * @param uint64_t start_index The starting index of the relation
 * @param uint64_t end_index The ending index of the relation
 * @param histogram NULL
 * @param unsigned short Which byte is used to create the histogram
 * @return 0 if successful
 */

void create_histogram(relation r,uint64_t start_index,uint64_t end_index, uint64_t *h,unsigned short byte);
/**
 * Transforms the histogram in to a cumulative histogram
 * @param histogram * The histogram to transform
 * @return 0 if successful
 */
int transform_histogram_to_cumulative_histogram(histogram *h);

//Or one function that calls the 2 above and returns the cumulative
