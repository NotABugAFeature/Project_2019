# Solution of SIGMOD 2018 programming contest
Project for the Software Development for Information Systems class

### Team
Georgakopoulos Panagiotis\
Karamina Maria\
Koursiounis Georgios
- - - -

## Compilation and Execution

- Queries and tests: `make` or `make all`
- Only queries: `make notests`
- Only tests: `make tests`
- Deletion of executable and objective files: `make clean`

-O3 optimization is used by default

### Flags

| Flag | Make Command | Function |
| --- | --- | --- |
| -DSORTED_PROJECTIONS | sorted_projections=true | Print results in the correct order |
| -DSERIAL_EXECUTION | s_execution=true | One query per thread |
| -DSERIAL_JOIN | s_join=true | Join of relations r, s executed by one thread |
| -DSERIAL_SORTING | s_sorting=true | Sorting of a relation executed by one thread |
| -DSERIAL_PRESORTING | s_presorting=true | Creation of relations R/S executed by one thread (must be combined with s_sort, s_join) |
| -DSERIAL_FILTER | s_filter=true | Filters of type 0.1 <>= uint64_t executed by one thread |
| -DSERIAL_SELFJOIN | s_selfjoin=true | Filters of type 0.1=0.2 executed by one thread |
| -DSERIAL_PROJECTIONS | s_projections=true | Projections executed by one thread |
| -DONE_QUERY_AT_A_TIME | one_query=true | All threads used in execution of each query |
| -DMAX_QUERIES_LIMIT | max_queries=true | Program takes an extra parameter, the max number of queries being executed at the same time |
| -DONE_BATCH_AT_A_TIME | one_batch=true | One batch at a time is executed |


### Execution
`cat <init file> <work file> | queries <thread num>`

If the max_queries flag is used:
`cat <init file> <work file> | queries <thread num> <max queries>`


## Results
Best execution results on B450 AORUS M computer
| Execution Type | Number of Threads | Execution Time (sec) |
| --- | --- | --- |
| Basic execution with full parallelism & sorted projections (sorted_projections=true) | 12 | 11.396190 |
| One query per thread (s_execution=true) & sorted projections (sorted_projections=true) | 12 | 8.987036 |


