# Solution for SIGMOD 2018 programming contest
Constuction of a database subset that manipulates data stored exclusively in RAM

Assignment for the 'Software Development for Information Systems' course

## Team
- Georgakopoulos Panagiotis
- Karamina Maria
- Koursiounis Georgios

## Compilation and Execution

- Queries and tests: `make` or `make all`
- Only queries: `make notests`
- Only tests: `make tests`
- Deletion of executable and objective files: `make clean`

- -O3 optimization is used by default

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


## Results for dataset 'MEDIUM'
(For complete specs list see: report.pdf)

Best execution results on **Intel(R) Core(TM) i7-6500U CPU @ 2.50GHz**
| Execution Type | Number of Threads | Execution Time (sec) |
| --- | --- | --- |
| Basic execution with full parallelism & sorted projections (sorted_projections=true) | 4 | 21.290931 |
| One query per thread (s_execution=true) & sorted projections (sorted_projections=true) | 4 | 17.406146 |


Best execution results on **AMD Ryzen 5 3600 6-Core Processor**
| Execution Type | Number of Threads | Execution Time (sec) |
| --- | --- | --- |
| Basic execution with full parallelism & sorted projections (sorted_projections=true) | 12 | 11.396190 |
| One query per thread (s_execution=true) & sorted projections (sorted_projections=true) | 12 | 8.987036 |

The results show that the fastest implementation is the one that each thread
executes a query from the beginning (analysis, optimization) to the calculation of
of its projections. This happens because:
1) All executed queries were known from the beginning
2) The number of threads of the processors we tested is less than
number of questions.

If we have a machine with a larger number of threads than queries, the
specific implementation will waste time since many threads will have no query to execute.

Complete parallelism is quite efficient despite the RAM consumption which can be solved by limiting the number of concurrent queries to be executed as mentioned above.
