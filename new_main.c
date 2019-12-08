#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include "query.h"
#include "string_list.h"
#include "execute_query.h"

/**
 * Reads queries from stdin and returns them in a list
 * @return string_list of the queries
 */
string_list *read_batch(void)
{
    char line[STRING_SIZE];
    string_list *list=string_list_create();
    while(1)
    {
        fgets(line, STRING_SIZE, stdin);
        if(feof(stdin))
        {
            printf("End of input\n");
            return NULL;
        }
        if(line==NULL)
        {
            return NULL;
        }
        if(line[strlen(line) - 1] == '\n')
        {
            line[strlen(line) - 1] = '\0';
            if(line[strlen(line) - 1] == '\r')
            {
                line[strlen(line) - 1] = '\0';
            }
        }

        if(strcmp(line, "F")==0||strcmp(line, "f")==0)
        {
            break;
        }
        string_list_insert(list, line);
    }
    return list;
}
int main(void)
{

    string_list *list=read_tables();
    printf("List of names:\n");
    string_list_print(list);
    table_index *ti=insert_tables_from_list(list);
    printf("ti->num_tables: %" PRIu64 "\n", ti->num_tables);
    for(uint32_t i=0; i<ti->num_tables; i++)
    {
        printf("ti->tables[%d].table_id: %" PRIu32 " - ti->tables[%d].columns: %" PRIu64 " - ti->tables[%d].rows: %" PRIu64 "\n", i, ti->tables[i].table_id, i, ti->tables[i].columns, i, ti->tables[i].rows);
    }
    while(1)
    {
        list=read_batch();
        if(list==NULL)
        {
            return 0;
        }
        //Call query analysis, execute queries
        char *query_str;
        for(;list->num_nodes>0;)
        {
            query_str=string_list_remove(list);
            clock_t t; 
            t = clock(); 
            printf("The query to analyze: %s\n",query_str);
            query* q=create_query();
            if(q==NULL)
            {
                delete_table_index(ti);
                return -3;
            }
            if(analyze_query(query_str,q)!=0)
            {
                delete_query(q);
                continue;
            }
            printf("After analyzing: ");
            print_query_like_an_str(q);
            if(validate_query(q,ti)!=0)
            {
                delete_query(q);
                continue;
            }
            printf("After validation: ");
            print_query_like_an_str(q);
            if(validate_query(q,ti)!=0)
            {
                delete_query(q);
                continue;
            }
            if(optimize_query(q,ti)!=0)
            {
                delete_query(q);
                continue;
            }
            printf("After optimizing: ");
            print_query_like_an_str(q);
            bool* bool_array=NULL;
            if(create_sort_array(q,&bool_array)!=0)
            {
                delete_query(q);
                continue;
            }
            printf("After creating bool array: ");
            print_query_like_an_str(q);
            if(optimize_query_memory(q)!=0)
            {
                delete_query(q);
                continue;
            }
            printf("After optimizing memory: ");
            print_query_like_an_str(q);
            t = clock() - t; 
            double time_taken = ((double)t)/CLOCKS_PER_SEC; // in seconds 
            printf("Optimization took %f seconds to execute \n", time_taken); 

            middleman *middle = execute_query(q, ti, bool_array);
            
            calculate_projections(q, ti, middle);

            //Execute

            free(middle->tables);
            free(middle);

            free(bool_array);
        }
    }
    delete_table_index(ti);
    return 0;
}
