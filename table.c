#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include "table.h"

/* ---------- TABLE_NAME_LIST ---------- */

/**
 * Creates a table_name_list
 * @return a table_name_list, NULL for error
 */
table_name_list *table_name_list_create(void)
{
	table_name_list *list = malloc(sizeof(table_name_list));
	if(list == NULL)
	{
		perror("table_name_list_create: malloc error");
		return NULL;
	}
	list->num_nodes = 0;
	list->head = NULL;
	list->tail = NULL;

	return list;
}


/**
 * Inserts a table name in a table_name_list
 *
 * @param list - an existing list
 * @param filename - name to insert
 * @return 0 for success, <0 for error
 */
int table_name_list_insert(table_name_list *list, char *filename)
{
	table_name_list_node *node = malloc(sizeof(table_name_list_node));
	if(node == NULL)
	{
		perror("table_name_list_insert: malloc error");
		return -1;
	}
	strcpy(node->filename, filename);
	node->next = NULL;

	if(list->head == NULL)
	{
		list->head = node;
		list->tail = node;
	}
	else
	{
		list->tail->next = node;
		list->tail = node;
	}
	list->num_nodes++;

	return 0;
}


/**
 * Removes the first name from a table_name_list
 * @param list - an existing list
 * @return the first filename
 */
char *table_name_list_remove(table_name_list *list)
{
	if(list->head == NULL)
	{
		return NULL;
	}

	table_name_list_node *node = list->head;
	if(list->head == list->tail)
	{
		list->tail = list->tail->next;
	}
	list->head = list->head->next;

	char *filename = malloc(FILENAME_SIZE*sizeof(char));
	if(filename == NULL)
	{
		perror("table_name_list_remove: malloc error");
		free(node);
		return NULL;
	}
	strcpy(filename, node->filename);
	free(node);
	list->num_nodes--;
	return filename;
}


/**
 * Prints a table_name_list
 * @param list - list to print
 */
void table_name_list_print(table_name_list *list)
{
	table_name_list_node *node = list->head;
	while(node != NULL)
	{
		printf("%s\n", node->filename);
		node = node->next;
	}
}


/**
 * Reads filenames of tables from stdin and returns them in a list
 * @return table_name_list of the names
 */
table_name_list *read_tables(void)
{
	char line[FILENAME_SIZE];
	table_name_list *list = table_name_list_create();

	while(1)
	{
		fgets(line, FILENAME_SIZE, stdin);
		line[strlen(line) - 1] = '\0';
		if(strcmp(line, "Done") == 0)
		{
			break;
		}
		table_name_list_insert(list, line);

	}

	return list;
}


/* ---------- TABLES ---------- */


/**
 * Reads a table from a file
 * @param table - an (already allocated) table
 * @param filename - name of the file to read from
 * @return 0 for success, <0 for error
 */
int table_from_file(table *t, char *filename)
{
	FILE *fp;
	uint64_t rows, columns;

	if(t == NULL)
	{
		fprintf(stderr, "table_from_file: given table is NULL\n");
		return -1;
	}

    //open file
    fp = fopen(filename, "rb");
    if(fp == NULL)
    {
        perror("table_from_file: open error");
        return -2;
    }

    if(fread(&rows, sizeof(uint64_t), 1, fp) < 1)
    {
    	perror("table_from_file: read error");
    	fclose(fp);
    	return -2;
    }

    if(fread(&columns, sizeof(uint64_t), 1, fp) < 1)
    {
    	perror("table_from_file: read error");
    	fclose(fp);
    	return -2;
    }

    uint32_t id;
    sscanf(filename, "%*[^0123456789]%" PRIu32, &id);
    t->table_id = id;
    t->rows = rows;
    t->columns = columns;
    t->array = malloc(columns * sizeof(uint64_t *));
    if(t->array == NULL)
    {
    	perror("table_from_file: malloc error");
    	return -3;
    }

    for(uint64_t i=0; i<columns; i++)
    {
    	t->array[i] = malloc(rows * sizeof(uint64_t));
    	if(t->array[i] == NULL)
    	{
    		perror("table_from_file: malloc error");
    		return -3;
    	}
    }

    //read content
    for(uint64_t i = 0; i < columns; i++)
    {
        for(uint64_t j = 0; j < rows; j++)
        {
            if(fread(&(t->array[i][j]), sizeof(uint64_t), 1, fp) < 1)
	    	{
				fprintf(stderr, "table_from_file: incorrect file format\n");
				fclose(fp);
				return -3;
	    	}
        }
    }

    fclose(fp);

    return 0;

}


//TODO: Old version (possibly remove it)
/**
 * Reads a table from a file
 *
 * @param filename - path of the file
 * @return table in table * format, NULL for error
 */
table *read_from_file(char *filename)
{
    FILE *fp;
    uint64_t rows=0, columns=0;
    table *table_r = malloc(sizeof(table));
    if(table_r == NULL)
    {
    	perror("read_from_file: malloc error");
    	return NULL;
    }

    //open file
    fp = fopen(filename, "rb");
    if(fp == NULL)
    {
        perror("read_from_file: fopen error");
	free(table_r);
        return NULL;
    }

    if(fscanf(fp, "%" PRIu64 " %" PRIu64, &rows, &columns) != 2)
    {
	fprintf(stderr, "read_from_file: incorrect file format\n");
	fclose(fp);
	free(table_r);
	return NULL;
    }

    table_r->rows = columns;
    table_r->columns = rows;
    if(rows == 0 || columns == 0)
    {
	fprintf(stderr, "read_from_file: can't create empty relation\n");
	return NULL;
    }

    //allocate memory
    table_r->array = malloc(columns * sizeof(uint64_t *));
    if(table_r->array == NULL)
    {
        perror("read_from_file: malloc error");
	fclose(fp);
	free(table_r);
	return NULL;
    }

    for(uint64_t i=0; i<columns; i++)
    {
        table_r->array[i] = malloc(rows * sizeof(uint64_t));
        if(table_r->array[i] == NULL)
        {
	    perror("read_from_file: malloc error");
            fclose(fp);
	    delete_table(table_r);
            return NULL;
        }
    }

    //read content
    for(uint64_t i = 0; i < rows; i++)
    {
        for(uint64_t j = 0; j < columns; j++)
        {
            if(fscanf(fp, "%" PRIu64, &table_r->array[j][i]) != 1)
	    {
		fprintf(stderr, "read_from_file: incorrect file format\n");
		fclose(fp);
	        delete_table(table_r);
		return NULL;
	    }
        }
    }

    fclose(fp);
    return table_r;
}


/**
 * Frees the memory used by the table
 * @param table*
 */
void delete_table(table*table_r)
{
    if(table_r!=NULL)
    {
        for(uint64_t i=0; i<table_r->columns; i++)
        {
            free(table_r->array[i]);
            table_r->array[i]=NULL;
        }
        free(table_r->array);
        table_r->array=NULL;
        free(table_r);
        table_r=NULL;
    }
}

/**
 * Frees the memory used by the contents of a table (doesn't free the table itself)
 * @param table*
 */
void delete_table_contents(table*table_r)
{
    if(table_r!=NULL)
    {
        for(uint64_t i=0; i<table_r->columns; i++)
        {
            free(table_r->array[i]);
            table_r->array[i]=NULL;
        }
        free(table_r->array);
        table_r->array=NULL;
    }
}


/**
 * Takes a list of table names and reads the tables from their files into a table_index struct
 * @param list - list of filenames
 * @return the tables in table_index format
 */
table_index *insert_tables_from_list(table_name_list *list)
{
	table_index *ti = malloc(sizeof(table_index));
	if(ti == NULL)
	{
		perror("insert_tables: malloc error");
		return NULL;
	}

	ti->num_tables = list->num_nodes;
	ti->tables = malloc(ti->num_tables*sizeof(table));
	if(ti->tables == NULL)
	{
		perror("insert_tables: malloc error");
		return NULL;
	}

	char *filename;
	for(int i=0; i<ti->num_tables; i++)
	{
		filename = table_name_list_remove(list);
		if(table_from_file(&(ti->tables[i]), filename) < 0)
		{
			fprintf(stderr, "insert_tables: error reading in table %s\n", filename);
			free(filename);
			break;
		}

		free(filename);
	}

	free(list);
	return ti;
}


/**
 * Finds a table based on its id
 * @param table_index - a table_index struct that holds the tables
 * @param id - id of the table to find
 # @return pointer to the table, NULL if not found
 */
table *get_table(table_index *ti, uint32_t id)
{
	for(int i=0; i<ti->num_tables; i++)
	{
		if(ti->tables[i].table_id == id)
		{
			return &(ti->tables[i]);
		}
	}
	return NULL;
}

/**
 * Deletes a table_index and all its tables
 * @param table_index the table index
 */
void delete_table_index(table_index *ti)
{
	for(int i=0; i<ti->num_tables; i++)
	{
		delete_table_contents(&(ti->tables[i]));
	}
	free(ti->tables);

	free(ti);
	ti = NULL;
}


/**
 * Reads in the tables from the files given from stdin
 * @return the tables in table_index format
 */
table_index *insert_tables(void)
{
	table_name_list *list = read_tables();
	table_index *ti = insert_tables_from_list(list);
	return ti;
}