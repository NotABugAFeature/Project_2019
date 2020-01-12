#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include "table.h"

/**
 * Reads filenames of tables from stdin and returns them in a list
 * @return string_list of the names
 */
string_list *read_tables(void)
{
  char line[STRING_SIZE];
  string_list *list = string_list_create();

  while(1)
  {
    if(fgets(line, STRING_SIZE, stdin)==NULL)
    {
      break;
    }
    if(line[strlen(line) - 1] == '\n')
    {
      line[strlen(line) - 1] = '\0';
      if(line[strlen(line) - 1] == '\r')
      {
        line[strlen(line) - 1] = '\0';
      }
    }
    if(strlen(line) < 1)
    {
      continue;
    }

    if(strcmp(line, "Done") == 0 || feof(stdin))
    {
      break;
    }
    string_list_insert(list, line);

  }

  return list;
}


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

    t->columns_stats = malloc(columns * sizeof(statistics));
    if(t->columns_stats == NULL)
    {
      perror("table_from_file: malloc error");
      return -4;
    }


    //read content
    for(uint64_t i = 0; i < columns; i++)
    {
        t->columns_stats[i].f_A = rows;

        for(uint64_t j = 0; j < rows; j++)
        {
          if(fread(&(t->array[i][j]), sizeof(uint64_t), 1, fp) < 1)
          {
            fprintf(stderr, "table_from_file: incorrect file format\n");
            fclose(fp);
            return -3;
          }

          if(j == 0)
          {
            t->columns_stats[i].i_A = t->array[i][j];
            t->columns_stats[i].u_A = t->array[i][j];
          }
          else
          {
            if(t->array[i][j] < t->columns_stats[i].i_A)
            {
              t->columns_stats[i].i_A = t->array[i][j];
            }
            if(t->array[i][j] > t->columns_stats[i].u_A)
            {
              t->columns_stats[i].u_A = t->array[i][j];
            }
          }
        }

        uint64_t min_max = t->columns_stats[i].u_A - t->columns_stats[i].i_A + 1;
        if(min_max > N) 
        {
          t->num_vals = (N%8 > 0) ? (N/8 + 1): (N/8);
          t->over_n = true;
        }
        else
        {
          t->num_vals = (min_max%8 > 0) ? (min_max/8 + 1): (min_max/8);
          t->over_n = false;
        }

        t->distinct_vals = malloc(t->num_vals * sizeof(int8_t));
          
        if(t->distinct_vals == NULL)
        {
          fprintf(stderr, "table_from_file: malloc error\n");
          fclose(fp);
          return -5;
        }

        for(uint64_t j = 0; j < t->num_vals; j++)
          t->distinct_vals[j] = 0;

        for(uint64_t j = 0; j < rows; j++)
        {
          if(t->over_n)
          {
            int8_t b = t->distinct_vals[((t->array[i][j] - t->columns_stats[i].i_A) % N)/8];
            int position = ((t->array[i][j] - t->columns_stats[i].i_A) % N)%8;

            switch(position)
            {
              case 0:
                b = b | 0x80;
                break;
              case 1:
                b = b | 0x40;
                break;
              case 2:
                b = b | 0x20;
                break;
              case 3:
                b = b | 0x10;
                break;
              case 4:
                b = b | 0x08;
                break;
              case 5:
                b = b | 0x04;
                break;
              case 6:
                b = b | 0x02;
                break;
              case 7:
                b = b | 0x01;
                break;
            }

            t->distinct_vals[((t->array[i][j] - t->columns_stats[i].i_A) % N)/8] = b;
          }
          else
          {
            int8_t b = t->distinct_vals[(t->array[i][j] - t->columns_stats[i].i_A)/8];
            int position = (t->array[i][j] - t->columns_stats[i].i_A)%8;

            switch(position)
            {
              case 0:
                b = b | 0x80;
                break;
              case 1:
                b = b | 0x40;
                break;
              case 2:
                b = b | 0x20;
                break;
              case 3:
                b = b | 0x10;
                break;
              case 4:
                b = b | 0x08;
                break;
              case 5:
                b = b | 0x04;
                break;
              case 6:
                b = b | 0x02;
                break;
              case 7:
                b = b | 0x01;
                break;
            }

            t->distinct_vals[(t->array[i][j] - t->columns_stats[i].i_A)/8] = b;
          }
        }

        for(uint64_t j = 0; j < t->num_vals; j++)
        {
          int8_t b = t->distinct_vals[j];
          for(int k = 0; k < 8; k++)
          {
            if(b < 0)
              t->columns_stats[i].d_A++;
            b = b<<1;
          }
        }

        //printf("%llu\n%llu\n%llu\n%llu\n\n", t->columns_stats[i].i_A, t->columns_stats[i].u_A, t->columns_stats[i].f_A, t->columns_stats[i].d_A);
    }

    fclose(fp);

    return 0;

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
table_index *insert_tables_from_list(string_list *list)
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
  for(uint32_t i=0; i<ti->num_tables; i++)
  {
    filename = string_list_remove(list);
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
  if(ti == NULL)
  {
    fprintf(stderr, "get_table: table index is NULL\n");
    return NULL;
  }

  if(ti->tables == NULL)
  {
    fprintf(stderr, "get_table: tables of table index is NULL\n");
    return NULL;
  }

  for(uint32_t i=0; i<ti->num_tables; i++)
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
  for(uint32_t i=0; i<ti->num_tables; i++)
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
  string_list *list = read_tables();
  table_index *ti = insert_tables_from_list(list);
  return ti;
}
