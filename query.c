#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <stdbool.h> 
#include "query.h"

query* create_query()
{
    query* q=NULL;
    q=malloc(sizeof(query));
    if(q==NULL)
    {
        perror("create_query: error in malloc");
        return NULL;
    }
    q->number_of_predicates=0;
    q->number_of_projections=0;
    q->number_of_tables=0;
    q->predicates=NULL;
    q->projections=NULL;
    q->table_ids=NULL;
    return q;
}

void delete_query(query*q)
{
    if(q==NULL)
    {
        fprintf(stderr, "delete_query: NULL query pointer\n");
    }
    if(q->number_of_predicates!=0&&q->predicates==NULL)
    {
        fprintf(stderr, "delete_query: NULL predicates pointer but counter !=0\n");
    }
    if(q->number_of_projections!=0&&q->projections==NULL)
    {
        fprintf(stderr, "delete_query: NULL projection pointer but counter !=0\n");
    }
    if(q->number_of_tables!=0&&q->table_ids==NULL)
    {
        fprintf(stderr, "delete_query: NULL table_ids pointer but counter !=0\n");
    }
    if(q->predicates!=NULL)
    {
        for(unsigned int i=0; i<q->number_of_predicates; i++)
        {
            if(q->predicates[i].p!=NULL)
            {
                free(q->predicates[i].p);
                q->predicates[i].p=NULL;
            }
        }
        free(q->predicates);
        q->predicates=NULL;
    }
    if(q->projections!=NULL)
    {
        free(q->projections);
        q->projections=NULL;
    }
    if(q->table_ids!=NULL)
    {
        free(q->table_ids);
        q->table_ids=NULL;
    }
    q=NULL;
}

/**
 * Parse the string representation of the predicate
 * @param char* The string representation of the predicate
 * @param predicate* Pointer to a predicate to write the data
 * @return 0 on succes
 */
int parse_predicate(char* token, predicate*p)
{
    if(token==NULL||p==NULL||token[0]=='\0')
    {
        fprintf(stderr, "parse_predicate: NULL parameter\n");
        return -1;
    }
    //Find the type of predicate by counting the dots
    unsigned int dots=0;
    predicate_type p_type;
    predicate_filter_type p_f_type=Not_Specified;
    unsigned int i=0;
    while(token[i]!='\0')
    {
        if(token[i]=='.')
        {
            dots++;
        }
        else if(token[i]=='=')
        {
            if(p_f_type==Not_Specified)
            {
                p_f_type=Equal;
            }
            else if(p_f_type==Less)
            {
                p_f_type=Less_Equal;
            }
            else if(p_f_type==More)
            {
                p_f_type=More_Equal;
            }
            else
            {
                fprintf(stderr, "analyze_query: wrong operation symbol %s\n", token);
                return -2;
            }
        }
        else if(token[i]=='<')
        {
            if(p_f_type==Not_Specified)
            {
                p_f_type=Less;
            }
            else
            {
                fprintf(stderr, "analyze_query: wrong operation symbol %s\n", token);
                return -3;
            }
        }
        else if(token[i]=='>')
        {
            if(p_f_type==Not_Specified)
            {
                p_f_type=More;
            }
            else if(p_f_type==Less)
            {
                p_f_type=Not_Equal;
            }
            else
            {
                fprintf(stderr, "analyze_query: wrong operation symbol %s\n", token);
                return -4;
            }
        }
        i++;
    }
    if(dots==2&&p_f_type==Equal)
    {//Join predicate
        p->type=Join;
        p->p=malloc(sizeof(predicate_join));
        if(p->p==NULL)
        {
            perror("parse_predicate: predicate join malloc error\n");
            return -5;
        }
        if(sscanf(token, "%u.%u=%u.%u", &(((predicate_join*) p->p)->r.table_id), &(((predicate_join*) p->p)->r.column_id), &(((predicate_join*) p->p)->s.table_id), &(((predicate_join*) p->p)->s.column_id))!=4)
        {
            fprintf(stderr, "parse_predicate: predicate join sscanf error %s\n", token);
            return -6;
        }
    }
    else if(dots==1)
    {
        p->type=Filter;
        p->p=malloc(sizeof(predicate_filter));
        if(p->p==NULL)
        {
            perror("parse_predicate: predicate filter malloc error\n");
            return -7;
        }
        ((predicate_filter*) p->p)->filter_type=p_f_type;
        if(((predicate_filter*) p->p)->filter_type==Less)
        {
            if(sscanf(token, "%u.%u<%"PRIu64, &((predicate_filter*) p->p)->r.table_id, &((predicate_filter*) p->p)->r.column_id, &((predicate_filter*) p->p)->value)!=3)
            {
                fprintf(stderr, "parse_predicate: predicate join sscanf error %s\n", token);
                return -8;
            }
        }
        else if(((predicate_filter*) p->p)->filter_type==Less_Equal)
        {
            if(sscanf(token, "%u.%u<=%"PRIu64, &((predicate_filter*) p->p)->r.table_id, &((predicate_filter*) p->p)->r.column_id, &((predicate_filter*) p->p)->value)!=3)
            {
                fprintf(stderr, "parse_predicate: predicate join sscanf error %s\n", token);
                return -9;
            }
        }
        else if(((predicate_filter*) p->p)->filter_type==Equal)
        {
            if(sscanf(token, "%u.%u=%"PRIu64, &((predicate_filter*) p->p)->r.table_id, &((predicate_filter*) p->p)->r.column_id, &((predicate_filter*) p->p)->value)!=3)
            {
                fprintf(stderr, "parse_predicate: predicate join sscanf error %s\n", token);
                return -10;
            }
        }
        else if(((predicate_filter*) p->p)->filter_type==Not_Equal)
        {
            if(sscanf(token, "%u.%u<>%"PRIu64, &((predicate_filter*) p->p)->r.table_id, &((predicate_filter*) p->p)->r.column_id, &((predicate_filter*) p->p)->value)!=3)
            {
                fprintf(stderr, "parse_predicate: predicate join sscanf error %s\n", token);
                return -11;
            }
        }
        else if(((predicate_filter*) p->p)->filter_type==More)
        {
            if(sscanf(token, "%u.%u>%"PRIu64, &((predicate_filter*) p->p)->r.table_id, &((predicate_filter*) p->p)->r.column_id, &((predicate_filter*) p->p)->value)!=3)
            {
                fprintf(stderr, "parse_predicate: predicate join sscanf error %s\n", token);
                return -12;
            }
        }
        else if(((predicate_filter*) p->p)->filter_type==More_Equal)
        {
            if(sscanf(token, "%u.%u>=%"PRIu64, &((predicate_filter*) p->p)->r.table_id, &((predicate_filter*) p->p)->r.column_id, &((predicate_filter*) p->p)->value)!=3)
            {
                fprintf(stderr, "parse_predicate: predicate join sscanf error %s\n", token);
                return -13;
            }
        }
        else
        {
            fprintf(stderr, "parse_predicate: predicate wrong format\n", token);
            return -14;
        }
    }
    else
    {
        fprintf(stderr, "parse_predicate: predicate format error %s\n", token);
        return -15;
    }
    return 0;
}

/**
 * Removes the delimiter characters at the start/end of the str 
 * and then the duplicates inside the str
 * Example:(Input "---This--is----a-test----",'-', Result"This-is-a-test")
 * @param char* The str
 * @param char The delimiter character
 */
void remove_extra_chars(char* str, char delimiter)
{
    if(str==NULL)
    {
        return;
    }
    //    printf("Before|%s|\n", str);
    size_t i=0;
    size_t j=0;
    //Remove leading spaces
    while(str[i]!='\0'&&str[i]==delimiter)
    {
        i++;
    }
    while(str[j+i]!='\0')
    {
        str[j]=str[j+i];
        j++;
    }
    str[j]=str[j+i];
    //    printf("|%s|\n",str);
    //Remove trailing spaces
    i=0;
    j=0;
    size_t len=strlen(str);
    if(len>0)
    {
        unsigned int end_index=len-1;
        while(i<len)
        {
            if(str[end_index-i]!=delimiter)
            {
                break;
            }
            i++;
        }
        str[end_index-i+1]='\0';
    }
    //    printf("|%s|\n",str);
    //Remove duplicate spaces
    i=0;
    j=0;
    int space=0;
    while(str[i]!='\0')
    {
        //        printf("%zu %zu |%s|\n",i,j,str);
        if(str[i]!=delimiter)
        {
            str[j]=str[i];
            j++;
            space=0;
        }
        else if(space==0)
        {
            str[j]=str[i];
            j++;
            space++;
        }
        else
        {
            space++;
        }
        i++;
    }
    str[j]='\0';
    //    printf("After|%s|\n", str);
}

int analyze_query(char*query_str, query*q)
{
    /////////////////////////////////////////////////////////////////////Add Integrity check after this function
    if(query_str==NULL)
    {
        fprintf(stderr, "analyze_query: NULL query str parameter\n");
        return -1;
    }
    if(q==NULL)
    {
        fprintf(stderr, "analyze_query: NULL query pointer parameter\n");
        return -2;
    }
    if(q->number_of_predicates!=0||q->number_of_projections!=0||q->number_of_tables!=0||
            q->predicates!=NULL||q->projections!=NULL||q->table_ids!=NULL)
    {
        fprintf(stderr, "analyze_query: NULL query parameter not empty\n");
        return -3;
    }
    //Check for illegal characters and for 2 |
    short pipe_counter=0;
    for(unsigned int i=0; query_str[i]!='\0'; i++)
    {
        if(query_str[i]<'0'||query_str[i]>'9')//Not a digit
        {
            if(query_str[i]!='&'&&query_str[i]!='.'&&query_str[i]!=' '&&query_str[i]!='>'&&query_str[i]!='<'&&query_str[i]!='=')
            {
                if(query_str[i]=='|')
                {
                    pipe_counter++;
                }
                else
                {
                    fprintf(stderr, "analyze_query: illegal character: %c\n", query_str[i]);
                    return -4;
                }
            }
        }
    }
    if(pipe_counter!=2)
    {
        fprintf(stderr, "analyze_query: wrong number of \'|\' expected: 2 gived: %hd \n", pipe_counter);
        return -5;
    }
    //Tokenize the string with delimiter the |
    char *save_query_ptr;
    char* token=strtok_r(query_str, "|", &save_query_ptr);
    int i=0;
    if(token==NULL)
    {
        fprintf(stderr, "analyze_query: error with strtok_r table token\n");
        return -6;
    }
    else
    {
        //Parse tables
        //        printf("%d: %s\n", i, token);
        unsigned int spaces=0;
        //Remove the extra spaces
        remove_extra_chars(token, ' ');
        //Check for illegal characters in the table token
        //    printf("|%s|\n",token);
        for(unsigned int i=0; token[i]!='\0'; i++)
        {
            if(token[i]<'0'||token[i]>'9')
            {//Not a digit check if space
                if(token[i]==' ')
                {
                    spaces++;
                }
                else
                {
                    fprintf(stderr, "analyze_query: illegal character in tables token: %c\n", token[i]);
                    return -7;
                }
            }
        }
        if(token[0]=='\0')
        {
            fprintf(stderr, "analyze_query: missing numbers at the tables token%s\n", token);
            return -8;
        }
        if(spaces==0)
        {//Only a table parse the number
            q->number_of_tables=1;
            q->table_ids=malloc(sizeof(unsigned int));
            if(q->table_ids==NULL)
            {
                perror("analyze_query: table malloc error\n");
                return -9;
            }
            if(sscanf(token, "%u", &q->table_ids[0])!=1)
            {
                fprintf(stderr, "analyze_query: tables sscanf error %s\n", token);
                return -10;
            }
        }
        else
        {//Tokenize the string with delimiter the ' '
            q->number_of_tables=spaces+1;
            q->table_ids=malloc(sizeof(unsigned int)*q->number_of_tables);
            if(q->table_ids==NULL)
            {
                perror("analyze_query: table malloc error\n");
                return -11;
            }
            //Tokenize the string with delimiter the |
            char *save_query_ptr2;
            char* subtoken=strtok_r(token, " ", &save_query_ptr2);
            int j=0;
            while(subtoken!=NULL)
            {
                //                printf("%d: %s\n", j, subtoken);
                if(sscanf(subtoken, "%u", &q->table_ids[j])!=1)
                {
                    fprintf(stderr, "analyze_query: tables sscanf error %s\n", subtoken);
                    return -12;
                }
                j++;
                subtoken=strtok_r(NULL, " ", &save_query_ptr2);
            }
        }
    }
    //Get predicates token
    token=strtok_r(NULL, "|", &save_query_ptr);
    i++;
    if(token==NULL)
    {
        fprintf(stderr, "analyze_query: error with strtok_r predicate token\n");
        return -13;
    }
    else
    {
        //Parse predicates
        //        printf("%d: %s\n", i, token);
        //Remove the extra spaces
        remove_extra_chars(token, ' ');
        size_t token_size=strlen(token)+1;
        char *token_copy=malloc(sizeof(char)*token_size);
        if(token_copy==NULL)
        {
            perror("analyze_query: char* malloc error\n");
            return -14;
        }
        strncpy(token_copy, token, token_size);
        remove_extra_chars(token_copy, '&');
        //        printf("%d: %s\n", i, token_copy);
        if(strncmp(token_copy, token, token_size)!=0)
        {
            fprintf(stderr, "analyze_query: predicate error with & %s\n", token);
            return -13;
        }
        free(token_copy);
        token_copy=NULL;
        bool number=false;
        bool dot=false;
        bool seperator=false;
        bool operation_symbol=false;
        unsigned int predicate_counter=0;
        //Check for illegal characters in the predicate token
        for(unsigned int i=0; token[i]!='\0'; i++)
        {
            if(token[i]<'0'||token[i]>'9')
            {//Not a digit check if dot or & or <, =, >
                if(token[i]=='.')
                {
                    dot=true;
                }
                else if(token[i]=='&')
                {
                    seperator=true;
                    predicate_counter++;
                }
                else if(token[i]=='<')
                {
                    operation_symbol=true;
                }
                else if(token[i]=='=')
                {
                    operation_symbol=true;
                }
                else if(token[i]=='>')
                {
                    operation_symbol=true;
                }
                else
                {
                    fprintf(stderr, "analyze_query: illegal character in predicate token: %c\n", token[i]);
                    return -14;
                }
            }
            else
            {
                number=true;
            }
        }
        if(token[0]=='\0')
        {//empty predicate
            fprintf(stderr, "analyze_query: Empty Predicate\n");
            return -15;
        }
        else if(number&&dot&&operation_symbol)
        {
            if(seperator)
            {//More than one predicates
                q->number_of_predicates=predicate_counter+1;
                q->predicates=malloc(sizeof(predicate)*q->number_of_predicates);
                if(q->predicates==NULL)
                {
                    perror("analyze_query: predicate malloc error\n");
                    return -16;
                }
                for(unsigned int i=0; i<q->number_of_predicates; i++)
                {
                    q->predicates[i].p=NULL;
                }
                //Tokenize the string with delimiter the '&'
                char *save_query_ptr2;
                char* subtoken=strtok_r(token, "&", &save_query_ptr2);
                int j=0;
                while(subtoken!=NULL)
                {
                    //                    printf("%d: %s\n", j, subtoken);
                    if(parse_predicate(subtoken, &q->predicates[j])!=0)
                    {
                        return -17;
                    }
                    j++;
                    subtoken=strtok_r(NULL, "&", &save_query_ptr2);
                }
            }
            else
            {//One predicate
                q->number_of_predicates=1;
                q->predicates=malloc(sizeof(predicate));
                if(q->predicates==NULL)
                {
                    perror("analyze_query: predicate malloc error\n");
                    return -18;
                }
                q->predicates[0].p=NULL;
                //Find the type of predicate by counting the dots
                if(parse_predicate(token, &q->predicates[0])!=0)
                {
                    return -19;
                }
            }
        }
        else
        {
            fprintf(stderr, "analyze_query: predicate token has wrong format%s\n", token);
            return -20;
        }
    }
    //Get the pjojection token
    token=strtok_r(NULL, "|", &save_query_ptr);
    i++;
    if(token==NULL)
    {
        fprintf(stderr, "analyze_query: error with strtok_r projection token%s\n", token);
        return -21;
    }
    else
    {
        //Parse projections
        //        printf("%d: %s\n", i, token);
        unsigned int spaces=0;
        unsigned int dots=0;
        unsigned int numbers=0;
        //Remove the extra spaces
        remove_extra_chars(token, ' ');
        //Check for illegal characters in the table token
        //    printf("|%s|\n",token);
        for(unsigned int i=0; token[i]!='\0'; i++)
        {
            if(token[i]<'0'||token[i]>'9')
            {//Not a digit check if space of dot
                if(token[i]==' ')
                {
                    spaces++;
                }
                else if(token[i]=='.')
                {
                    dots++;
                }
                else
                {
                    fprintf(stderr, "analyze_query: illegal character in projection token: %c\n", token[i]);
                    return -22;
                }
            }
            else
            {
                numbers++;
            }
        }
        if(numbers==0)
        {
            fprintf(stderr, "analyze_query: missing numbers at the projection token %s\n", token);
            return -23;
        }
        if(spaces==0)
        {//Only a table parse the number
            q->number_of_projections=1;
            q->projections=malloc(sizeof(projection));
            if(q->projections==NULL)
            {
                perror("analyze_query: projections malloc error\n");
                return -24;
            }
            if(sscanf(token, "%u.%u", &q->projections[0].column_to_project.table_id, &q->projections[0].column_to_project.column_id)!=2)
            {
                fprintf(stderr, "analyze_query: projections sscanf error %s\n", token);
                return -25;
            }
        }
        else if(dots=spaces+1)
        {//Tokenize the string with delimiter the ' '
            q->number_of_projections=dots;
            q->projections=malloc(sizeof(projection)*q->number_of_projections);
            if(q->projections==NULL)
            {
                perror("analyze_query: projections malloc error\n");
                return -26;
            }
            //Tokenize the string with delimiter the ' '
            char *save_query_ptr2;
            char* subtoken=strtok_r(token, " ", &save_query_ptr2);
            int j=0;
            while(subtoken!=NULL)
            {
                //                printf("%d: %s\n", j, subtoken);
                if(sscanf(subtoken, "%u.%u", &q->projections[j].column_to_project.table_id, &q->projections[j].column_to_project.column_id)!=2)
                {
                    fprintf(stderr, "analyze_query: projection sscanf error %s\n", subtoken);
                    return -27;
                }
                j++;
                subtoken=strtok_r(NULL, " ", &save_query_ptr2);
            }
        }
        else
        {
            fprintf(stderr, "analyze_query: projections format error %s\n", token);
            return -28;
        }
    }
    return 0;
}

/**
 * Prints the table id and column number
 * @param Pointer to a table_column
 */
void print_table_column(table_column* tc)
{
    if(tc!=NULL)
    {
        printf("Table ID: %u\n", tc->table_id);
        printf("Column ID: %u\n", tc->column_id);
    }
}

/**
 * Prints the predicate type
 * @param predicate_type
 */
void print_predicate_type(predicate_type pt)
{
    printf("Predicate Type: ");
    if(pt==Filter)
    {
        printf("Filter");
    }
    else if(pt==Join)
    {
        printf("Join");
    }
    printf("\n");
}

/**
 * Prints the predicate filter type
 * @param predicate_filter_type
 */
void print_predicate_filter_type(predicate_filter_type ptf)
{
    printf("Predicate Filter Type: ");
    if(ptf==Not_Specified)
    {
        printf("Not Specified");
    }
    else if(ptf==Less)
    {
        printf("Less (<)");
    }
    else if(ptf==Less_Equal)
    {
        printf("Less Equal (<=)");
    }
    else if(ptf==Equal)
    {
        printf("Equal (=)");
    }
    else if(ptf==Not_Equal)
    {
        printf("Not Equal (<>)");
    }
    else if(ptf==More)
    {
        printf("More (>)");
    }
    else if(ptf==More_Equal)
    {
        printf("More Equal (>=)");
    }
    printf("\n");
}

/**
 * Prints the predicate join data
 * @param predicate_join*
 */
void print_predicate_join(predicate_join* pj)
{
    if(pj!=NULL)
    {
        printf("R:\n");
        print_table_column(&(pj->r));
        printf("S:\n");
        print_table_column(&(pj->s));
    }
}

/**
 * Prints the predicate_filter data
 * @param predicate_filter*
 */
void print_predicate_filter(predicate_filter* pf)
{
    if(pf!=NULL)
    {
        print_predicate_filter_type(pf->filter_type);
        printf("R:\n");
        print_table_column(&(pf->r));
        printf("Value: %ld\n", pf->value);
    }
}

/**
 * Prints the predicate data
 * @param predicate*
 */
void print_predicate(predicate* p)
{
    if(p!=NULL)
    {
        print_predicate_type(p->type);
        if(p->type==Filter)
        {
            print_predicate_filter((predicate_filter*) p->p);
        }
        else if(p->type==Join)
        {
            print_predicate_join((predicate_join*) p->p);
        }
        else
        {
            printf("Empty Predicate Type\n");
        }
    }
}

/**
 * Prints the projection data
 * @param projection*
 */
void print_projection(projection* p)
{
    if(p!=NULL)
    {
        print_table_column(&(p->column_to_project));
    }
}

void print_query(query* q)
{
    if(q!=NULL)
    {
        printf("Number of tables: %u\n", q->number_of_tables);
        if(q->table_ids!=NULL)
        {
            for(unsigned int i=0; i<q->number_of_tables; i++)
            {
                printf("Table %u: %u\n", i, q->table_ids[i]);
            }
        }
        printf("Number of predicates: %u\n", q->number_of_predicates);
        if(q->predicates!=NULL)
        {
            for(unsigned int i=0; i<q->number_of_predicates; i++)
            {
                print_predicate(&q->predicates[i]);
            }
        }
        printf("Number of projections: %u\n", q->number_of_projections);
        if(q->projections!=NULL)
        {
            for(unsigned int i=0; i<q->number_of_projections; i++)
            {
                print_projection(&q->projections[i]);
            }
        }
    }
}
