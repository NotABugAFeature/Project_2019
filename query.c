#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include "query.h"

query* create_query(void)
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
        return;
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
        for(uint32_t i=0; i<q->number_of_predicates; i++)
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
    free(q);
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
    uint32_t dots=0;
    predicate_filter_type p_f_type=Not_Specified;
    uint32_t i=0;
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
            else if(p_f_type==Greater)
            {
                p_f_type=Greater_Equal;
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
                p_f_type=Greater;
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
        if(sscanf(token, "%"PRIu32".%"PRIu32"=%"PRIu32".%"PRIu32, &(((predicate_join*) p->p)->r.table_index), &(((predicate_join*) p->p)->r.column_index), &(((predicate_join*) p->p)->s.table_index), &(((predicate_join*) p->p)->s.column_index))!=4)
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
            if(sscanf(token, "%"PRIu32".%"PRIu32"<%"PRIu64, &((predicate_filter*) p->p)->r.table_index, &((predicate_filter*) p->p)->r.column_index, &((predicate_filter*) p->p)->value)!=3)
            {
                fprintf(stderr, "parse_predicate: predicate join sscanf error %s\n", token);
                return -8;
            }
        }
        else if(((predicate_filter*) p->p)->filter_type==Less_Equal)
        {
            if(sscanf(token, "%"PRIu32".%"PRIu32"<=%"PRIu64, &((predicate_filter*) p->p)->r.table_index, &((predicate_filter*) p->p)->r.column_index, &((predicate_filter*) p->p)->value)!=3)
            {
                fprintf(stderr, "parse_predicate: predicate join sscanf error %s\n", token);
                return -9;
            }
        }
        else if(((predicate_filter*) p->p)->filter_type==Equal)
        {
            if(sscanf(token, "%"PRIu32".%"PRIu32"=%"PRIu64, &((predicate_filter*) p->p)->r.table_index, &((predicate_filter*) p->p)->r.column_index, &((predicate_filter*) p->p)->value)!=3)
            {
                fprintf(stderr, "parse_predicate: predicate join sscanf error %s\n", token);
                return -10;
            }
        }
        else if(((predicate_filter*) p->p)->filter_type==Not_Equal)
        {
            if(sscanf(token, "%"PRIu32".%"PRIu32"<>%"PRIu64, &((predicate_filter*) p->p)->r.table_index, &((predicate_filter*) p->p)->r.column_index, &((predicate_filter*) p->p)->value)!=3)
            {
                fprintf(stderr, "parse_predicate: predicate join sscanf error %s\n", token);
                return -11;
            }
        }
        else if(((predicate_filter*) p->p)->filter_type==Greater)
        {
            if(sscanf(token, "%"PRIu32".%"PRIu32">%"PRIu64, &((predicate_filter*) p->p)->r.table_index, &((predicate_filter*) p->p)->r.column_index, &((predicate_filter*) p->p)->value)!=3)
            {
                fprintf(stderr, "parse_predicate: predicate join sscanf error %s\n", token);
                return -12;
            }
        }
        else if(((predicate_filter*) p->p)->filter_type==Greater_Equal)
        {
            if(sscanf(token, "%"PRIu32".%"PRIu32">=%"PRIu64, &((predicate_filter*) p->p)->r.table_index, &((predicate_filter*) p->p)->r.column_index, &((predicate_filter*) p->p)->value)!=3)
            {
                fprintf(stderr, "parse_predicate: predicate join sscanf error %s\n", token);
                return -13;
            }
        }
        else
        {
            fprintf(stderr, "parse_predicate: predicate wrong format %s\n", token);
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
        size_t end_index=len-1;
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
//    printf("|%s|\n", query_str);
    for(uint32_t i=0; query_str[i]!='\0'; i++)
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
        uint32_t spaces=0;
        //Remove the extra spaces
        remove_extra_chars(token, ' ');
        //Check for illegal characters in the table token
        //    printf("|%s|\n",token);
        for(uint32_t i=0; token[i]!='\0'; i++)
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
            q->table_ids=malloc(sizeof(uint32_t));
            if(q->table_ids==NULL)
            {
                perror("analyze_query: table malloc error\n");
                return -9;
            }
            if(sscanf(token, "%"PRIu32"", &q->table_ids[0])!=1)
            {
                fprintf(stderr, "analyze_query: tables sscanf error %s\n", token);
                return -10;
            }
        }
        else
        {//Tokenize the string with delimiter the ' '
            q->number_of_tables=spaces+1;
            q->table_ids=malloc(sizeof(uint32_t)*q->number_of_tables);
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
                if(sscanf(subtoken, "%"PRIu32"", &q->table_ids[j])!=1)
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
        uint32_t predicate_counter=0;
        //Check for illegal characters in the predicate token
        for(uint32_t i=0; token[i]!='\0'; i++)
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
            {//Greater than one predicates
                q->number_of_predicates=predicate_counter+1;
                q->predicates=malloc(sizeof(predicate)*q->number_of_predicates);
                if(q->predicates==NULL)
                {
                    perror("analyze_query: predicate malloc error\n");
                    return -16;
                }
                for(uint32_t i=0; i<q->number_of_predicates; i++)
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
        uint32_t spaces=0;
        uint32_t dots=0;
        uint32_t numbers=0;
        //Remove the extra spaces
        remove_extra_chars(token, ' ');
        //Check for illegal characters in the table token
        //    printf("|%s|\n",token);
        for(uint32_t i=0; token[i]!='\0'; i++)
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
            if(sscanf(token, "%"PRIu32".%"PRIu32"", &q->projections[0].column_to_project.table_index, &q->projections[0].column_to_project.column_index)!=2)
            {
                fprintf(stderr, "analyze_query: projections sscanf error %s\n", token);
                return -25;
            }
        }
        else if(dots==spaces+1)
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
                if(sscanf(subtoken, "%"PRIu32".%"PRIu32"", &q->projections[j].column_to_project.table_index, &q->projections[j].column_to_project.column_index)!=2)
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
        printf("Table Index: %"PRIu32"\n", tc->table_index);
        printf("Column Index: %"PRIu32"\n", tc->column_index);
    }
    else
    {
        fprintf(stderr, "print_table_column: NULL Parameter\n");
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
    else if(pt==Self_Join)
    {
        printf("Self Join");
    }
    else
    {
        printf("ERROR");
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
    else if(ptf==Greater)
    {
        printf("Greater (>)");
    }
    else if(ptf==Greater_Equal)
    {
        printf("Greater Equal (>=)");
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
    else
    {
        fprintf(stderr, "print_predicate_join: NULL Parameter\n");
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
    else
    {
        fprintf(stderr, "print_predicate_filter: NULL Parameter\n");
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
        else if(p->type==Join||p->type==Self_Join)
        {
            print_predicate_join((predicate_join*) p->p);
        }
        else
        {
            printf("Empty Predicate Type\n");
        }
    }
    else
    {
        fprintf(stderr, "print_predicate: NULL Parameter\n");
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
    else
    {
        fprintf(stderr, "print_projection: NULL Parameter\n");
    }
}

void print_query(query* q)
{
    if(q!=NULL)
    {
        printf("Number of tables: %"PRIu32"\n", q->number_of_tables);
        if(q->table_ids!=NULL)
        {
            for(uint32_t i=0; i<q->number_of_tables; i++)
            {
                printf("Table ID: %"PRIu32": %"PRIu32"\n", i, q->table_ids[i]);
            }
        }
        printf("Number of predicates: %"PRIu32"\n", q->number_of_predicates);
        if(q->predicates!=NULL)
        {
            for(uint32_t i=0; i<q->number_of_predicates; i++)
            {
                print_predicate(&q->predicates[i]);
            }
        }
        printf("Number of projections: %"PRIu32"\n", q->number_of_projections);
        if(q->projections!=NULL)
        {
            for(uint32_t i=0; i<q->number_of_projections; i++)
            {
                print_projection(&q->projections[i]);
            }
        }
    }
    else
    {
        fprintf(stderr, "print_query: NULL Parameter\n");
    }
}

bool compare_predicates(predicate* p1, predicate* p2)
{
    if(p1->type!=p2->type)
    {
        return false;
    }
    if(p1->type==Join||p1->type==Self_Join)
    {
        if((((predicate_join*) (p1->p))->r.table_index==((predicate_join*) (p2->p))->r.table_index&&
                ((predicate_join*) (p1->p))->r.column_index==((predicate_join*) (p2->p))->r.column_index&&
                ((predicate_join*) (p1->p))->s.table_index==((predicate_join*) (p2->p))->s.table_index&&
                ((predicate_join*) (p1->p))->s.column_index==((predicate_join*) (p2->p))->s.column_index)||
                (((predicate_join*) (p1->p))->r.table_index==((predicate_join*) (p2->p))->s.table_index&&
                ((predicate_join*) (p1->p))->r.column_index==((predicate_join*) (p2->p))->s.column_index&&
                ((predicate_join*) (p1->p))->s.table_index==((predicate_join*) (p2->p))->r.table_index&&
                ((predicate_join*) (p1->p))->s.column_index==((predicate_join*) (p2->p))->r.column_index))
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    else
    {
        if(((predicate_filter*) (p1->p))->r.table_index!=((predicate_filter*) (p2->p))->r.table_index||
                ((predicate_filter*) (p1->p))->r.column_index!=((predicate_filter*) (p2->p))->r.column_index||
                ((predicate_filter*) (p1->p))->filter_type!=((predicate_filter*) (p2->p))->filter_type||
                ((predicate_filter*) (p1->p))->value!=((predicate_filter*) (p2->p))->value)
        {
            return false;
        }
    }
    return true;
}

void swap_predicates(predicate* p1, predicate* p2)
{
    predicate t=*p1;
    *p1=*p2;
    *p2=t;
}

int validate_query(query*q, table_index* ti)
{
    //Check the parameters
    if(q==NULL||ti==NULL||ti->num_tables==0||ti->tables==NULL||
            q->number_of_tables==0||q->number_of_predicates==0||
            q->number_of_projections==0||q->table_ids==NULL||q->predicates==NULL||
            q->projections==NULL)
    {
        fprintf(stderr, "validate_query: Error with the parameters\n");
        return -1;
    }
    ////////////////////////////////////////////////Add 2 2 3|0.1=1.1 -> 2 3|0.0=0.1
    ////////////////////////////////////////////////Add table -> check inside predicate
    //Add two arrays one uint32_t and one bool
    uint32_t* table_indexes=malloc(sizeof(uint32_t)*q->number_of_tables);
    if(table_indexes==NULL)
    {
        perror("validate_query: table_indexes malloc error");
        return -2;
    }
    bool* table_used=malloc(sizeof(bool)*q->number_of_tables);
    if(table_used==NULL)
    {
        free(table_indexes);
        perror("validate_query: table_indexes malloc error");
        return -3;
    }
    for(uint32_t i=0; i<q->number_of_tables; i++)
    {
        table_indexes[i]=i;
        table_used[i]=false;
    }
    //Verify the table ids and check for duplicates
    for(uint32_t i=0; i<q->number_of_tables; i++)
    {
        if(get_table(ti, q->table_ids[i])==NULL)
        {
            free(table_indexes);
            free(table_used);
            fprintf(stderr, "validate_query: Table index: %"PRIu32" not in tables\n", q->table_ids[i]);
            return -4;
        }
        //Check for duplicates
        if(table_indexes[i]==i)//Not a duplicate
        {
            for(uint32_t j=i+1; j<q->number_of_tables; j++)
            {
                if(q->table_ids[i]==q->table_ids[j])
                {//Duplicate table Ids
                    printf("Duplicate table ids\n");
                    table_indexes[j]=i;
                }
            }
        }
    }
//    for(uint32_t i=0; i<q->number_of_tables; i++)
//    {
//        printf("%d ", table_used[i]);
//    }
//    printf("\n");
//    for(uint32_t i=0; i<q->number_of_tables; i++)
//    {
//        printf("%u ", table_indexes[i]);
//    }
//    printf("\n");
    //Verify the predicates
    for(uint32_t i=0; i<q->number_of_predicates; i++)
    {
        if(q->predicates[i].type==Join)
        {
            //Check if the r table id exists
            //            if(!check_table_array(q->table_ids,q->number_of_tables,((predicate_join*)q->predicates[i].p)->r.table_id))
            //            printf("rt%"PRIu32" %"PRIu32"\n", q->number_of_tables, ((predicate_join*) q->predicates[i].p)->r.table_index);
            if(q->number_of_tables<=((predicate_join*) q->predicates[i].p)->r.table_index)
            {
                free(table_indexes);
                free(table_used);
                fprintf(stderr, "validate_query: Table r id in predicate join not in the table array\n");
                return -5;
            }
            //            table_used[((predicate_join*) q->predicates[i].p)->r.table_index]=true;
            //Change the table r index if duplicate
            if(((predicate_join*) q->predicates[i].p)->r.table_index!=table_indexes[((predicate_join*) q->predicates[i].p)->r.table_index])
            {
                ((predicate_join*) q->predicates[i].p)->r.table_index=table_indexes[((predicate_join*) q->predicates[i].p)->r.table_index];
                //                table_used[((predicate_join*) q->predicates[i].p)->r.table_index]=true;
            }
            table_used[((predicate_join*) q->predicates[i].p)->r.table_index]=true;
            //            printf("rc%"PRIu32" %"PRIu32"\n", ((predicate_join*) q->predicates[i].p)->r.column_index, get_table(ti, q->table_ids[((predicate_join*) q->predicates[i].p)->r.table_index])->columns);
            //Check if the r column exists
            if(((predicate_join*) q->predicates[i].p)->r.column_index>=get_table(ti, q->table_ids[((predicate_join*) q->predicates[i].p)->r.table_index])->columns)
            {
                free(table_indexes);
                free(table_used);
                fprintf(stderr, "validate_query: Table r column in predicate join does not exist\n");
                return -6;
            }
            //Check if the s table id exists
            //            printf("st%"PRIu32" %"PRIu32"\n", q->number_of_tables, ((predicate_join*) q->predicates[i].p)->s.table_index);
            //            if(!check_table_array(q->table_ids,q->number_of_tables,((predicate_join*)q->predicates[i].p)->s.table_id))
            if(q->number_of_tables<=((predicate_join*) q->predicates[i].p)->s.table_index)
            {
                free(table_indexes);
                free(table_used);
                fprintf(stderr, "validate_query: Table s id in predicate join not in the table array\n");
                return -7;
            }
            //            table_used[((predicate_join*) q->predicates[i].p)->s.table_index]=true;
            //Change the table s index if duplicate
            if(((predicate_join*) q->predicates[i].p)->s.table_index!=table_indexes[((predicate_join*) q->predicates[i].p)->s.table_index])
            {
                ((predicate_join*) q->predicates[i].p)->s.table_index=table_indexes[((predicate_join*) q->predicates[i].p)->s.table_index];
                //                table_used[((predicate_join*) q->predicates[i].p)->s.table_index]=true;
            }
            table_used[((predicate_join*) q->predicates[i].p)->s.table_index]=true;
            //            printf("sc%"PRIu32" %"PRIu32"\n", ((predicate_join*) q->predicates[i].p)->s.column_index, get_table(ti, q->table_ids[((predicate_join*) q->predicates[i].p)->s.table_index])->columns);
            //Check if the s column exists
            if(((predicate_join*) q->predicates[i].p)->s.column_index>=get_table(ti, q->table_ids[((predicate_join*) q->predicates[i].p)->s.table_index])->columns)
            {
                free(table_indexes);
                free(table_used);
                fprintf(stderr, "validate_query: Table s column in predicate join does not exist\n");
                return -8;
            }
            //Check if it is a self join
            if((((predicate_join*) q->predicates[i].p)->r.table_index)==((predicate_join*) q->predicates[i].p)->s.table_index)
            {
                q->predicates[i].type=Self_Join;
                //Check if the columns id are equal
                if((((predicate_join*) q->predicates[i].p)->r.column_index)==((predicate_join*) q->predicates[i].p)->s.column_index)
                {
                    if(i!=q->number_of_predicates)
                    {
                        swap_predicates(&q->predicates[i], &q->predicates[q->number_of_predicates-1]);
                    }
                    free(q->predicates[q->number_of_predicates-1].p);
                    q->predicates[q->number_of_predicates-1].p=NULL;
                    q->number_of_predicates--;
                    i--;
                }
            }
        }
        else if(q->predicates[i].type==Filter)
        {
            //Check if the r table id exists
            //            if(!check_table_array(q->table_ids,q->number_of_tables,((predicate_filter*)q->predicates[i].p)->r.table_id))
            if(q->number_of_tables<=((predicate_filter*) q->predicates[i].p)->r.table_index)
            {
                free(table_indexes);
                free(table_used);
                fprintf(stderr, "validate_query: Table r id in predicate filter not in the table array\n");
                return -9;
            }
            //            table_used[((predicate_filter*) q->predicates[i].p)->r.table_index]=true;
            //Change the table r index if duplicate
            if(((predicate_filter*) q->predicates[i].p)->r.table_index!=table_indexes[((predicate_filter*) q->predicates[i].p)->r.table_index])
            {
                ((predicate_filter*) q->predicates[i].p)->r.table_index=table_indexes[((predicate_filter*) q->predicates[i].p)->r.table_index];
                //                table_used[((predicate_filter*) q->predicates[i].p)->r.table_index]=true;
            }
            table_used[((predicate_filter*) q->predicates[i].p)->r.table_index]=true;
            //Check if the r column exists
            if(((predicate_filter*) q->predicates[i].p)->r.column_index>=get_table(ti, q->table_ids[((predicate_filter*) q->predicates[i].p)->r.table_index])->columns)
            {
                free(table_indexes);
                free(table_used);
                fprintf(stderr, "validate_query: Table r column in predicate filter does not exist\n");
                return -10;
            }
        }
    }
    //Verify the projections
    for(uint32_t i=0; i<q->number_of_projections; i++)
    {
        //        print_projection(&q->projections[i]);
        //        if(!check_table_array(q->table_ids,q->number_of_tables,q->projections[i]->column_to_project->table_id))
        if(q->number_of_tables<=q->projections[i].column_to_project.table_index)
        {
            free(table_indexes);
            free(table_used);
            fprintf(stderr, "validate_query: projection table id not in the table array\n");
            return -11;
        }
        //Change the table index if duplicate
        if(q->projections[i].column_to_project.table_index!=table_indexes[q->projections[i].column_to_project.table_index])
        {
            q->projections[i].column_to_project.table_index=table_indexes[q->projections[i].column_to_project.table_index];
        }
        table_used[q->projections[i].column_to_project.table_index]=true;
        if(q->projections[i].column_to_project.column_index>=get_table(ti, q->table_ids[q->projections[i].column_to_project.table_index])->columns)
        {
            free(table_indexes);
            free(table_used);
            fprintf(stderr, "validate_query: projection column does not exist\n");
            return -12;
        }
    }
//    printf("Check\n");
    //Check if all the tables are used
    //If not remove them and change the indexes in the predicates and projections if needed
    for(uint32_t i=0; i<q->number_of_tables; i++)
    {
//        for(uint32_t j=0; j<q->number_of_tables; j++)
//        {
//            printf("%u ", table_indexes[j]);
//        }
//        printf("\n");
        if(table_used[i]==false)//Table not used
        {
//            printf("Not Used %"PRIu32"\n", i);
            //            not_used++;
            for(uint32_t j=i+1; j<q->number_of_tables; j++)
            {
                if(table_indexes[i]<=table_indexes[j]&&table_indexes[j]>0)
                {
                    table_indexes[j]--;
                }
            }
        }
    }
    //    for(uint32_t i=0; i<q->number_of_tables; i++)
    //    {
    //Change the predicates and the projections
    for(uint32_t i=0; i<q->number_of_predicates; i++)
    {
//        print_predicate(&q->predicates[i]);
        if(q->predicates[i].type==Join||q->predicates[i].type==Self_Join)
        {
            //Change the table r index if needed
            if(((predicate_join*) q->predicates[i].p)->r.table_index!=table_indexes[((predicate_join*) q->predicates[i].p)->r.table_index])
            {
//                printf("Old: %u New: %u\n", ((predicate_join*) q->predicates[i].p)->r.table_index, table_indexes[((predicate_join*) q->predicates[i].p)->r.table_index]);
                ((predicate_join*) q->predicates[i].p)->r.table_index=table_indexes[((predicate_join*) q->predicates[i].p)->r.table_index];
            }
            //Change the table s index if needed
            if(((predicate_join*) q->predicates[i].p)->s.table_index!=table_indexes[((predicate_join*) q->predicates[i].p)->s.table_index])
            {
//                printf("Old: %u New: %u\n", ((predicate_join*) q->predicates[i].p)->s.table_index, table_indexes[((predicate_join*) q->predicates[i].p)->s.table_index]);
                ((predicate_join*) q->predicates[i].p)->s.table_index=table_indexes[((predicate_join*) q->predicates[i].p)->s.table_index];
            }
        }
        else if(q->predicates[i].type==Filter)
        {
            //Change the table r index if needed
            if(((predicate_filter*) q->predicates[i].p)->r.table_index!=table_indexes[((predicate_filter*) q->predicates[i].p)->r.table_index])
            {
//                printf("Old: %u New: %u\n", ((predicate_filter*) q->predicates[i].p)->r.table_index, table_indexes[((predicate_filter*) q->predicates[i].p)->r.table_index]);
                ((predicate_filter*) q->predicates[i].p)->r.table_index=table_indexes[((predicate_filter*) q->predicates[i].p)->r.table_index];
            }
        }
    }
    for(uint32_t i=0; i<q->number_of_projections; i++)
    {
        //Change the table index if needed
        if(q->projections[i].column_to_project.table_index!=table_indexes[q->projections[i].column_to_project.table_index])
        {
            q->projections[i].column_to_project.table_index=table_indexes[q->projections[i].column_to_project.table_index];
        }
    }
    //    }
    //Remove duplicate predicates
    for(uint32_t i=0; i<q->number_of_predicates; i++)
    {
        for(uint32_t j=i+1; j<q->number_of_predicates; j++)
        {
            if(compare_predicates(&q->predicates[i], &q->predicates[j]))
            {
                if(j!=q->number_of_predicates)
                {
                    swap_predicates(&q->predicates[j], &q->predicates[q->number_of_predicates-1]);
                }
                free(q->predicates[q->number_of_predicates-1].p);
                q->predicates[q->number_of_predicates-1].p=NULL;
                q->number_of_predicates--;
                j--;
            }
        }
    }
//    for(uint32_t i=0; i<q->number_of_tables; i++)
//    {
////        printf("%d ", table_used[i]);
//    }
//    printf("\n");
//    for(uint32_t i=0; i<q->number_of_tables; i++)
//    {
//        printf("%u ", table_indexes[i]);
//    }
//    printf("\n");

    uint32_t removed=0;
    for(uint32_t i=0; i<q->number_of_tables; i++)
    {
        if(table_used[i+removed]==false)//Table not used
        {
//            printf("%u %u\n", i, q->number_of_tables);
            //            print_query(q);
            uint32_t z=i;
            for(uint32_t j=z+1; j<q->number_of_tables; j++, z++)
            {
                q->table_ids[z]=q->table_ids[j];
            }
            q->number_of_tables--;
            removed++;
            i--;
        }
    }
    free(table_indexes);
    free(table_used);
    return 0;
}

void swap_tc_in_predicate(predicate* p)
{
    if(p==NULL||p->type!=Join)
    {
        return;
    }
    table_column temp;
    temp.table_index=((predicate_join*)p->p)->r.table_index;
    temp.column_index=((predicate_join*)p->p)->r.column_index;
    ((predicate_join*)p->p)->r.table_index=((predicate_join*)p->p)->s.table_index;
    ((predicate_join*)p->p)->r.column_index=((predicate_join*)p->p)->s.column_index;
    ((predicate_join*)p->p)->s.table_index=temp.table_index;
    ((predicate_join*)p->p)->s.column_index=temp.column_index;
}
typedef struct counter_node counter_node;
typedef struct counter_node
{
    uint32_t counter;
    table_column tc;
    counter_node* next;
}counter_node;
typedef struct counter_list
{
    uint32_t number_of_nodes;
    counter_node* head;
}counter_list;
counter_node* new_counter_node(void)
{
    counter_node* new_node=malloc(sizeof(counter_node));
    if(new_node==NULL)
    {
        perror("new_counter_node: malloc error");
        return NULL;
    }
    new_node->counter=0;
    new_node->next=NULL;
    new_node->tc.table_index=0;
    new_node->tc.column_index=0;
    return new_node;
}
void print_counter_list(counter_list* cl)
{
//    printf("\t\tprint_counter_list:\n");
    if(cl==NULL)
    {
        printf("NULL Parameter\n");
        return;
    }
    counter_node* temp=cl->head;
    printf("List Counter: %"PRIu32"\n",cl->number_of_nodes);
    uint32_t counter=0;
    while(temp!=NULL)
    {
        printf("%"PRIu32": Table Index: %"PRIu32" Column Index: %"PRIu32"\n",counter,temp->tc.table_index,temp->tc.column_index);
        printf("Counter: %"PRIu32"\n",temp->counter);
        temp=temp->next;
        counter++;
    }
}
int counter_list_append(counter_list*list,table_column* tc/*,predicate_join* p*/)
{
    if(list==NULL||tc==NULL/*||p==NULL*/)
    {
        return -1;
    }
    if(list->head==NULL)
    {
        list->head=new_counter_node();
        if(list->head==NULL)
        {
            return -2;
        }
        list->number_of_nodes++;
        list->head->counter++;
        list->head->tc.table_index=tc->table_index;
        list->head->tc.column_index=tc->column_index;
        return 0;
    }
    counter_node* temp=list->head;
    counter_node* temp_to_add=list->head;
    while(temp!=NULL)
    {
        temp_to_add=temp;
        if(temp->tc.table_index==tc->table_index&&temp->tc.column_index==tc->column_index)
        {//Already in the list
            temp->counter++;
            return 0;
        }
        temp=temp->next;
    }
    temp_to_add->next=new_counter_node();
    if(temp_to_add->next==NULL)
    {
        return -6;
    }
    list->number_of_nodes++;
    temp_to_add->next->counter++;
    temp_to_add->next->tc.table_index=tc->table_index;
    temp_to_add->next->tc.column_index=tc->column_index;
    return 0;
}
int counter_list_remove(counter_list*list,table_column* tc)
{
    if(list==NULL||tc==NULL)
    {
        return -1;
    }
    if(list->head!=NULL)
    {
        if(list->head->tc.table_index==tc->table_index&&list->head->tc.column_index==tc->column_index)
        {//Found in head
            list->head->counter--;
            if(list->head->counter==0)
            {//Delete the node
                counter_node*temp=list->head->next;
                free(list->head);
                list->head=temp;
                list->number_of_nodes--;
            }
            return 0;
        }
    }
    counter_node* temp=list->head;
    while(temp->next!=NULL)
    {
        if(temp->next->tc.table_index==tc->table_index&&temp->next->tc.column_index==tc->column_index)
        {//Found in the list
            temp->next->counter--;
            if(temp->next->counter==0)
            {//Remove The Node
                counter_node*temp_d=temp->next->next;
                free(temp->next);
                temp->next=temp_d;
                list->number_of_nodes--;
            }
            return 0;
        }
        temp=temp->next;
    }
    return 0;
}
uint32_t get_counter(counter_list* list, table_column* tc)
{
    if(list==NULL||tc==NULL)
    {
        return 0;
    }
    counter_node* temp=list->head;
    while(temp!=NULL)
    {
        if(temp->tc.table_index==tc->table_index&&temp->tc.column_index==tc->column_index)
        {//Found in the list
            return temp->counter;
        }
        temp=temp->next;
    }
    return 0;
}
counter_list* create_counter_list(void)
{
    //Create the list
    counter_list* new_list;
    new_list=malloc(sizeof(counter_list));
    if(new_list==NULL)
    {
        perror("create_counter_list(): error in malloc");
        return NULL;
    }
    //Initialize the list to be empty
    new_list->head=NULL;
    new_list->number_of_nodes=0;
    return new_list;
}
void delete_counter_list(counter_list* list)
{
    if(list==NULL)
    {
        printf("delete_counter_list: NULL list pointer\n");
        return;
    }
    counter_node* temp=list->head;
    //Delete all the nodes
    while(list->head!=NULL)
    {
        list->head=temp->next;
        free(temp);
        temp=list->head;
        list->number_of_nodes--;
    }
    free(list);
//    printf("List Deleted\n");
}
int optimize_query(query*q,table_index* ti)
{
    //Check the parameters
    if(q==NULL||ti==NULL||ti->num_tables==0||ti->tables==NULL||
       q->number_of_tables==0||q->number_of_predicates==0||
       q->number_of_projections==0||q->table_ids==NULL||q->predicates==NULL||
       q->projections==NULL)
    {
        fprintf(stderr, "optimize_query: Error with the parameters\n");
        return -1;
    }
    counter_list*c_list=create_counter_list();
    if(c_list==NULL)
    {
        return -2;
    }
    uint64_t* table_row_count=malloc(sizeof(uint64_t)*q->number_of_tables);
    if(table_row_count==NULL)
    {
        perror("optimize_query: malloc error");
        delete_counter_list(c_list);
        return -3;
    }
    //Store the column count of the tables
    for(uint32_t i=0;i<q->number_of_tables;i++)
    {
        table_row_count[i]=(get_table(ti,q->table_ids[i]))->rows;
    }
    //First put the filters and count the join/self joins
    uint32_t j=0;
    for(uint32_t i=0; i<q->number_of_predicates; i++)
    {
        //If Filter move to beginning
        if(q->predicates[i].type==Filter)
        {
            swap_predicates(&q->predicates[j], &q->predicates[i]);
            j++;
        }
        else if(q->predicates[i].type==Join)//Count the table.rowid pairs
        {
            if(counter_list_append(c_list,&(((predicate_join*) (q->predicates[i].p))->r)/*,((predicate_join*)(q->predicates[i].p))*/)!=0)
            {
                delete_counter_list(c_list);
                free(table_row_count);
                printf("optimize_query: counter_list_append error");
                return -5;
            }
            if(counter_list_append(c_list,&(((predicate_join*) (q->predicates[i].p))->s)/*,((predicate_join*)(q->predicates[i].p))*/)!=0)
            {
                delete_counter_list(c_list);
                free(table_row_count);
                printf("optimize_query: counter_list_append error");
                return -6;
            }
        }
    }
    //Then the self joins
    for(uint32_t i=j; i<q->number_of_predicates; i++)
    {
        //If Filter move to Beginning
        if(q->predicates[i].type==Self_Join)
        {
            swap_predicates(&q->predicates[j], &q->predicates[i]);
            j++;
        }
    }
    //Then the joins
    bool next_join=false;
    table_column*next_tc=NULL;
    for(uint32_t i=j; i<q->number_of_predicates&&c_list->number_of_nodes>0; i=j+1)
    {
        //Find the pair that is used most often
        uint32_t max_value=c_list->head->counter;
        uint32_t max_index=0;
        if(!next_join||next_tc==NULL)
        {
            uint64_t lowest_row_count=table_row_count[c_list->head->tc.table_index];
            counter_node* temp=c_list->head->next;
            next_tc=&c_list->head->tc;
            for(uint32_t z=1; z<c_list->number_of_nodes; z++)
            {
                if(max_value<temp->counter||((max_value==temp->counter)&&(lowest_row_count>table_row_count[temp->tc.table_index])))
                {
                    max_value=temp->counter;
                    next_tc=&temp->tc;
                    max_index=z;
                    lowest_row_count=table_row_count[temp->tc.table_index];
                }
                temp=temp->next;
            }
        }
        else
        {
            max_value=get_counter(c_list,next_tc);
//            print_table_column(next_tc);
            if(max_value==0)
            {
                delete_counter_list(c_list);
                free(table_row_count);
                printf("optimize_query: qet value error");
                return -7;
            }
        }
//        printf("Max value %u index %u\n",max_value,max_index);
//        print_table_column(next_tc);
        uint32_t p_index_to_swap=j;
        //Add all the predicates in the beginning
        for(uint32_t z=0;z<max_value;z++)
        {
//            print_query(q);
            //Find the predicate that has a tc that is used the least ammount of times
            //Or if counter==1 the greater value
            for(uint32_t i=j; i<q->number_of_predicates; i++)
            {
                if(((predicate_join*)q->predicates[i].p)->r.table_index==next_tc->table_index&&((predicate_join*)q->predicates[i].p)->r.column_index==next_tc->column_index)
                {//S is different
                    if(get_counter(c_list,&((predicate_join*)q->predicates[i].p)->s)>1)
                    {
                        if(z==max_value-1)//Last occurance of max
                        {
                            next_join=true;
                            swap_tc_in_predicate(&q->predicates[i]);
                            next_tc=&((predicate_join*)q->predicates[i].p)->r;
                            p_index_to_swap=i;
                            break;
                        }
                        else
                        {
                            next_join=false;
                            continue;
                        }
                    }
                    else
                    {
                        next_join=false;
                        p_index_to_swap=i;
                    }
                }
                else if(((predicate_join*)q->predicates[i].p)->s.table_index==next_tc->table_index&&((predicate_join*)q->predicates[i].p)->s.column_index==next_tc->column_index)
                {//R is different
                    if(z!=max_value-1)//Not the last occurance of max
                    {
                        swap_tc_in_predicate(&q->predicates[i]);
                    }
                    if(get_counter(c_list,&((predicate_join*)q->predicates[i].p)->r)==1)
                    {
                        p_index_to_swap=i;
                        next_join=false;
                    }
                }
            }
            {
                //Remove from list
                if(counter_list_remove(c_list,&((predicate_join*)q->predicates[p_index_to_swap].p)->r)!=0)
                {
                    delete_counter_list(c_list);
                    free(table_row_count);
                    printf("optimize_query: counter_list_remove error");
                    return -7;
                }
                if(counter_list_remove(c_list,&((predicate_join*)q->predicates[p_index_to_swap].p)->s)!=0)
                {
                    delete_counter_list(c_list);
                    free(table_row_count);
                    printf("optimize_query: counter_list_remove error");
                    return -7;
                }
                swap_predicates(&q->predicates[j], &q->predicates[p_index_to_swap]);
                j++;
            }
        }
    }
    //Create the bool table
    delete_counter_list(c_list);
    free(table_row_count);
    return 0;
}
int create_sort_array(query*q,bool**t_c_to_sort)
{
    //Check the parameters
    if(q==NULL||t_c_to_sort==NULL||
       *t_c_to_sort!=NULL||q->number_of_tables==0||q->number_of_predicates==0||
       q->number_of_projections==0||q->table_ids==NULL||q->predicates==NULL||
       q->projections==NULL)
    {
        fprintf(stderr, "create_sort_array: Error with the parameters\n");
        return -1;
    }
    bool** joined_tables=malloc(sizeof(bool*)*q->number_of_tables);
    //Array that keeps which table have become one
    //^Used to idendify self joins after two or more tables have joined
    if(joined_tables==NULL)
    {
        perror("create_sort_array: malloc error");
        return -3;
    }
    for(uint32_t i=0;i<q->number_of_tables;i++)
    {
        joined_tables[i]=malloc(sizeof(bool)*q->number_of_tables);
        if(joined_tables[i]==NULL)
        {
            perror("create_sort_array: malloc error");
            for(uint32_t j=0;j<i;j++)
            {
                free(joined_tables[j]);
            }
            free(joined_tables);
            return -3;
        }
    }
    //Initialize to 0
    for(uint32_t i=0;i<q->number_of_tables;i++)
    {
        for(uint32_t j=0;j<q->number_of_tables;j++)
        {
            joined_tables[i][j]=false;
        }
    }
    //Find the hidden self joins
    //And count the real joins
    uint32_t join_counter=0;
    for(uint32_t i=0; i<q->number_of_predicates; i++)
    {
        if(q->predicates[i].type==Join)
        {
            //Check if the arrays have been joined
            if(joined_tables[((predicate_join*)(q->predicates[i].p))->r.table_index][((predicate_join*)(q->predicates[i].p))->s.table_index])
            {//Joined
                q->predicates[i].type=Self_Join;//Set it as Self Join
            }
            else
            {//Not Joined update the array
                join_counter++;
                joined_tables[((predicate_join*)(q->predicates[i].p))->r.table_index][((predicate_join*)(q->predicates[i].p))->s.table_index]=true;
                joined_tables[((predicate_join*)(q->predicates[i].p))->s.table_index][((predicate_join*)(q->predicates[i].p))->r.table_index]=true;
                for(uint32_t j=0; j<q->number_of_tables; j++)
                {
                    if(j!=((predicate_join*)(q->predicates[i].p))->r.table_index)
                    {
                        if(joined_tables[((predicate_join*)(q->predicates[i].p))->r.table_index][j])
                        {//Old Join found Update
                            joined_tables[((predicate_join*)(q->predicates[i].p))->s.table_index][j]=true;
                            joined_tables[j][((predicate_join*)(q->predicates[i].p))->s.table_index]=true;
                        }
                    }
                }
                for(uint32_t j=0; j<q->number_of_tables; j++)
                {
                    if(j!=((predicate_join*)(q->predicates[i].p))->s.table_index)
                    {
                        if(joined_tables[((predicate_join*)(q->predicates[i].p))->s.table_index][j])
                        {//Old Join found Update
                            joined_tables[((predicate_join*)(q->predicates[i].p))->r.table_index][j]=true;
                            joined_tables[j][((predicate_join*)(q->predicates[i].p))->r.table_index]=true;
                        }
                    }
                }
            }
        }
//        for(uint32_t i=0;i<q->number_of_tables;i++)
//        {
//            for(uint32_t j=0;j<q->number_of_tables;j++)
//            {
//                printf("%d",joined_tables[i][j]);
//            }
//            printf("\n");
//        }
//        printf("%u\n",join_counter);
    }
    //Create the bool table
    bool* bool_array=malloc(sizeof(bool)*join_counter*2);
    if(bool_array==NULL)
    {
        perror("create_sort_array: malloc error");
        return -4;
    }
    table_column* last_sorted=NULL;
    uint32_t bool_counter=0;
    for(uint32_t i=0; i<q->number_of_predicates; i++)
    {
        if(q->predicates[i].type==Join)
        {
            bool_array[bool_counter]=true;
            if(last_sorted!=NULL&&last_sorted->table_index==((predicate_join*)(q->predicates[i].p))->r.table_index&&
                    last_sorted->column_index==((predicate_join*)(q->predicates[i].p))->r.column_index)
            {//Already sorted
                bool_array[bool_counter]=false;
            }
            bool_counter++;
            bool_array[bool_counter]=true;
            if(last_sorted!=NULL&&last_sorted->table_index==((predicate_join*)(q->predicates[i].p))->s.table_index&&
                    last_sorted->column_index==((predicate_join*)(q->predicates[i].p))->s.column_index)
            {//Already sorted
                bool_array[bool_counter]=false;
            }
            bool_counter++;
            last_sorted=&((predicate_join*)(q->predicates[i].p))->r;
        }
    }
    *t_c_to_sort=bool_array;
    //Free resources
    for(uint32_t j=0;j<q->number_of_tables;j++)
    {
        free(joined_tables[j]);
    }
    free(joined_tables);
//    return join_counter*2;
    return 0;
}
int optimize_query_after_bool_array(query*q)
{
    //Check the parameters
    if(q==NULL||q->number_of_tables==0||q->number_of_predicates==0||
       q->number_of_projections==0||q->table_ids==NULL||q->predicates==NULL||
       q->projections==NULL)
    {
        fprintf(stderr, "optimize_query_after_bool_array: Error with the parameters\n");
        return -1;
    }
    //First move the filters as much to the right as you can before it is needed
    uint32_t j=0;
    for(uint32_t i=0; i<q->number_of_predicates; i++)
    {
        //If Filter move to beginning
        if(q->predicates[i].type==Filter)
        {
            swap_predicates(&q->predicates[j], &q->predicates[i]);
            j++;
        }
    }
    //Then the self joins to the left after the joins
    for(uint32_t i=j; i<q->number_of_predicates; i++)
    {
        //If Filter move to Beginning
        if(q->predicates[i].type==Self_Join)
        {
            swap_predicates(&q->predicates[j], &q->predicates[i]);
            j++;
        }
    }
    return 0;
}
