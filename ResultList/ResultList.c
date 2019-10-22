/******************************************************************************/
/*  Application         :    Project_2019                                     */
/*  File                :    ResultList.c                                     */
/*  Author              :    Georgakopoulos Panagiotis 1115201600028          */
/*  Team Member         :    Karamhna Maria            1115201600059          */
/*  Team Member         :    Koursiounis Georgios      1115201600077          */
/*  Instructor          :    Sarantis Paskalis                                */
/*  All Tests Conducted At The University's Linux Machines                    */
/******************************************************************************/
/*  This File Contains The Implementation Of The Results List                 */
#include <stdio.h>
#include <stdlib.h>
#include "ResultList.h"

/**
 * Checks If The Bucket Given Is Full
 * @param RowIDArray The Bucket To Check
 * @return int 0 If The Bucket Is Full
 */
int IsResultBucketFull(struct RowIDArray rowids)
{
    if(rowids.index==LISTBUCKETSIZE)
    {
        return 0;
    }
    return 1;
}

int AppendToBucket(struct RowIDArray* rowids, short r_rowId, short s_rowId)
{
    if(IsResultBucketFull(*rowids)!=0)
    {
        rowids->RowIDs[rowids->index][ROWIDRINDEX]=r_rowId;
        rowids->RowIDs[rowids->index][ROWID_S_INDEX]=s_rowId;
        rowids->index++;
        return 0;
    }
    return 1;
}

struct ResultList* CreateResultList()
{
    //Create The List
    struct ResultList* newList;
    newList=malloc(sizeof(struct ResultList));
    if(newList==NULL)
    {
        printf("Error In Malloc\n");
        return NULL;
    }
    //Initialize Empty
    newList->Head=NULL;
    newList->Tail=NULL;
    newList->NumberOfNodes=0;
    return newList;
}

/**
 * Creates And Initializes A New Empty Node (Bucket)
 * @return ResultListNode* The New Node
 */
struct ResultListNode* CreateResultListNode()
{
    //Create The Node/Bucket
    struct ResultListNode* newNode;
    newNode=malloc(sizeof(struct ResultListNode));
    //Initialize Empty
    if(newNode==NULL)
    {
        printf("Error In Malloc\n");
        return NULL;
    }
    newNode->Next=NULL;
    newNode->Result.index=0;
    return newNode;
}

void DeleteResultList(struct ResultList* list)
{
    if(list==NULL)
    {
        printf("DeleteResultList NULL POINTER\n");
        return;
    }
    struct ResultListNode* temp=list->Head;
    //Delete All The Nodes(Buckets)
    while(list->Head!=NULL)
    {
        printf("Nodes: %d\n", list->NumberOfNodes);
        list->Head=temp->Next;
        free(temp);
        temp=list->Head;
        list->NumberOfNodes--;
        printf("Node Deleted\n");
    }
    free(list);
    printf("List Deleted\n");
}

void PrintResultList(struct ResultList* list)
{
    if(list==NULL)
    {
        printf("List Pointer Is NULL");
        return;
    }
    int index=0;
    struct ResultListNode *temp=list->Head;
    printf("Number Of Buckets: %d\n", list->NumberOfNodes);
    while(temp!=NULL)//Visit All The Nodes(Buckets) And Print Them
    {
        printf("Bucket Index: %d\n", index);
        //Print The Array Inside The Bucket
        printf("Index To Add: %u\n", temp->Result.index);
        for(int i=0; i<temp->Result.index; i++)
        {
            printf("RowIdR: %d RowIdS: %d\n", temp->Result.RowIDs[i][ROWIDRINDEX], temp->Result.RowIDs[i][ROWID_S_INDEX]);
        }
        index++;
        temp=temp->Next;
    }
}

int AppendToList(struct ResultList* list, short r_rowId, short s_rowId)
{
    if(list->Head==NULL)//Append As First Node
    {
        list->Head=CreateResultListNode();
        //No Need To Check
        if(AppendToBucket(&list->Head->Result,r_rowId,s_rowId))
        {
            printf("AppendToList Error Cannot Add To Empty Bucket");
            return 1;
        }
        list->Tail=list->Head;
        list->NumberOfNodes++;
    }
    else//Add To The Tail
    {
        if(list->Tail==NULL||list->Tail->Next!=NULL)
        {
            printf("AppendToList: Error Of The List\n");
            return 2;
        }
        else
        {
            if(AppendToBucket(&list->Tail->Result,r_rowId,s_rowId))
            {//Full Bucket
                list->Tail->Next=CreateResultListNode();
                //No Need To Check
                list->Tail=list->Tail->Next;
                if(AppendToBucket(&list->Tail->Result,r_rowId,s_rowId))
                {
                    printf("AppendToList Error Cannot Add To Empty Bucket");
                    return 3;
                }
                list->NumberOfNodes++;
            }
        }
    }
    return 0;
}