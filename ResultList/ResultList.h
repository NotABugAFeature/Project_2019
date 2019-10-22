/******************************************************************************/
/*  Application         :    Project_2019                                     */
/*  File                :    ResultList.h                                     */
/*  Author              :    Georgakopoulos Panagiotis 1115201600028          */
/*  Team Member         :    Karamhna Maria            1115201600059          */
/*  Team Member         :    Koursiounis Georgios      1115201600077          */
/*  Instructor          :    Sarantis Paskalis                                */
/*  All Tests Conducted At The University's Linux Machines                    */
/******************************************************************************/
/*  This File Contains The Declaration Of The Results List                    */

#ifndef RESULTLIST_H
#define RESULTLIST_H

#include <stdint.h>

//#define LISTBUCKETSIZE 1048576/(2*sizeof(uint64_t))
#define LISTBUCKETSIZE 16/(2*sizeof(short))
#define ROWID_R_INDEX 0
#define ROWID_S_INDEX 1
struct ResultList
{
    struct ResultListNode* Head; //The First Node Of The List
    struct ResultListNode* Tail; //The Last Node Of The List
    int NumberOfNodes; //Counter Of The Buckets;
};
struct RowIDArray
{
    short RowIDs[LISTBUCKETSIZE][2];
    unsigned int index;
};
struct ResultListNode
{
    struct RowIDArray Result;
    struct ResultListNode* Next;
};
/**
 * Creates An Empty ResultList And Returns A Pointer To That List
 * @return ResultList* The New List
 */
struct ResultList* CreateResultList();
/**
 * Deletes All The Nodes Of The List Given
 * @param ResultList* The List To Delete
 */
void DeleteResultList(struct ResultList*);

/**
 * Adds A Pair Of Row Ids In The Last Bucket Of The List And Creates A New
 * Bucket If Needed.
 * @param ResultList* The List To Add The Row Ids
 * @param short RowIdR
 * @param short RowIdS
 * @return int 0 If Successful
 */
int AppendToList(struct ResultList* list, short r_rowId, short s_rowId);
/**
 * Prints All The Nodes And Buckets Of The List From First To Last.
 */
void PrintResultList(struct ResultList* list);
/**
 * Returns If The List Is Empty.
 * @return int 0 If Empty Else 1
 */
inline int IsEmpty(struct ResultList* list)
{
    //    if(list->Head==NULL)
    //    {
    //        return 0;
    //    }
    //    return 1;
    if(list->NumberOfNodes==0)
    {
        return 0;
    }
    return 1;
}
/**
 * Counts The Number Of Nodes In The List.
 * @return int The Number Of Nodes
 */
inline int GetNumberOfBuckets(struct ResultList* list)
{
    return list->NumberOfNodes;
}
#endif /*CLIENTLIST_H*/