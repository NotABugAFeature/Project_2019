#include <stdio.h>
#include <stdlib.h>
#include "List.h"

/* Creates a ListNode node and returns it */
ListNode *createListNode(Item *item) {
	ListNode *ln = malloc(sizeof(ListNode));
	ln->item = item;
	ln->next = NULL;
}

/* Inserts a pre-existing ListNode node to the end of a list */
void insertListNode(ListNode **head, ListNode *ln) {
	if((*head) == NULL) {
		(*head) = ln;
	}
	else {
		ListNode *temp = *head;
		while(temp->next != NULL) {
			temp = temp->next;
		}
		temp->next = ln;
	}
}

/* Prints a list */
void printList(ListNode *node) {
	while(node != NULL) {
		printItem(node->item);
		node = node->next;
	}
}

/* Prints a list in reverse with appropriate formating */
void printReverseList(ListNode *ListNode) {
	if(ListNode != NULL) {
		printReverseList(ListNode->next);
		printf("->%d->|%s|", ListNode->item->weight, ListNode->item->endNode->name);
	}
}

/* Pushes a node to the top of the list (list functions as a stack) */
void pushListNode(ListNode **head, ListNode *ln) {
	if((*head) == NULL) {
		(*head) = ln;
	}
	else {
		ln->next = *head;
		*head = ln;
	}
}

/* Pops the node from the top of the list (list functions as a stack) */
ListNode *popListNode(ListNode **head) {
	if((*head) == NULL) {
		return NULL;
	}
	else {
		ListNode *temp = *head;
		*head = (*head)->next;
		temp->next = NULL;
		return temp;
	}
}


/* Removes a node from the list and deletes its Item */
ListNode *deleteListNode(ListNode *ln, Item *item) {
    ListNode *temp, *traverse, *prev;

    if(ln == NULL)
        return NULL;

    if(ln->item == item) {
        temp = ln->next;
        delteItem(ln->item);
        free(ln);
        return temp;
    }

    prev = ln;
    traverse = ln;

    while(traverse != NULL)	{
        if(traverse->item == item) {
            temp = traverse;
            prev->next = traverse->next;
            delteItem(temp->item);
            free(temp);
            return ln;
        }
        prev = traverse;
        traverse = traverse->next;
    }

    return ln;
}


/* Removes a node from the List without deleting its Item */
ListNode *removeListNode(ListNode *ln, Item *item)	{
    ListNode *temp, *traverse, *prev;

    if(ln == NULL)
        return NULL;

    if(ln->item == item) {
        temp = ln->next;
        free(ln);
        return temp;
    }

    prev = ln;
    traverse = ln;

    while(traverse != NULL) {
        if(traverse->item == item) {
            temp = traverse;
            prev->next = traverse->next;
            free(temp);
            return ln;
        }
        prev = traverse;
        traverse = traverse->next;
    }

    return ln;
}


/* Deletes the list. Also deletes all the items (deleteItem) */
ListNode *deleteList(ListNode *head) {
    ListNode *temp;

    while(head != NULL) {
        temp = head->next;
        deleteItem(head->item);
        free(head);
        head = temp;
    }
    
    return NULL;
}

/* Deletes the list without deleting its items */
ListNode *cleanList(ListNode *head) {
    ListNode *temp;

    while(head != NULL) {
        temp = head->next;
        free(head);
        head = temp;
    }
    
    return NULL;
}