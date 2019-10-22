

typedef struct {
	Item * item;
	ListNode * next;
} ListNode;




ListNode * createListNode(Item *);							//Creates a list node and returns it
void insertListNode(ListNode ** , ListNode *);				//Inserts a pre-existing ListNode node into a list
void printList(ListNode *);									//Prints a list
void printReverseList(ListNode * list);						//Prints a list in reverse with appropriate formating
void pushListNode(ListNode ** , ListNode *);				//Pushes a node to the top of the list (list functions as a stack)
ListNode * popListNode(ListNode **);						//Pops the node from the top of the list (list functions as a stack)
ListNode *deleteListNode(ListNode *, Item *);				//Removes a node from the list and deletes its Item
ListNode *removeListNode(ListNode *, Item *);				//Removes a node from the list without deleting its Item
ListNode *deleteList(ListNode *);							//Deletes the list. Also deletes all the items (deleteItem)
ListNode *cleanList(ListNode *);							//Deletes the list without deleting its items
