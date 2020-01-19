#ifndef QUEUE_H
#define QUEUE_H

#include <stdbool.h>
#include <stdint.h>

typedef struct {
	unsigned short byte;
	uint64_t start;
	uint64_t end;
} window;

typedef struct node {
	window *record;
	struct node *next;
} q_node;


typedef struct {
	q_node *head;
	q_node *tail;
} queue;


queue *create_queue();
bool is_empty(queue *);
void push(queue *, window *);
window *pop(queue *);

#endif //QUEUE_H