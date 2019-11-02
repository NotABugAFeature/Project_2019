#include <stdio.h>
#include "relation.h"

int main(void)
{
	table *t = read_from_file("bla.txt");
	return t==NULL;
}
