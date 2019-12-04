#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

int main(int argc, char **argv)
{
	uint64_t columns = 3, rows = 10;
	if(argc == 1)
	{
		printf("Usage: %s <filename> (<columns> <rows>)\n", argv[0]);
		return -1;
	}

	char *filename = argv[1];

	if(argc == 4)
	{
		columns = atoi(argv[2]);
		rows = atoi(argv[3]);
	}

	printf("Creating file %s with %" PRIu64 " columns and %" PRIu64 " rows\n", filename, columns, rows);

	FILE *fp = fopen(filename, "wb");
	if(fwrite(&rows, sizeof(uint64_t), 1, fp) < 0)
	{
		perror("fwrite error");
		return -2;
	}
	
	if(fwrite(&columns, sizeof(uint64_t), 1, fp) < 0)
	{
		perror("fwrite error");
		return -2;
	}

	for(uint64_t i=0; i<columns; i++)
	{
		for(uint64_t j=0; j<rows; j++)
		{
			uint64_t k = i + j;
			if(fwrite(&k, sizeof(uint64_t), 1, fp) < 0)
			{
				perror("fwrite error");
				return -2;
			}
		}
	}

	fclose(fp);
}