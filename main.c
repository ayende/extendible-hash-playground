#include <stdlib.h>
#include <stdio.h>
#include <malloc.h>
#include <inttypes.h>
#include <stdbool.h>
#include <time.h>

#include "wyhash.h" //https://raw.githubusercontent.com/wangyi-fudan/wymlp/master/wyhash.h
#include "ehash.h"


int allocations = 0;

void* allocate_4k_page(uint32_t n) {
	allocations++;
	return _aligned_malloc(HASH_BUCKET_PAGE_SIZE * n, HASH_BUCKET_PAGE_SIZE);
}

void release_4k_page(void* p) {	 
	if (!p)
		return;
	allocations--;
	_aligned_free(p);
}

int main()
{
	
	uint32_t const size = 686;

	uint64_t* keys = malloc(size * sizeof(uint64_t));
	uint64_t* values = malloc(size * sizeof(uint64_t));
	if (!keys || !values)
		return -1;

	for (size_t i = 0; i < size; i++)
	{
		keys[i] = wygrand();
		values[i] = wygrand();
	}

	clock_t start, end;
	double cpu_time_used;

	start = clock();

	struct hash_ctx ctx = { allocate_4k_page, release_4k_page };
	if (!hash_table_init(&ctx)) {
		printf("Failed to init\n");
		return -1;
	}

	uint64_t first_key = keys[0];
	uint64_t first_value = values[0];


	for (size_t i = 0; i < size; i++)
	{
		if (!hash_table_put(&ctx, keys[i], values[i])) {
			printf("Failed to put %llu\n", i);
			write_dir_graphviz(&ctx, "PUT-ERR");
			return -1;
		}

		if (keys[i] == first_key)
			first_value = values[i];
#if VALIDATE
		uint64_t v;
		if (!hash_table_get(&ctx, first_key, &v) || v != first_value) {
			printf("Failed to get 0 on %llu\n", i);
			write_dir_graphviz(&ctx, "PUT-ERR");
		}
#endif
	}

	for (size_t i = 0; i < 100; i++)
	{
		if (!(keys[i] & 1)) {
			hash_old_value_t old;
			if (hash_table_delete(&ctx, keys[i], &old)) {
				printf("Removed: %llu\n", old.value);
			}
		}

	}

	write_dir_graphviz(&ctx, "DEL");
	return;
}


	/*for (size_t i = 0; i < size; i+=3)
	{
		hash_old_value_t old;
		if (hash_table_delete(&ctx, keys[i], &old)) {
			printf("Removed: %llu\n", old.value);
		}

	}
	end = clock();
	cpu_time_used = ((double)(end - (double)start)) / CLOCKS_PER_SEC;

	uint64_t k, v;
	hash_iteration_state_t state;
	hash_table_iterate_init(&ctx, &state);
	while (hash_table_iterate_next(&state, &k, &v)) {
		printf("%llu = %llu\n", k, v);
	}

	printf("%llu", ctx.dir->number_of_entries);
	return;
	//printf("<tr><td>%u</td><td>%f</td><td>%f</td><td>%f</td></tr>\n", MAX_CHAIN_LENGTH, cpu_time_used, (cpu_time_used * 1000 * 1000) / size, ((double)allocations * (double)HASH_BUCKET_PAGE_SIZE) / 1024.0 / 1024.0);

	//write_dir_graphviz(&ctx, "FINAL");
	for (size_t x = 0; x < size; x++)
	{
		uint64_t val;

		if (!hash_table_get(&ctx, keys[x], &val)) {
			printf("Failed to get %llu\n", x);
			write_dir_graphviz(&ctx, "GET-ERR");
			return -1;
		}
		if (val != values[x]) {
		//	printf("mismatch on %I32u - %I64u - %I64u, %I64u \n", x, keys[x], val, values[x]);
		}
	}
	print_hash_stats(&ctx);


	return 0;*/