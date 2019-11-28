#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <inttypes.h>

#include "ehash.h"

void print_bits(FILE* fd, uint64_t v, int depth)
{
	uint8_t* b = (uint8_t*)&v;
	uint8_t byte;
	int i, j;
	int bits = sizeof(uint64_t) * 8;
	bool foundNonZero = false;
	for (i = sizeof(uint64_t) - 1; i >= 0; i--)
	{
		for (j = 7; j >= 0; j--)
		{
			bits--;
			byte = (b[i] >> j) & 1;
			if (byte) {
				foundNonZero = true;
			}
			if (!foundNonZero && bits > depth&& bits > 8)
				continue;
			fprintf(fd, "%u", byte);
			if (bits == depth)
				fprintf(fd, " ");
		}
	}
}

void print_hash_stats(hash_ctx_t* ctx) {

	printf("Depth: %i - Entries: %I64u, Buckets: %i \n", ctx->dir->depth, ctx->dir->number_of_entries, ctx->dir->number_of_buckets);
	for (size_t i = 0; i < ctx->dir->number_of_buckets; i++)
		ctx->dir->buckets[i]->seen = false;

	uint32_t min = 64, max = 0, total = 0, empties = 0;
	uint64_t sum = 0;

	uint32_t max_overflow_chain = 0;
	uint32_t total_chains = 0;
	uint32_t sum_overflow_chain = 0;

	for (size_t i = 0; i < ctx->dir->number_of_buckets; i++)
	{
		hash_bucket_t* b = ctx->dir->buckets[i];
		if (b->seen)
			continue;
		b->seen = true;
		for (size_t i = 0; i < NUMBER_OF_HASH_BUCKET_PIECES; i++) {
			if (!b->pieces[i].overflowed) {
				continue;
			}
			uint32_t cur = 1;
			for (size_t j = 1; j < NUMBER_OF_HASH_BUCKET_PIECES - 1; j++) {
				if (!b->pieces[(i + j) % NUMBER_OF_HASH_BUCKET_PIECES].overflowed)
					break;
				cur++;
			}
			if (cur > max_overflow_chain)
				max_overflow_chain = cur;
			if (cur >= 20)
				total_chains++;
			sum_overflow_chain += cur;
		}
		//printf("%p - Depth: %i, Entries: %I64u, Size: %i\n", b, b->depth, b->number_of_entries, total_used);
	}


	for (size_t i = 0; i < ctx->dir->number_of_buckets; i++)
		ctx->dir->buckets[i]->seen = false;

	for (size_t i = 0; i < ctx->dir->number_of_buckets; i++)
	{
		hash_bucket_t* b = ctx->dir->buckets[i];
		if (b->seen)
			continue;
		b->seen = true;
		size_t total_used = 0;
		for (size_t i = 0; i < NUMBER_OF_HASH_BUCKET_PIECES; i++) {
			total_used += b->pieces[i].bytes_used;
			if (b->pieces[i].bytes_used)
				min = min < b->pieces[i].bytes_used ? min : b->pieces[i].bytes_used;
			else
				empties++;
			max = max > b->pieces[i].bytes_used ? max : b->pieces[i].bytes_used;
			sum += b->pieces[i].bytes_used;
			total++;
		}
		//printf("%p - Depth: %i, Entries: %I64u, Size: %i\n", b, b->depth, b->number_of_entries, total_used);
	}
	printf("Total: %i, Min: %i, Max: %iu, Sum: %llu, Empties: %i, Max Chain: %i, Sum chain: %i, Total Chains: %i, Avg: %f\n", total, min, max, sum, empties, max_overflow_chain, sum_overflow_chain, total_chains, sum / (float)total);
}

void print_bucket(FILE* fd, hash_bucket_t* b, uint8_t idx) {
	size_t total_used = 0;
	for (size_t i = 0; i < NUMBER_OF_HASH_BUCKET_PIECES; i++) {
		total_used += b->pieces[i].bytes_used;
	}
	fprintf(fd, "\tbucket_%p [label=\"Depth: %u, Entries: %llu, Size: %llu, Index: %u\\l--------\\l",
		b, b->depth, b->number_of_entries, total_used, idx);
	for (size_t i = 0; i < NUMBER_OF_HASH_BUCKET_PIECES; i++) {
		hash_bucket_piece_t* p = &b->pieces[i];
		uint8_t* buf = p->data;
		uint8_t* end = p->data + p->bytes_used;
		while (buf < end)
		{
			uint64_t k = 0, v = 0;
			varint_decode(&buf, &k);
			varint_decode(&buf, &v);

			print_bits(fd, k, b->depth);
			fprintf(fd, " \\| %4llu = %4llu\\l", k, v);
		}
	}
	fprintf(fd, "\"]\n");
}

void print_dir_graphviz_to_file(FILE* fd, hash_ctx_t* ctx) {
	fprintf(fd, "digraph hash {\n\tnode[shape = record ]; \n");
	fprintf(fd, "\ttable [label=\"Depth: %i, Size: %i\\lPages: %i, Entries: %I64u\\l\"]\n", ctx->dir->depth, ctx->dir->number_of_buckets, ctx->dir->directory_pages, ctx->dir->number_of_entries);
	fprintf(fd, "\tbuckets [label=\"");
	for (size_t i = 0; i < ctx->dir->number_of_buckets; i++)
	{
		if (i != 0)
			fprintf(fd, "|");
		fprintf(fd, "<bucket_%Iu> %Iu - %p ", i, i, &ctx->dir->buckets[i]);
		ctx->dir->buckets[i]->seen = false;
	}
	fprintf(fd, "\"]\n");
	for (uint32_t i = 0; i < ctx->dir->number_of_buckets; i++)
	{
		if (ctx->dir->buckets[i]->seen)
			continue;
		ctx->dir->buckets[i]->seen = true;
		print_bucket(fd, ctx->dir->buckets[i], i);
	}

	for (size_t i = 0; i < ctx->dir->number_of_buckets; i++) {
		fprintf(fd, "\tbuckets:bucket_%Iu -> bucket_%p;\n", i, ctx->dir->buckets[i]);
	}
	fprintf(fd, "\ttable->buckets;\n}\n");
}

void print_dir_graphviz(hash_ctx_t* ctx) {
	print_dir_graphviz_to_file(stdout, ctx);
}

void write_dir_graphviz(hash_ctx_t* ctx, const char* prefix) {
	static int counter = 0;
	uint8_t buffer[256];
	memset(buffer, 0, sizeof buffer);
	counter++;
	sprintf_s(buffer, sizeof buffer, "%s-%i.txt", prefix, counter);
	FILE* f = NULL;
	errno_t err = fopen_s(&f, buffer, "w");
	if (err)
	{
		printf("Failed to open %s - %i", buffer, err);
		return;
	}
	print_dir_graphviz_to_file(f, ctx);
	if (f != 0)
		fclose(f);

	char* copy = _strdup(buffer);
	memset(buffer, 0, sizeof buffer);
	sprintf_s(buffer, sizeof buffer, "C:\\Users\\ayende\\Downloads\\graphviz-2.38\\release\\bin\\dot.exe  -Tsvg %s > %s.svg", copy, copy);
	system(buffer);
	memset(buffer, 0, sizeof buffer);
	sprintf_s(buffer, sizeof buffer, "%s.svg", copy);
	system(buffer);
	free(copy);
}
