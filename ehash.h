#pragma once

#include <stdint.h>

#define VALIDATE 0

#define HASH_BUCKET_PAGE_SIZE			   8192
#define NUMBER_OF_HASH_BUCKET_PIECES		127
#define PIECE_BUCKET_BUFFER_SIZE			 63
#define MAX_CHAIN_LENGTH					 128

typedef struct hash_bucket_piece {
	uint8_t overflowed : 1;
	uint8_t bytes_used : 7;
	uint8_t data[PIECE_BUCKET_BUFFER_SIZE];
} hash_bucket_piece_t;

static_assert(sizeof(hash_bucket_piece_t) == 64, "hash_bucket_piece_t is expected to be 64 bytes exactly");

typedef struct hash_bucket {
	union {
		struct {
			uint64_t number_of_entries;
			uint8_t depth;
			bool seen;
		};
		uint8_t _padding[64];
	};
	hash_bucket_piece_t pieces[NUMBER_OF_HASH_BUCKET_PIECES];
} hash_bucket_t;

typedef struct hash_directory {
	uint64_t number_of_entries;
	uint32_t number_of_buckets;
	uint32_t directory_pages;
	uint8_t depth;
	hash_bucket_t* buckets[0];
} hash_directory_t;

typedef struct hash_ctx {
	void* (*allocate_page)(uint32_t n);
	void (*release_page)(void* p);
	hash_directory_t* dir;
} hash_ctx_t;

// --- debug ---

void validate_bucket(hash_ctx_t* ctx, hash_bucket_t* tmp);

void write_dir_graphviz(hash_ctx_t* ctx, const char* prefix);

void print_dir_graphviz(hash_ctx_t* ctx);

void print_hash_stats(hash_ctx_t* ctx);


// --- API ---

bool hash_table_get(hash_ctx_t* ctx, uint64_t key, uint64_t* value);

bool hash_table_put(hash_ctx_t* ctx, uint64_t key, uint64_t value);

bool hash_table_init(hash_ctx_t* ctx);

// --- utils --- 
void varint_decode(uint8_t** buf, uint64_t* val);
