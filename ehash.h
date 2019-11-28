#pragma once

#include <stdint.h>

#define VALIDATE 1

#define HASH_BUCKET_PAGE_SIZE			   8192
#define HASH_BUCKET_PAGE_SIZE_MERGE_LIMIT  6144
#define NUMBER_OF_HASH_BUCKET_PIECES		127
#define PIECE_BUCKET_BUFFER_SIZE			 63
#define MAX_CHAIN_LENGTH					 16

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
	uint32_t version;
	uint8_t depth;
	hash_bucket_t* buckets[0];
} hash_directory_t;

typedef struct hash_ctx {
	void* (*allocate_page)(uint32_t n);
	void (*release_page)(void* p);
	hash_directory_t* dir;
} hash_ctx_t;

typedef struct hash_old_value {
	uint64_t value;
	bool exists;
} hash_old_value_t;

typedef struct hash_iteration_state {
	hash_directory_t* dir;
	uint32_t version;
	uint32_t current_bucket_idx;
	uint8_t current_piece_idx;
	uint8_t current_piece_byte_pos;
} hash_iteration_state_t;

// --- debug ---

void write_dir_graphviz(hash_ctx_t* ctx, const char* prefix);

void print_dir_graphviz(hash_ctx_t* ctx);

void print_hash_stats(hash_ctx_t* ctx);


// --- API ---

bool hash_table_get(hash_ctx_t* ctx, uint64_t key, uint64_t* value);

bool hash_table_put(hash_ctx_t* ctx, uint64_t key, uint64_t value);

bool hash_table_replace(hash_ctx_t* ctx, uint64_t key, uint64_t value, hash_old_value_t* old_value);

void hash_table_iterate_init(hash_ctx_t* ctx, hash_iteration_state_t* state);

bool hash_table_iterate_next(hash_iteration_state_t* state, uint64_t* key, uint64_t* value);

bool hash_table_delete(hash_ctx_t* ctx, uint64_t key, hash_old_value_t* old_value);

bool hash_table_init(hash_ctx_t* ctx);

// --- utils --- 
void varint_decode(uint8_t** buf, uint64_t* val);
