#include <string.h>
#include <inttypes.h>
#include <stdbool.h>

#include "ehash.h"


void varint_decode(uint8_t** buf, uint64_t* val) {
	uint64_t result = 0;
	uint32_t shift = 0;
	uint8_t* ptr = *buf;
	uint8_t cur = 0;
	do
	{
		cur = *ptr++;
		result |= (uint64_t)(cur & 0x7f) << shift;
		shift += 7;
	} while (cur & 0x80);
	*val = result;
	*buf = ptr;
}

void varint_encode(uint64_t val, uint8_t** buf) {
	uint8_t* ptr = *buf;
	while (val >= 0x80) {
		*ptr++ = ((uint8_t)val | 0x80);
		val >>= 7;
	}
	*ptr++ = (uint8_t)(val);
	*buf = ptr;
}


static inline uint32_t hash_table_bucket_number(hash_ctx_t* ctx, uint64_t h) {
	return h & (((uint64_t)1 << ctx->dir->depth) - 1);
}

static inline size_t hash_table_get_directory_capacity(hash_ctx_t* ctx) {
	return (((size_t)ctx->dir->directory_pages * HASH_BUCKET_PAGE_SIZE) - sizeof(hash_directory_t)) / sizeof(hash_bucket_t*);
}

static hash_bucket_t* create_hash_bucket(hash_ctx_t* ctx) {
	hash_bucket_t* b = ctx->allocate_page(1);
	if (b == NULL)
		return NULL;

	memset(b, 0, sizeof(hash_bucket_t));
	b->depth = ctx->dir->depth;
	return b;
}

bool hash_table_get(hash_ctx_t* ctx, uint64_t key, uint64_t* value) {
	uint32_t bucket_idx = hash_table_bucket_number(ctx, key);
	hash_bucket_t* b = ctx->dir->buckets[bucket_idx];
	uint32_t piece_idx = key % NUMBER_OF_HASH_BUCKET_PIECES;

	for (size_t i = 0; i < NUMBER_OF_HASH_BUCKET_PIECES; i++)
	{
		hash_bucket_piece_t* p = &b->pieces[(piece_idx + i) % NUMBER_OF_HASH_BUCKET_PIECES];
		uint8_t* buf = p->data;
		uint8_t* end = p->data + p->bytes_used;
		while (buf < end)
		{
			uint64_t k, v;
			varint_decode(&buf, &k);
			varint_decode(&buf, &v);
			if (k == key) {
				*value = v;
				return true;
			}
		}
		if (!p->overflowed)
			break;
	}

	return false;
}

bool hash_table_piece_append_kv(hash_bucket_t* cur, uint32_t piece_idx, uint8_t* buffer, uint8_t size) {

	for (size_t i = 0; i < MAX_CHAIN_LENGTH; i++)
	{
		hash_bucket_piece_t* p = &cur->pieces[(piece_idx + i) % NUMBER_OF_HASH_BUCKET_PIECES];
		if (size + p->bytes_used > PIECE_BUCKET_BUFFER_SIZE) {
			p->overflowed = true;
			continue;
		}
		memcpy(p->data + p->bytes_used, buffer, size);
		p->bytes_used += size;
		cur->number_of_entries++;
		return true;
	}
	return false;
}

bool hash_table_put_increase_size(hash_ctx_t* ctx, hash_bucket_t* b, uint64_t key, uint64_t value, uint8_t* buffer, uint8_t encoded_size) {

	if (ctx->dir->depth == b->depth) {
		hash_directory_t* new_dir;
		if ((size_t)ctx->dir->number_of_buckets * 2 * sizeof(hash_bucket_t*) > hash_table_get_directory_capacity(ctx)) {

			// have to increase the actual allocated memory here
			new_dir = ctx->allocate_page(ctx->dir->directory_pages * 2);
			if (new_dir == NULL)
				return false;

			size_t dir_size = (size_t)ctx->dir->directory_pages * HASH_BUCKET_PAGE_SIZE;
			memcpy(new_dir, ctx->dir, dir_size);
			new_dir->directory_pages *= 2;
		}
		else {
			new_dir = ctx->dir; // there is enough space to increase size without allocations
		}
		size_t buckets_size = ctx->dir->number_of_buckets * sizeof(hash_bucket_t*);
		memcpy((uint8_t*)new_dir->buckets + buckets_size, (uint8_t*)ctx->dir->buckets, buckets_size);
		new_dir->depth++;
		new_dir->number_of_buckets *= 2;
		if (new_dir != ctx->dir) {
			ctx->release_page(ctx->dir);
			ctx->dir = new_dir;
		}
	}
	//write_dir_graphviz(ctx, "BEFORE");
	hash_bucket_t* n = create_hash_bucket(ctx);
	if (!n)
		return false;

	hash_bucket_t* tmp = ctx->allocate_page(1);
	if (!tmp) {
		ctx->release_page(n);
		ctx->release_page(tmp);
		// no need to release the ctx->dir we allocated, was wired
		// properly to the table and will be freed with the whole table
		return false;
	}
	memcpy(tmp, b, HASH_BUCKET_PAGE_SIZE);
	memset(b, 0, sizeof(hash_bucket_t));
	memset(n, 0, sizeof(hash_bucket_t));
	n->depth = b->depth = tmp->depth + 1;

	uint32_t bit = 1 << tmp->depth;

#if VALIDATE
	uint64_t mask = (bit >> 1) - 1;
	bool has_first = false;
	uint64_t first_key = 0;
#endif

	for (size_t i = 0; i < NUMBER_OF_HASH_BUCKET_PIECES; i++)
	{
		uint8_t* buf = tmp->pieces[i].data;
		uint8_t* end = buf + tmp->pieces[i].bytes_used;
		while (buf < end)
		{
			uint64_t k, v;
			uint8_t* start = buf;
			varint_decode(&buf, &k);
			varint_decode(&buf, &v);
#if VALIDATE
			if (!has_first) {
				first_key = k;
				has_first = true;
			}

			if ((first_key & mask) != (k & mask)) {
				printf("mistmatch!: %I64u != %I64u\n", first_key, k);
			}
#endif

			hash_bucket_t* cur = k & bit ? n : b;
			bool success = hash_table_piece_append_kv(cur, k % NUMBER_OF_HASH_BUCKET_PIECES, start, (uint8_t)(buf - start));
#if VALIDATE
			if (!success)
				printf("Can't split a page properly? Impossible");
#endif
		}
	}
	ctx->release_page(tmp);

	for (size_t i = key & (bit - 1); i < ctx->dir->number_of_buckets; i += bit)
	{
		ctx->dir->buckets[i] = i & bit ? n : b;
	}

	// now can add the new value in...
	{
		hash_bucket_t* cur = key & bit ? n : b;
		if (hash_table_piece_append_kv(cur, key % NUMBER_OF_HASH_BUCKET_PIECES, buffer, encoded_size)) {
			ctx->dir->number_of_entries++;
		}
		else {
			if (!hash_table_put(ctx, key, value)) {
#if VALIDATE
				printf("Unable to recursively add? That really should never happen.");
#endif
				return false;
			}
		}
	}

	validate_bucket(ctx, n);
	validate_bucket(ctx, b);
	return true;
}

bool hash_table_put(hash_ctx_t* ctx, uint64_t key, uint64_t value) {
	uint32_t bucket_idx = hash_table_bucket_number(ctx, key);
	hash_bucket_t* b = ctx->dir->buckets[bucket_idx];
	uint32_t piece_idx = key % NUMBER_OF_HASH_BUCKET_PIECES;

	uint8_t tmp_buffer[20]; // each varint can take up to 10 bytes
	uint8_t* buf_end = tmp_buffer;
	varint_encode(key, &buf_end);
	uint8_t* key_end = buf_end;
	varint_encode(value, &buf_end);
	ptrdiff_t encoded_size = buf_end - tmp_buffer;

	for (size_t i = 0; i < MAX_CHAIN_LENGTH; i++)
	{
		hash_bucket_piece_t* p = &b->pieces[(piece_idx + i) % NUMBER_OF_HASH_BUCKET_PIECES];
		uint8_t* buf = p->data;
		uint8_t* end = p->data + p->bytes_used;
		while (buf < end)
		{
			uint64_t k, v;
			uint8_t* cur_buf_start = buf;
			varint_decode(&buf, &k);
			varint_decode(&buf, &v);

			if (k == key) {
				if (v == value)
					return true; // nothing to do, value is already there
				ptrdiff_t diff = buf - cur_buf_start;
				if (diff == encoded_size) {
					// new value fit exactly where the old one went, let's put it there
					memcpy(cur_buf_start, tmp_buffer, encoded_size);
					validate_bucket(ctx, b);
					return true;
				}

				memmove(cur_buf_start, buf, end - buf);
				p->bytes_used -= (uint8_t)diff;
				b->number_of_entries--;
				ctx->dir->number_of_entries--;
				break; // found it, let's put this in 
			}
		}

		if (p->bytes_used + encoded_size <= PIECE_BUCKET_BUFFER_SIZE) {
			memcpy(p->data + p->bytes_used, tmp_buffer, encoded_size);
			p->bytes_used += (uint8_t)encoded_size;
			b->number_of_entries++;
			ctx->dir->number_of_entries++;

			validate_bucket(ctx, b);
			return true; // was able to update the value in the same piece, done
		}

		// if we are looking at an overflow page, move to the next one and try to find it there
		if (!p->overflowed)
			break;
	}
	// couldn't find it in the proper place, let's put it in overflow
	for (size_t i = 0; i < MAX_CHAIN_LENGTH; i++)
	{
		hash_bucket_piece_t* p = &b->pieces[(piece_idx + i) % NUMBER_OF_HASH_BUCKET_PIECES];
		if (p->bytes_used + encoded_size <= PIECE_BUCKET_BUFFER_SIZE) {
			memcpy(p->data + p->bytes_used, tmp_buffer, encoded_size);
			p->bytes_used += (uint8_t)encoded_size;
			b->number_of_entries++;
			ctx->dir->number_of_entries++;

			validate_bucket(ctx, b);
			return true; // was able to update the value in the same piece, done
		}
		p->overflowed = true;
	}

	// there is no room here, need to expand
	return hash_table_put_increase_size(ctx, b, key, value, tmp_buffer, (uint8_t)encoded_size);
}


bool hash_table_init(hash_ctx_t* ctx) {
	ctx->dir = ctx->allocate_page(1);
	if (ctx->dir == NULL)
		return false;

	memset(ctx->dir, 0, HASH_BUCKET_PAGE_SIZE);
	ctx->dir->number_of_entries = 0;
	ctx->dir->number_of_buckets = 2;
	ctx->dir->directory_pages = 1;
	ctx->dir->depth = 1;

	ctx->dir->buckets[0] = create_hash_bucket(ctx);
	ctx->dir->buckets[1] = create_hash_bucket(ctx);

	if (!ctx->dir->buckets[0] || !ctx->dir->buckets[1]) {
		ctx->release_page(ctx->dir->buckets[0]);
		ctx->release_page(ctx->dir->buckets[1]);
		ctx->release_page(ctx->dir);
		return false;
	}

	return true;
}