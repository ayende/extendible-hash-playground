#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdlib.h>

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


static inline uint32_t _hash_table_bucket_number(hash_ctx_t* ctx, uint64_t h) {
	return h & (((uint64_t)1 << ctx->dir->depth) - 1);
}

static inline size_t _hash_table_get_directory_capacity(hash_ctx_t* ctx) {
	return (((size_t)ctx->dir->directory_pages * HASH_BUCKET_PAGE_SIZE) - sizeof(hash_directory_t)) / sizeof(hash_bucket_t*);
}

static hash_bucket_t* _create_hash_bucket(hash_ctx_t* ctx) {
	hash_bucket_t* b = ctx->allocate_page(1);
	if (b == NULL)
		return NULL;

	memset(b, 0, sizeof(hash_bucket_t));
	b->depth = ctx->dir->depth;
	return b;
}

bool hash_table_get(hash_ctx_t* ctx, uint64_t key, uint64_t* value) {
	uint32_t bucket_idx = _hash_table_bucket_number(ctx, key);
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

static bool _hash_table_piece_append_kv(hash_bucket_t* cur, uint32_t piece_idx, uint8_t* buffer, uint8_t size) {

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



static void _validate_bucket(hash_ctx_t* ctx, hash_bucket_t* tmp) {
#if VALIDATE
	uint64_t mask = ((uint64_t)1 << tmp->depth) - 1;
	uint64_t first = 0;
	bool has_first = false;

	for (size_t i = 0; i < NUMBER_OF_HASH_BUCKET_PIECES; i++)
	{
		uint8_t* buf = tmp->pieces[i].data;
		uint8_t* end = buf + tmp->pieces[i].bytes_used;
		while (buf < end)
		{
			uint64_t k, v;
			varint_decode(&buf, &k);
			varint_decode(&buf, &v);

			if (has_first == false)
			{
				first = k;
				has_first = true;
			}

			if ((k & mask) != (first & mask)) {
				write_dir_graphviz(ctx, "problem");
				break;
			}
		}
	}
#endif
}


static bool _hash_table_put_increase_size(hash_ctx_t* ctx, hash_bucket_t* b, uint64_t key, uint64_t value, uint8_t* buffer, uint8_t encoded_size) {

	if (ctx->dir->depth == b->depth) {
		hash_directory_t* new_dir;
		if ((size_t)ctx->dir->number_of_buckets * 2 * sizeof(hash_bucket_t*) > _hash_table_get_directory_capacity(ctx)) {

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
	hash_bucket_t* n = _create_hash_bucket(ctx);
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
			bool success = _hash_table_piece_append_kv(cur, k % NUMBER_OF_HASH_BUCKET_PIECES, start, (uint8_t)(buf - start));
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
		if (_hash_table_piece_append_kv(cur, key % NUMBER_OF_HASH_BUCKET_PIECES, buffer, encoded_size)) {
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

	_validate_bucket(ctx, n);
	_validate_bucket(ctx, b);
	return true;
}

static bool _hash_table_overflow_merge(hash_ctx_t* ctx, hash_bucket_t* b, uint32_t piece_idx) {
	size_t max_overflow = 0;
	for (size_t j = 0; j < NUMBER_OF_HASH_BUCKET_PIECES; j++) {
		if (!b->pieces[(piece_idx + j) % NUMBER_OF_HASH_BUCKET_PIECES].overflowed) {
			break;
		}
		max_overflow++;
	}

	bool has_overflow = false;
	while (max_overflow)
	{
		uint32_t cur_piece_idx = (piece_idx + max_overflow) % NUMBER_OF_HASH_BUCKET_PIECES;
		hash_bucket_piece_t* cur = &b->pieces[cur_piece_idx];
		has_overflow |= cur->overflowed; // if the current one is overflowed, we can't mark the previous as not overflowed

		uint64_t k = 0, v = 0;
		uint8_t* buf = cur->data;
		uint8_t* end = buf + cur->bytes_used;
		while (buf < end) {
			uint8_t* cur_buf_start = buf;
			varint_decode(&buf, &k);
			varint_decode(&buf, &v);
			uint32_t key_piece_idx = k % NUMBER_OF_HASH_BUCKET_PIECES;
			if (key_piece_idx != cur_piece_idx) {
				// great, found something that we can move backward
				ptrdiff_t diff = buf - cur_buf_start;
				hash_bucket_piece_t* key_p = &b->pieces[key_piece_idx];
				if (diff + key_p->bytes_used <= PIECE_BUCKET_BUFFER_SIZE) {
					memmove(key_p->data + key_p->bytes_used, cur_buf_start, diff);
					memmove(cur_buf_start, buf, end - buf);
					cur->bytes_used -= (uint8_t)diff;
					key_p->bytes_used += (uint8_t)diff;
					end -= diff;
					buf = cur_buf_start;
				}
				else {
					// we can't move this overflow
					has_overflow = true;
				}
			}
		}
		if (!has_overflow) {
			uint32_t prev_idx = cur_piece_idx ? cur_piece_idx - 1 : NUMBER_OF_HASH_BUCKET_PIECES;
			hash_bucket_piece_t* prev = &b->pieces[prev_idx];
			prev->overflowed = false;
		}
		max_overflow--;
	}
	// if we are overflow *or* have some data, don't try to compact the page with its sibling
	return b->pieces[piece_idx].overflowed || b->pieces[piece_idx].bytes_used > 0;
}

static size_t _get_bucket_size(hash_bucket_t* b) {
	size_t total = NUMBER_OF_HASH_BUCKET_PIECES * (sizeof(hash_bucket_piece_t) - PIECE_BUCKET_BUFFER_SIZE);
	for (size_t j = 0; j < NUMBER_OF_HASH_BUCKET_PIECES; j++) {
		total += b->pieces[j].bytes_used;
	}
	return total;
}

static bool _hash_bucket_copy(hash_bucket_t* dst, hash_bucket_t* src) {
	for (size_t i = 0; i < NUMBER_OF_HASH_BUCKET_PIECES; i++)
	{
		uint8_t* buf = src->pieces[i].data;
		uint8_t* end = buf + src->pieces[i].bytes_used;
		while (buf < end)
		{
			uint64_t k, v;
			uint8_t* start = buf;
			varint_decode(&buf, &k);
			varint_decode(&buf, &v);
			if (!_hash_table_piece_append_kv(dst, k % NUMBER_OF_HASH_BUCKET_PIECES, start, (uint8_t)(buf - start))) {
				return false;
			}
		}
	}
	return true;
}

static void _hash_table_compact_pages(hash_ctx_t* ctx, uint64_t key, uint32_t bucket_idx) {
	if (ctx->dir->number_of_buckets <= 2)
		return; // can't compact if we have just 2 pages
	hash_bucket_t* left = ctx->dir->buckets[bucket_idx];
	uint32_t sibling_idx = bucket_idx ^ ((uint64_t)1 << (left->depth - 1));
	hash_bucket_t* right = ctx->dir->buckets[sibling_idx];
	if (_get_bucket_size(right) + _get_bucket_size(left) > HASH_BUCKET_PAGE_SIZE_MERGE_LIMIT)
		return; // too big for compaction, we'll try again later

	hash_bucket_t* merged = _create_hash_bucket(ctx);
	// we couldn't merge, out of mem, but that is fine, we don't *have* to
	if (!merged)
		return;

	merged->depth = left->depth < right->depth ? left->depth : right->depth;
	merged->depth--;
	if (!_hash_bucket_copy(merged, left) || !_hash_bucket_copy(merged, right)) {
		// failed to copy, sad, but we'll try again later
		ctx->release_page(merged);
		return;
	}
	_validate_bucket(ctx, merged);

	size_t bit = (uint64_t)1 << merged->depth;
	for (size_t i = key & (bit - 1); i < ctx->dir->number_of_buckets; i += bit)
	{
		ctx->dir->buckets[i] = merged;
	}
	ctx->release_page(right);
	ctx->release_page(left);

	size_t max_depth = 0;
	for (size_t i = 0; i < ctx->dir->number_of_buckets; i++)
	{
		if (ctx->dir->depth > max_depth)
			max_depth = ctx->dir->depth;
	}
	if (max_depth == ctx->dir->depth)
		return;

	// we can decrease the size of the directory now
	ctx->dir->depth--;
	ctx->dir->number_of_buckets /= 2;

	if (ctx->dir->number_of_buckets == 1 || 
		(size_t)ctx->dir->number_of_buckets * 2 * sizeof(hash_bucket_t*) >= _hash_table_get_directory_capacity(ctx))
		return; // we are using more than half the space, nothing to touch here

	hash_directory_t* new_dir = ctx->allocate_page(ctx->dir->directory_pages / 2);
	if (new_dir != NULL) { // if we can't allocate, just ignore this, it is fine
		size_t dir_size = (size_t)(ctx->dir->directory_pages / 2) * HASH_BUCKET_PAGE_SIZE;
		memcpy(new_dir, ctx->dir, dir_size);
		new_dir->directory_pages /= 2;
		ctx->release_page(ctx->dir);
		ctx->dir = new_dir;
	}
}

bool hash_table_delete(hash_ctx_t* ctx, uint64_t key, hash_old_value_t* old_value) {
	uint32_t bucket_idx = _hash_table_bucket_number(ctx, key);
	hash_bucket_t* b = ctx->dir->buckets[bucket_idx];
	uint32_t piece_idx = key % NUMBER_OF_HASH_BUCKET_PIECES;

	ctx->dir->version++;

	if (old_value)
		old_value->exists = false;

	for (size_t i = 0; i < MAX_CHAIN_LENGTH; i++)
	{
		uint32_t cur_piece_idx = (piece_idx + i) % NUMBER_OF_HASH_BUCKET_PIECES;
		hash_bucket_piece_t* p = &b->pieces[cur_piece_idx];
		uint8_t* buf = p->data;
		uint8_t* end = p->data + p->bytes_used;
		while (buf < end)
		{
			uint64_t k, v;
			uint8_t* cur_buf_start = buf;
			varint_decode(&buf, &k);
			varint_decode(&buf, &v);

			if (k == key) {

				if (old_value) {
					old_value->exists = true;
					old_value->value = v;
				}

				ptrdiff_t diff = buf - cur_buf_start;
				memmove(cur_buf_start, buf, end - buf);
				p->bytes_used -= (uint8_t)diff;
				b->number_of_entries--;
				ctx->dir->number_of_entries--;

				if (p->bytes_used == 0) {
					if (!_hash_table_overflow_merge(ctx, b, cur_piece_idx)) {
						_hash_table_compact_pages(ctx, key, bucket_idx);
					}
				}

				return true;
			}
		}

		// if we are looking at an overflow page, move to the next one and try to find it there
		if (!p->overflowed)
			break;
	}

	return false;
}

bool hash_table_put(hash_ctx_t* ctx, uint64_t key, uint64_t value) {
	return hash_table_replace(ctx, key, value, NULL);
}

bool hash_table_replace(hash_ctx_t* ctx, uint64_t key, uint64_t value, hash_old_value_t* old_value) {
	uint32_t bucket_idx = _hash_table_bucket_number(ctx, key);
	hash_bucket_t* b = ctx->dir->buckets[bucket_idx];
	uint32_t piece_idx = key % NUMBER_OF_HASH_BUCKET_PIECES;

	ctx->dir->version++;

	uint8_t tmp_buffer[20]; // each varint can take up to 10 bytes
	uint8_t* buf_end = tmp_buffer;
	varint_encode(key, &buf_end);
	uint8_t* key_end = buf_end;
	varint_encode(value, &buf_end);
	ptrdiff_t encoded_size = buf_end - tmp_buffer;

	if (old_value)
		old_value->exists = false;

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

				if (old_value) {
					old_value->exists = true;
					old_value->value = v;
				}

				if (v == value)
					return true; // nothing to do, value is already there
				ptrdiff_t diff = buf - cur_buf_start;
				if (diff == encoded_size) {
					// new value fit exactly where the old one went, let's put it there
					memcpy(cur_buf_start, tmp_buffer, encoded_size);
					_validate_bucket(ctx, b);
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

			_validate_bucket(ctx, b);
			return true; // was able to update the value in the same piece, done
		}

		// if we are looking at an overflow page, move to the next one and try to find it there
		if (!p->overflowed)
			break;
	}
	// couldn't find it in the proper place, let's put it in overflow
	for (size_t i = 0; i < MAX_CHAIN_LENGTH; i++) {
		hash_bucket_piece_t* p = &b->pieces[(piece_idx + i) % NUMBER_OF_HASH_BUCKET_PIECES];
		if (p->bytes_used + encoded_size <= PIECE_BUCKET_BUFFER_SIZE) {
			memcpy(p->data + p->bytes_used, tmp_buffer, encoded_size);
			p->bytes_used += (uint8_t)encoded_size;
			b->number_of_entries++;
			ctx->dir->number_of_entries++;

			_validate_bucket(ctx, b);
			return true; // was able to update the value in the same piece, done
		}
		p->overflowed = true;
	}

	// there is no room here, need to expand
	return _hash_table_put_increase_size(ctx, b, key, value, tmp_buffer, (uint8_t)encoded_size);
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

	ctx->dir->buckets[0] = _create_hash_bucket(ctx);
	ctx->dir->buckets[1] = _create_hash_bucket(ctx);

	if (!ctx->dir->buckets[0] || !ctx->dir->buckets[1]) {
		ctx->release_page(ctx->dir->buckets[0]);
		ctx->release_page(ctx->dir->buckets[1]);
		ctx->release_page(ctx->dir);
		return false;
	}

	return true;
}

void hash_table_iterate_init(hash_ctx_t* ctx, hash_iteration_state_t* state) {
	memset(state, 0, sizeof(hash_iteration_state_t));
	state->dir = ctx->dir;
	state->version = ctx->dir->version;
	// need to mark the buckets as unseen, so we'll not traverse the same bucket twice
	// because it shows up multiple times in the directory
	for (size_t i = 0; i < ctx->dir->number_of_buckets; i++)
	{
		ctx->dir->buckets[i]->seen = false;
	}
}

bool hash_table_iterate_next(hash_iteration_state_t* state, uint64_t* key, uint64_t* value) {
	while (true) {

		if (state->version != state->dir->version) {
			errno = EINVAL;
			return false;
		}

		if (state->current_bucket_idx >= state->dir->number_of_buckets)
			return false;

		if (state->current_piece_idx >= NUMBER_OF_HASH_BUCKET_PIECES) {
			state->current_piece_idx = 0;
			state->current_bucket_idx++;
			if (state->dir->buckets[state->current_bucket_idx]->seen) {
				// we'll now skip the already seen bucket
				state->current_piece_idx = NUMBER_OF_HASH_BUCKET_PIECES;
			}
			state->dir->buckets[state->current_bucket_idx]->seen = true;
			continue;
		}

		hash_bucket_t* b = state->dir->buckets[state->current_bucket_idx];
		hash_bucket_piece_t* p = &b->pieces[state->current_piece_idx];
		if (state->current_piece_byte_pos >= p->bytes_used) {
			state->current_piece_byte_pos = 0;
			state->current_piece_idx++;
			continue;
		}

		uint8_t* buf = p->data + state->current_piece_byte_pos;
		varint_decode(&buf, key);
		varint_decode(&buf, value);

		state->current_piece_byte_pos = (uint8_t)(buf - p->data);

		return true;
	}
}

int compare_ptrs(const void* a, const void* b) {
	hash_bucket_t* x = *(hash_bucket_t**)a;
	hash_bucket_t* y = *(hash_bucket_t**)b;

	ptrdiff_t diff = x - y;
	if (diff)
		return diff > 0 ? 1 : -1;
	return 0;
}

void hash_table_free(hash_ctx_t* ctx) {
	qsort(ctx->dir->buckets, ctx->dir->number_of_buckets, sizeof(hash_bucket_t*), compare_ptrs);
	hash_bucket_t* prev = NULL;
	for (size_t i = 0; i < ctx->dir->number_of_buckets; i++)
	{
		if (prev == ctx->dir->buckets[i]) {
			continue;
		}
		prev = ctx->dir->buckets[i];
		ctx->release_page(ctx->dir->buckets[i]);
	}
	ctx->release_page(ctx->dir);
	ctx->dir = NULL;
}