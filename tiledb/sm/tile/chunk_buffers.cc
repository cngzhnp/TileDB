/**
 * @file chunk_buffers.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file implements class ChunkBuffers.
 */

#include "tiledb/sm/tile/tile.h"

namespace tiledb {
namespace sm {

ChunkBuffers::ChunkBuffers()
	: chunk_size_(0)
	, last_chunk_size_(0) {
}

ChunkBuffers::ChunkBuffers(const ChunkBuffers& rhs) {
	deep_copy(rhs);
}

ChunkBuffers::ChunkBuffers& operator=(const ChunkBuffers &rhs) {
	deep_copy(rhs);
	return *this;
}

ChunkBuffers::~ChunkBuffers() {
}

void deep_copy(const ChunkBuffers &rhs) {
	buffers_.reserve(rhs.buffers_.size());
	for (int i = 0; i < rhs.buffers_.size(); ++i) {
		const uint32_t buffer_size =
			i == (rhs.buffers_.size() - 1) ?
				rhs.last_chunk_size_ :
				rhs.chunk_size_;
		void *const buffer_copy = malloc(buffer_size);
		memcpy(buffer_copy, rhs.buffers_[i], buffer_size);
		buffers_.emplace_back(buffer_copy);
	}

	chunk_size_ = rhs.chunk_size_;
	last_chunk_size_ = rhs.last_chunk_size_;
}

ChunkBuffers ChunkBuffers::shallow_copy() {
	ChunkBuffers copy;
	copy.buffers_ = buffers_;
	copy.chunk_size_ = chunk_size_;
	copy.last_chunk_size_ = last_chunk_size_;
	return copy;
}

void ChunkBuffers::swap(ChunkBuffers *const rhs) {
	std::swap(buffers_, rhs->buffers_);
	std::swap(chunk_size_, rhs->chunk_size_);
	std::swap(last_chunk_size_, rhs->last_chunk_size_);
}

void ChunkBuffers::free() {
	for (const auto& buffer : buffers_) {
		free(buffer);
	}

	buffers_.clear();
	chunk_size_ = 0;
	last_chunk_size_ = 0;
}

uint64_t ChunkBuffers::size() {
	if (buffers_.empty()) {
		return 0;
	}

	return
		chunk_size_ * (buffers_.size() - 1) +
		last_chunk_size_;
}

Status ChunkBuffers::init(
	const size_t nchunks,
	const uint32_t chunk_size,
  	const uint32_t last_chunk_size) {

	if (!buffers_.empty()) {
		return LOG_STATUS(
        	Status::TileError(
        		"Cannot get init chunk buffers; Chunk buffers non-empty."));
	}

	buffers_.resize(nchunks);
	chunk_size_ = chunk_size;
	last_chunk_size_ = last_chunk_size;

	return Status::Ok();
}

Status ChunkBuffers::alloc(
	const size_t chunk_idx,
	void **const buffer) {

  assert(buffer);

  if (chunk_idx >= buffers_.size()) {
    return LOG_STATUS(
      Status::TileError(
        "Cannot alloc internal chunk buffer; Chunk index out of bounds"));
  }

  const uint32_t chunk_size = chunk_size(chunk_idx);
  void *const chunk_buffer = malloc(chunk_size);
  if (!chunk_buffer) {
    LOG_FATAL("malloc() failed");
  }

  buffers_[chunk_idx] = chunk_buffer;
  return Status::Ok();
}

Status ChunkBuffers::internal_buffer(
  	const size_t chunk_idx,
  	void **const buffer,
  	uint32_t *const size) {

  assert(buffer);

  if (chunk_idx >= buffers_.size()) {
    return LOG_STATUS(
      Status::TileError(
        "Cannot get internal chunk buffer; Chunk index out of bounds"));
  }

  *buffer = buffers_[chunk_idx];
  if (size) {
  	*size = chunk_size(chunk_idx);
  }

  return Status::Ok();
}

uint32_t ChunkBuffers::chunk_size(const size_t chunk_idx) const {
	assert(chunk_idx < buffers_.size());
	return
	  chunk_idx == (buffers_.size() - 1) ?
        last_chunk_size_ : chunk_size_;
}

}  // namespace sm
}  // namespace tiledb
