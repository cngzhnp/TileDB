/**
 * @file chunk_buffers.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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
 * This file defines class ChunkBuffers.
 */

#ifndef TILEDB_CHUNK_BUFFERS_H
#define TILEDB_CHUNK_BUFFERS_H

#include <vector>

namespace tiledb {
namespace sm {

class ChunkBuffers {
 public:
  ChunkBuffers();

  ChunkBuffers(const ChunkBuffers& rhs);

  ~ChunkBuffers();

  ChunkBuffers& operator=(const ChunkBuffers &rhs);

  ChunkBuffers shallow_copy() const;

  void swap(ChunkBuffers *rhs);

  void free();

  uint64_t size() const;

  uint32_t chunk_size() const;

  uint32_t last_chunk_size() const;

  Status init(
  	size_t nchunks,
  	uint32_t chunk_size,
  	uint32_t last_chunk_size);

  Status alloc(size_t chunk_idx, void **buffer);

  // TODO, 'size' is optional.
  Status internal_buffer(
  	size_t chunk_idx, void **buffer, uint32_t *size = nullptr);



 private:
  void deep_copy(const ChunkBuffers &rhs);

  uint32_t chunk_size(size_t chunk_idx) const;

 private:
  std::vector<void *> buffers_;

  uint32_t chunk_size_;

  uint32_t last_chunk_size_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CHUNK_BUFFERS_H
