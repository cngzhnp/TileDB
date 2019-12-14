/**
 * @file   tile.cc
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
 * This file implements class Tile.
 */

#include "tiledb/sm/tile/tile.h"
#include "tiledb/sm/misc/logger.h"

#include <iostream>

namespace tiledb {
namespace sm {

Status Tile::compute_chunk_size(
  const uint64_t tile_size,
  const uint32_t tile_dim_num,
  const uint64_t tile_cell_size,
  uint32_t *const chunk_size) {

  const uint32_t dim_num = tile_dim_num > 0 ? tile_dim_num : 1;
  const uint64_t dim_tile_size = tile_size / dim_num;
  const uint64_t dim_cell_size = tile_cell_size / dim_num;

  uint64_t chunk_size64 =
    std::min(constants::max_tile_chunk_size, dim_tile_size);
  chunk_size64 = chunk_size64 / dim_cell_size * dim_cell_size;
  chunk_size64 = std::max(chunk_size64, dim_cell_size);
  if (chunk_size64 > std::numeric_limits<uint32_t>::max()) {
    return LOG_STATUS(
        Status::TileError("Chunk size exceeds uint32_t"));
  }

  *chunk_size = chunk_size64;
  return Status::Ok();
}

Status Tile::buffer_to_contigious_fixed_chunks(
  const Buffer& buffer,
  const uint32_t tile_dim_num,
  const uint64_t tile_cell_size,
  ChunkedBuffer *chunked_buffer) {

   // Calculate the chunk size for 'buff'.
  uint32_t chunk_size;
  RETURN_NOT_OK(compute_chunk_size(
    buffer.size(), tile_dim_num, tile_cell_size, &chunk_size));

  // Initialize contigious, fixed size 'chunked_buffer_'.
  RETURN_NOT_OK(chunked_buffer->init_fixed_size(
    ChunkedBuffer::BufferAddressing::CONTIGIOUS, buffer.size(), chunk_size));

  RETURN_NOT_OK(chunked_buffer->set_contigious(buffer.data()));

  return Status::Ok();
}

Status Tile::buffer_to_contigious_var_chunks(
  void *const buffer,
  std::vector<uint32_t>&& var_chunk_sizes,
  ChunkedBuffer *chunked_buffer) {

  // Initialize contigious, var sized 'chunked_buffer_'.
  RETURN_NOT_OK(chunked_buffer->init_var_size(
    ChunkedBuffer::BufferAddressing::CONTIGIOUS, std::move(var_chunk_sizes)));

  RETURN_NOT_OK(chunked_buffer->set_contigious(buffer));

#if 0
  // Set each individual chunk buffer, ensuring that 'buff'
  // may be accessed as a contiguous buffer from chunk 0.
  uint64_t offset = 0;
  for (size_t i = 0; i < chunked_buffer->nchunks(); ++i) {
    chunked_buffer->set(i, (static_cast<char*>(buffer) + offset));

    uint32_t chunk_buffer_size;
    RETURN_NOT_OK(chunked_buffer->internal_buffer_size(i, &chunk_buffer_size));
    offset += chunk_buffer_size;
  }
#endif

  return Status::Ok();
}

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Tile::Tile() {
  chunked_buffer_ = nullptr;
  offset_ = 0;
  cell_size_ = 0;
  dim_num_ = 0;
  filtered_ = false;
  owns_chunked_buffer_ = true;
  pre_filtered_size_ = 0;
  format_version_ = 0;
  type_ = Datatype::INT32;
}

Tile::Tile(unsigned int dim_num)
    : dim_num_(dim_num) {
  chunked_buffer_ = nullptr;
  offset_ = 0;
  cell_size_ = 0;
  filtered_ = false;
  owns_chunked_buffer_ = true;
  pre_filtered_size_ = 0;
  format_version_ = 0;
  type_ = Datatype::INT32;
}

Tile::Tile(
    const Datatype type,
    const uint64_t cell_size,
    const unsigned int dim_num,
    ChunkedBuffer *const chunked_buffer,
    const bool owns_buff)
    : chunked_buffer_(chunked_buffer)
    , offset_(0)
    , cell_size_(cell_size)
    , dim_num_(dim_num)
    , filtered_(false)
    , format_version_(0)
    , owns_chunked_buffer_(owns_buff)
    , pre_filtered_size_(0)
    , type_(type) {
}

Tile::Tile(
    const uint32_t format_version,
    const Datatype type,
    const uint64_t cell_size,
    const unsigned int dim_num,
    ChunkedBuffer *const chunked_buffer,
    const bool owns_buff)
    : chunked_buffer_(chunked_buffer)
    , offset_(0)
    , cell_size_(cell_size)
    , dim_num_(dim_num)
    , filtered_(false)
    , format_version_(format_version)
    , owns_chunked_buffer_(owns_buff)
    , pre_filtered_size_(0)
    , type_(type) {
}

Tile::Tile(
    Datatype type,
    uint64_t cell_size,
    unsigned int dim_num,
    Buffer* buff,
    bool owns_buff)
    : chunked_buffer_(nullptr)
    , offset_(0)
    , cell_size_(cell_size)
    , dim_num_(dim_num)
    , filtered_(false)
    , owns_chunked_buffer_(owns_buff)
    , pre_filtered_size_(0)
    , type_(type) {
  if (buff) { }
  LOG_FATAL(" Tile::Tile TODO DELETE");
}

#if 0
Status Tile::store_buffer(Buffer *const buff) {
  // Calculate the chunk size for 'buff'.
  uint32_t chunk_size;
  RETURN_NOT_OK(compute_chunk_size(buff->size(), &chunk_size));

  // Calculate the last chunk size.
  uint32_t last_chunk_size = buff->size() % chunk_size;
  if (last_chunk_size == 0) {
    last_chunk_size = chunk_size;
  }

  // Calculate the number of chunks required to store 'buff'.
  const size_t nchunks =
    last_chunk_size == chunk_size ?
      buff->size() / chunk_size :
      buff->size() / chunk_size + 1;

  // Initialize 'chunked_buffer_'.
  RETURN_NOT_OK(chunked_buffer_->init_fixed_size(
    nchunks, chunk_size, last_chunk_size));

  // Set each individual chunk buffer, ensuring that 'buff'
  // may be accessed as a contiguous buffer from chunk 0.
  uint64_t offset = 0;
  for (size_t i = 0; i < nchunks; ++i) {
    chunked_buffer_->set(i, buff->data(offset));
    offset += chunk_size;
  }

  return Status::Ok();
}
#endif

void Tile::release_chunked_buffer() {
  // If we own the buffers, we must free them.
  if (owns_chunked_buffer_) {
    chunked_buffer_->free();
  } else {
    chunked_buffer_->clear();
  }
}

Tile::Tile(const Tile& tile)
    : Tile() {
  // Make a deep-copy clone
  auto clone = tile.clone(true);
  // Swap with the clone
  swap(clone);
}

Tile::Tile(Tile&& tile)
    : Tile() {
  // Swap with the argument
  swap(tile);
}

Tile::~Tile() {
  if (owns_chunked_buffer_ && chunked_buffer_ != nullptr) {
    chunked_buffer_->free();
    delete chunked_buffer_;
  }
}

Tile& Tile::operator=(const Tile& tile) {
  // Free existing buffer if owned.
  if (owns_chunked_buffer_) {
    chunked_buffer_->free();
    delete chunked_buffer_;
    chunked_buffer_ = nullptr;
    owns_chunked_buffer_ = false;
  }

  // Make a deep-copy clone
  auto clone = tile.clone(true);
  // Swap with the clone
  swap(clone);

  return *this;
}

Tile& Tile::operator=(Tile&& tile) {
  // Swap with the argument
  swap(tile);

  return *this;
}

/* ****************************** */
/*               API              */
/* ****************************** */

uint64_t Tile::cell_num() const {
  return size() / cell_size_;
}

Status Tile::init(
    uint32_t format_version,
    Datatype type,
    uint64_t cell_size,
    unsigned int dim_num) {
  cell_size_ = cell_size;
  dim_num_ = dim_num;
  type_ = type;
  format_version_ = format_version;

  // how do we know how to init chunked buffer?
  LOG_FATAL("Tile::init not ready");

  return Status::Ok();
}

Status Tile::init(
    uint32_t format_version,
    Datatype type,
    uint64_t tile_size,
    uint64_t cell_size,
    unsigned int dim_num) {
  cell_size_ = cell_size;
  dim_num_ = dim_num;
  type_ = type;
  format_version_ = format_version;

    chunked_buffer_ = new ChunkedBuffer();
  if (chunked_buffer_ == nullptr)
    return LOG_STATUS(
        Status::TileError("Cannot initialize tile; ChunkedBuffer allocation failed"));

  uint32_t chunk_size;
  RETURN_NOT_OK(
    compute_chunk_size(tile_size, dim_num, cell_size_, &chunk_size));
  RETURN_NOT_OK(
    chunked_buffer_->init_fixed_size(
      ChunkedBuffer::BufferAddressing::DISCRETE, tile_size, chunk_size));

  return Status::Ok();
}

void Tile::advance_offset(uint64_t nbytes) {
  offset_ += nbytes;
}

#if 0
Buffer* Tile::buffer() const {
  return const_cast<Buffer*>(&buffer_);
}
#endif

Buffer* Tile::buffer2() const {
  LOG_FATAL("Tile::buffer2 not ready");
  return const_cast<Buffer*>(&buffer_);
}

Buffer* Tile::buffer3() const {
  LOG_FATAL("Tile::buffer3 not ready");
  return nullptr;
}

ChunkedBuffer* Tile::chunked_buffer() {
  return chunked_buffer_;
}

Tile Tile::clone(bool deep_copy) const {
  Tile clone;
  clone.cell_size_ = cell_size_;
  clone.dim_num_ = dim_num_;
  clone.filtered_ = filtered_;
  clone.format_version_ = format_version_;
  clone.pre_filtered_size_ = pre_filtered_size_;
  clone.type_ = type_;
  clone.offset_ = offset_;

  if (deep_copy) {
    clone.owns_chunked_buffer_ = owns_chunked_buffer_;
    if (owns_chunked_buffer_ && chunked_buffer_ != nullptr) {
      clone.chunked_buffer_ = new ChunkedBuffer();
      // Calls ChunkedBuffer copy-assign, which performs a deep copy.
      *clone.chunked_buffer_ = *chunked_buffer_;
    } else {
      clone.chunked_buffer_ = chunked_buffer_;
    }
  } else {
    clone.owns_chunked_buffer_ = false;
    clone.chunked_buffer_ = chunked_buffer_;
  }

  return clone;
}

uint64_t Tile::cell_size() const {
  return cell_size_;
}

#if 0
Status Tile::internal_data(
  const uint64_t offset,
  void **const buffer,
  uint32_t *buffer_size) const {

  if (offset >= chunked_buffer_->size()) {
    return Status::TileError("Invalid tile offset");
  }

  const size_t chunk_idx = offset / chunked_buffer_->chunk_size();
  RETURN_NOT_OK(chunked_buffer_->internal_buffer(buffer, buffer_size));

  const size_t chunk_offset = offset % chunked_buffer_->chunk_size();
  *buffer = static_cast<char*>(*buffer) + chunk_offset;
  *buffer_size - chunk_offset;

  assert(*buffer_size >= 1);
  assert(*buffer_size <= chunked_buffer_->chunk_size());

  return Status::Ok();
}
#endif

unsigned int Tile::dim_num() const {
  return dim_num_;
}

void Tile::disown_buff() {
  owns_chunked_buffer_ = false;
}

bool Tile::owns_buff() const {
  return owns_chunked_buffer_;
}

bool Tile::empty() const {
  return (chunked_buffer_ == nullptr) || chunked_buffer_->empty();
}

bool Tile::filtered() const {
  return filtered_;
}

uint32_t Tile::format_version() const {
  return format_version_;
}

bool Tile::full() const {
  return !empty() && offset_ >= chunked_buffer_->size();
}

uint64_t Tile::offset() const {
  return offset_;
}

uint64_t Tile::pre_filtered_size() const {
  return pre_filtered_size_;
}

#if 0
Status Tile::realloc(uint64_t nbytes) {
  return buffer_.realloc(nbytes);
}
#endif

#if 0
Status Tile::realloc_chunks(const uint64_t nchunks) {
  if (owns_chunked_buffer_) {
    chunked_buffer_->free();
  } else {
    chunked_buffer_->clear();
  }

  RETURN_NOT_OK(chunked_buffer_->init_fixed_size(nchunks));
  owns_chunked_buffer_ = true;
}
#endif

Status Tile::read(void* buffer, uint64_t nbytes) {
  RETURN_NOT_OK(chunked_buffer_->read(buffer, nbytes, offset_));
  offset_ += nbytes;

  return Status::Ok();
}

Status Tile::read(
    void* const buffer,
    const uint64_t nbytes,
    const uint64_t offset) const {

  return chunked_buffer_->read(buffer, nbytes, offset);
}

void Tile::reset() {
  reset_offset();
  release_chunked_buffer();
}

void Tile::reset_offset() {
  offset_ = 0;
}

#if 0
void Tile::reset_size() {
  buffer_.reset_size();
}
#endif

void Tile::set_filtered(bool filtered) {
  filtered_ = filtered;
}

void Tile::set_offset(uint64_t offset) {
  offset_ = offset;
}

void Tile::set_pre_filtered_size(uint64_t pre_filtered_size) {
  pre_filtered_size_ = pre_filtered_size;
}

#if 0
void Tile::set_size(uint64_t size) {
  buffer_.set_size(size);
}
#endif

uint64_t Tile::size() const {
  return (chunked_buffer_ == nullptr) ? 0 : chunked_buffer_->size();
}

void Tile::split_coordinates() {
  LOG_FATAL("Tile::split not ready");
  assert(dim_num_ > 0);

  // For easy reference
  uint64_t tile_size = buffer_.size();
  uint64_t coord_size = cell_size_ / dim_num_;
  uint64_t cell_num = tile_size / cell_size_;
  auto tile_c = (char*)buffer_.data();
  uint64_t ptr = 0, ptr_tmp = 0;

  // Create a tile clone
  auto tile_tmp = (char*)std::malloc(tile_size);
  std::memcpy(tile_tmp, tile_c, tile_size);

  // Split coordinates
  for (unsigned int j = 0; j < dim_num_; ++j) {
    ptr_tmp = j * coord_size;
    for (uint64_t i = 0; i < cell_num; ++i) {
      std::memcpy(tile_c + ptr, tile_tmp + ptr_tmp, coord_size);
      ptr += coord_size;
      ptr_tmp += cell_size_;
    }
  }

  // Clean up
  std::free((void*)tile_tmp);
}

bool Tile::stores_coords() const {
  return dim_num_ > 0;
}

Datatype Tile::type() const {
  return type_;
}

Status Tile::write(ConstBuffer* buf) {
  RETURN_NOT_OK(chunked_buffer_->write(buf->data(), buf->size(), offset_));
  offset_ += buf->size();

  return Status::Ok();
}

Status Tile::write(ConstBuffer* buf, uint64_t nbytes) {
  RETURN_NOT_OK(chunked_buffer_->write(buf->data(), nbytes, offset_));
  offset_ += nbytes;

  return Status::Ok();
}

Status Tile::write(const void* data, uint64_t nbytes) {
  RETURN_NOT_OK(chunked_buffer_->write(data, nbytes, offset_));
  offset_ += nbytes;

  return Status::Ok();
}

Status Tile::write(const Tile& rhs) {
  RETURN_NOT_OK(chunked_buffer_->write(*rhs.chunked_buffer_, offset_));
  offset_ += rhs.chunked_buffer_->size();

  return Status::Ok();
}

#if 0
Status Tile::write_with_shift(ConstBuffer* buf, uint64_t offset) {
  buffer_.write_with_shift(buf, offset);

  return Status::Ok();
}
#endif 

void Tile::zip_coordinates() {
  LOG_FATAL("Tile::zip_coordinates not ready");
  assert(dim_num_ > 0);

  // For easy reference
  uint64_t tile_size = buffer_.size();
  uint64_t coord_size = cell_size_ / dim_num_;
  uint64_t cell_num = tile_size / cell_size_;
  auto tile_c = (char*)buffer_.data();
  uint64_t ptr = 0, ptr_tmp = 0;

  // Create a tile clone
  auto tile_tmp = (char*)std::malloc(tile_size);
  std::memcpy(tile_tmp, tile_c, tile_size);

  // Zip coordinates
  for (unsigned int j = 0; j < dim_num_; ++j) {
    ptr = j * coord_size;
    for (uint64_t i = 0; i < cell_num; ++i) {
      std::memcpy(tile_c + ptr, tile_tmp + ptr_tmp, coord_size);
      ptr += cell_size_;
      ptr_tmp += coord_size;
    }
  }

  // Clean up
  std::free((void*)tile_tmp);
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void Tile::swap(Tile& tile) {
  // Note swapping buffer pointers here.
  std::swap(chunked_buffer_, tile.chunked_buffer_);
  std::swap(offset_, tile.offset_);
  std::swap(cell_size_, tile.cell_size_);
  std::swap(dim_num_, tile.dim_num_);
  std::swap(filtered_, tile.filtered_);
  std::swap(format_version_, tile.format_version_);
  std::swap(owns_chunked_buffer_, tile.owns_chunked_buffer_);
  std::swap(pre_filtered_size_, tile.pre_filtered_size_);
  std::swap(type_, tile.type_);
}

}  // namespace sm
}  // namespace tiledb
