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

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Tile::Tile() {
  offset_ = 0;
  cell_size_ = 0;
  dim_num_ = 0;
  filtered_ = false;
  owns_chunk_buffers_ = true;
  pre_filtered_size_ = 0;
  type_ = Datatype::INT32;
}

Tile::Tile(unsigned int dim_num)
    : dim_num_(dim_num) {
  offset_ = 0;
  cell_size_ = 0;
  filtered_ = false;
  owns_chunk_buffers_ = true;
  pre_filtered_size_ = 0;
  type_ = Datatype::INT32;
}

Tile::Tile(
    Datatype type,
    uint64_t cell_size,
    unsigned int dim_num,
    Buffer* buff,
    bool owns_buff)
    : offset_(0)
    , cell_size_(cell_size)
    , dim_num_(dim_num)
    , filtered_(false)
    , owns_chunk_buffers_(owns_buff)
    , pre_filtered_size_(0)
    , type_(type) {

  store_buffer_as_chunk_buffers(buff);
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
  if (owns_chunk_buffers_)
    chunk_buffers_.free();
}

Tile& Tile::operator=(const Tile& tile) {
  // Free existing buffer if owned.
  if (owns_chunk_buffers_) {
    chunk_buffers_.free();
    owns_chunk_buffers_ = false;
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

  return Status::Ok();
}

#if 0
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

  buffer_ = new Buffer();
  if (buffer_ == nullptr)
    return LOG_STATUS(
        Status::TileError("Cannot initialize tile; Buffer allocation failed"));
  RETURN_NOT_OK(buffer_->realloc(tile_size));

  return Status::Ok();
}
#endif

void Tile::advance_offset(uint64_t nbytes) {
  offset_ += nbytes;
}

#if 0
Buffer* Tile::buffer() const {
  return buffer_;
}
#endif

ChunkBuffers* Tile::chunk_buffers() {
  return &chunk_buffers_;
}

Tile Tile::clone(bool deep_copy) const {
  Tile clone;
  clone.cell_size_ = cell_size_;
  clone.dim_num_ = dim_num_;
  clone.filtered_ = filtered_;
  clone.format_version_ = format_version_;
  clone.pre_filtered_size_ = pre_filtered_size_;
  clone.type_ = type_;

  if (deep_copy) {
    clone.owns_chunk_buffers_ = owns_chunk_buffers_;
    if (owns_chunk_buffers_) {
      // Calls ChunkBuffers copy-assign, which performs a deep copy.
      *clone.chunk_buffers_ = *chunk_buffers_;
    } else {
      clone.chunk_buffers_ = chunk_buffers_.shallow_copy();
    }
  } else {
    clone.owns_chunk_buffers_ = false;
    clone.chunk_buffers_ = chunk_buffers_.shallow_copy();
  }

  return clone;
}

uint64_t Tile::cell_size() const {
  return cell_size_;
}

Status Tile::internal_data(
  const uint64_t offset,
  void **const buffer,
  uint32_t *buffer_size) const {

  if (offset >= chunk_buffers_.size()) {
    return Status::TileError("Invalid tile offset");
  }

  const size_t chunk_idx = offset / chunk_buffers_.chunk_size();
  RETURN_NOT_OK(chunk_buffers_.internal_buffer(buffer, buffer_size));

  const size_t chunk_offset = offset % chunk_buffers_.chunk_size();
  *buffer = static_cast<char*>(*buffer) + chunk_offset;
  *buffer_size - chunk_offset;

  assert(*buffer_size >= 1);
  assert(*buffer_size <= chunk_buffers_.chunk_size());

  return Status::Ok();
}

unsigned int Tile::dim_num() const {
  return dim_num_;
}

void Tile::disown_buff() {
  owns_chunk_buffers_ = false;
}

bool Tile::empty() const {
  return chunk_buffers_.empty();
}

bool Tile::filtered() const {
  return filtered_;
}

uint32_t Tile::format_version() const {
  return format_version_;
}

bool Tile::full() const {
  return !chunk_buffers_.empty() &&
         offset_ >= chunk_buffers_.size();
}

uint64_t Tile::offset() const {
  return offset_;
}

uint64_t Tile::pre_filtered_size() const {
  return pre_filtered_size_;
}

#if 0
Status Tile::realloc(uint64_t nbytes) {
  return buffer_->realloc(nbytes);
}
#endif

#if 0
Status Tile::realloc_chunks(const uint64_t nchunks) {
  if (owns_chunk_buffers_) {
    chunk_buffers_.free();
  } else {
    chunk_buffers_.clear();
  }

  RETURN_NOT_OK(chunk_buffers_->init(nchunks));
  owns_chunk_buffers_ = true;
}
#endif

Status Tile::read(void* buffer, uint64_t nbytes) {
  RETURN_NOT_OK(chunk_buffers_->read(buffer, nbytes, offset_));
  offset_ += nbytes;

  return Status::Ok();
}

Status Tile::read(
    void* const buffer,
    const uint64_t nbytes,
    const uint64_t offset) const {

  return chunk_buffers_->read(buffer, nbytes, offset);
}

void Tile::reset() {
  reset_offset();
  reset_size();
}

void Tile::reset_offset() {
  offset_ = 0;
}

#if 0
void Tile::reset_size() {
  buffer_->reset_size();
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
  buffer_->set_size(size);
}
#endif

uint64_t Tile::size() const {
  return chunk_buffers_.size();
}

void Tile::split_coordinates() {
  assert(dim_num_ > 0);

  // For easy reference
  uint64_t tile_size = chunk_buffers_.size();
  uint64_t coord_size = cell_size_ / dim_num_;
  uint64_t cell_num = tile_size / cell_size_;
  auto tile_c = (char*)buffer_->data();
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
  RETURN_NOT_OK(chunk_buffers_->write(buf->data(), buf->size(), offset_));
  offset_ += buf->size();

  return Status::Ok();
}

Status Tile::write(ConstBuffer* buf, uint64_t nbytes) {
  RETURN_NOT_OK(chunk_buffers_->write(buf->data(), nbytes, offset_));
  offset_ += nbytes;

  return Status::Ok();
}

Status Tile::write(const void* data, uint64_t nbytes) {
  RETURN_NOT_OK(chunk_buffers_->write(data, nbytes, offset_));
  offset_ += nbytes;

  return Status::Ok();
}

Status Tile::write(const Tile& rhs) {
  RETURN_NOT_OK(chunk_buffers_->write(rhs.chunk_buffers_, offset_));
  offset_ += rhs.chunk_buffers_.size();

  return Status::Ok();
}

#if 0
Status Tile::write_with_shift(ConstBuffer* buf, uint64_t offset) {
  buffer_->write_with_shift(buf, offset);

  return Status::Ok();
}
#endif

void Tile::zip_coordinates() {
  assert(dim_num_ > 0);

  // For easy reference
  uint64_t tile_size = chunk_buffers_->size();
  uint64_t coord_size = cell_size_ / dim_num_;
  uint64_t cell_num = tile_size / cell_size_;
  auto tile_c = (char*)buffer_->data();
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
  chunk_buffers_.swap(&tile.chunk_buffers_);
  std::swap(offset_, tile.offset_);
  std::swap(cell_size_, tile.cell_size_);
  std::swap(dim_num_, tile.dim_num_);
  std::swap(filtered_, tile.filtered_);
  std::swap(format_version_, tile.format_version_);
  std::swap(owns_chunk_buffers_, tile.owns_chunk_buffers_);
  std::swap(pre_filtered_size_, tile.pre_filtered_size_);
  std::swap(type_, tile.type_);
}

}  // namespace sm
}  // namespace tiledb
