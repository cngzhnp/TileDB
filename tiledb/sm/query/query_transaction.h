/**
 * @file   query_transaction.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2019 TileDB, Inc.
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
 * This file declares transactional Query class
 */

#ifndef TILEDB_QUERY_TRANSACTION_H
#define TILEDB_QUERY_TRANSACTION_H

#include "tiledb/sm/query/query.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

class QueryTransaction {
  public:
	QueryTransaction(Query* query);

	~QueryTransaction();

	Status commit();

	Status roll_back();

  	QueryType type() const {
  		return query_->type();
  	}

  	const Array* array() const {
  		return query_->array();
  	}

  	// TODO
  	Array* array() {
  		return query_->array();
  	}

	const Reader* reader() const {
		return query_->reader();
	}

	// TODO
	Reader* reader() {
		return query_->reader();
	}

	
	const Writer* writer() const {
		return query_->writer();
	}

	// TODO
	Writer* writer() {
		return query_->writer();
	}

  	const ArraySchema* array_schema() const {
  		return query_->array_schema();
  	}

  	// TODO
  	Status get_buffer(
      const char* attribute, void** buffer, uint64_t** buffer_size) const {
  		return query_->get_buffer(attribute, buffer, buffer_size);
  	}

  	// TODO
  	Status get_buffer(
      const char* attribute,
      uint64_t** buffer_off,
      uint64_t** buffer_off_size,
      void** buffer_val,
      uint64_t** buffer_val_size) const {
  		return query_->get_buffer(
  			attribute, buffer_off, buffer_off_size, buffer_val, buffer_val_size);
  	}

  	// TODO
  	Status get_attr_serialization_state(
      const std::string& attribute, Query::SerializationState::AttrState** state) {
  		return query_->get_attr_serialization_state(
  			attribute, state);
  	}

  	// TODO
  	Status set_buffer(
      const std::string& attribute,
      void* buffer,
      uint64_t* buffer_size,
      bool check_null_buffers = true) {
  		return query_->set_buffer(attribute, buffer, buffer_size, check_null_buffers);
  	}

  	// TODO
  	Status set_buffer(
      const std::string& attribute,
      uint64_t* buffer_off,
      uint64_t* buffer_off_size,
      void* buffer_val,
      uint64_t* buffer_val_size,
      bool check_null_buffers = true) {
  		return query_->set_buffer(
  			attribute, buffer_off, buffer_off_size, buffer_val, buffer_val_size, check_null_buffers);
  	}

  	// TODO
  	Status set_layout(Layout layout) {
  		return query_->set_layout(layout);
  	}

  	// TODO
  	void set_status(QueryStatus status) {
  		query_->set_status(status);
  	}

  private:
  	Query* const query_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_TRANSACTION_H
