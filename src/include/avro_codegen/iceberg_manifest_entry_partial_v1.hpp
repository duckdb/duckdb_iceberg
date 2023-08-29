/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     https://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


#ifndef CPX2_HH_2561633724_v1__H_
#define CPX2_HH_2561633724_v1__H_


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

namespace c {
struct data_file_v1 {
   std::string file_path;
   std::string file_format;
   int64_t record_count;
   data_file_v1() :
		 file_path(std::string()),
		 file_format(std::string()),
		 record_count(int64_t())
   { }
};

struct manifest_entry_v1 {
   int32_t status;
   data_file_v1 data_file_; // NOTE: as generated, this is called data_file, but this causes issues with GCC
   manifest_entry_v1() :
		 status(int32_t()),
		 data_file_()
   { }
};

}
namespace avro {
template<> struct codec_traits<c::data_file_v1> {
   static void encode(Encoder& e, const c::data_file_v1& v) {
	   avro::encode(e, v.file_path);
	   avro::encode(e, v.file_format);
	   avro::encode(e, v.record_count);
   }
   static void decode(Decoder& d, c::data_file_v1& v) {
	   if (avro::ResolvingDecoder *rd =
			   dynamic_cast<avro::ResolvingDecoder *>(&d)) {
		   const std::vector<size_t> fo = rd->fieldOrder();
		   for (std::vector<size_t>::const_iterator it = fo.begin();
				it != fo.end(); ++it) {
			   switch (*it) {
			   case 0:
				   avro::decode(d, v.file_path);
				   break;
			   case 1:
				   avro::decode(d, v.file_format);
				   break;
			   case 2:
				   avro::decode(d, v.record_count);
				   break;
			   default:
				   break;
			   }
		   }
	   } else {
		   avro::decode(d, v.file_path);
		   avro::decode(d, v.file_format);
		   avro::decode(d, v.record_count);
	   }
   }
};

template<> struct codec_traits<c::manifest_entry_v1> {
   static void encode(Encoder& e, const c::manifest_entry_v1& v) {
	   avro::encode(e, v.status);
	   avro::encode(e, v.data_file_);
   }
   static void decode(Decoder& d, c::manifest_entry_v1& v) {
	   if (avro::ResolvingDecoder *rd =
			   dynamic_cast<avro::ResolvingDecoder *>(&d)) {
		   const std::vector<size_t> fo = rd->fieldOrder();
		   for (std::vector<size_t>::const_iterator it = fo.begin();
				it != fo.end(); ++it) {
			   switch (*it) {
			   case 0:
				   avro::decode(d, v.status);
				   break;
			   case 1:
				   avro::decode(d, v.data_file_);
				   break;
			   default:
				   break;
			   }
		   }
	   } else {
		   avro::decode(d, v.status);
		   avro::decode(d, v.data_file_);
	   }
   }
};

}
#endif
