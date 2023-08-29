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


#ifndef CPX_HH_1629567502_v1__H_
#define CPX_HH_1629567502_v1__H_

#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

namespace c {
struct manifest_file_v1 {
   std::string manifest_path;
   manifest_file_v1() :
		 manifest_path(std::string())
   { }
};

}
namespace avro {
template<> struct codec_traits<c::manifest_file_v1> {
   static void encode(Encoder& e, const c::manifest_file_v1& v) {
	   avro::encode(e, v.manifest_path);
   }
   static void decode(Decoder& d, c::manifest_file_v1& v) {
	   if (avro::ResolvingDecoder *rd =
			   dynamic_cast<avro::ResolvingDecoder *>(&d)) {
		   const std::vector<size_t> fo = rd->fieldOrder();
		   for (std::vector<size_t>::const_iterator it = fo.begin();
				it != fo.end(); ++it) {
			   switch (*it) {
			   case 0:
				   avro::decode(d, v.manifest_path);
				   break;
			   default:
				   break;
			   }
		   }
	   } else {
		   avro::decode(d, v.manifest_path);
	   }
   }
};

}
#endif
