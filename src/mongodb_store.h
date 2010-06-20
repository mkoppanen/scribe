//  Copyright (c) 2010 Mikko Koppanen
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/
//
// @author Mikko Koppanen

#ifndef __SCRIBE_MONGODB_H__
# define __SCRIBE_MONGODB_H__

#ifdef USE_SCRIBE_MONGODB

#include "store.h"

/* Not proud of this */
#ifdef VERSION
# define VERSION_TMP VERSION
# undef VERSION
#endif
#include "client/dbclient.h"
#define VERSION VERSION_TMP

class MongoDBStore : public Store {

 public:
  MongoDBStore(StoreQueue* storeq,
               const std::string& category,
               bool multi_category);
  ~MongoDBStore();

  boost::shared_ptr<Store> copy(const std::string &category);
  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  bool open();
  bool isOpen();
  void configure(pStoreConf configuration, pStoreConf parent);
  void close();
  void flush();
  void periodicCheck();

 protected:
   mongo::DBClientConnection connection;
   bool hasConnection;
    
   std::string remoteHost;
   long int remotePort;
   std::string database;
   std::string collection; 
   bool forceFsync;
   bool addTimestamp;
   bool categoryAsCollection;

   bool verifyConnection();

 private:
  // disallow copy, assignment, and empty construction
  MongoDBStore();
  MongoDBStore(MongoDBStore& rhs);
  MongoDBStore& operator=(MongoDBStore& rhs);
};

#endif // USE_SCRIBE_MONGODB
#endif // __SCRIBE_MONGODB_H__