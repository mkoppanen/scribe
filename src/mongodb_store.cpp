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

#ifdef USE_SCRIBE_MONGODB

#include "mongodb_store.h"

using namespace mongo;

MongoDBStore::MongoDBStore(StoreQueue* storeq,
                          const string& category,
                          bool multi_category)
  : Store(storeq, category, "mongodb", multi_category),
    remoteHost("localhost"),
    remotePort(27017),
    database("scribe"),
    collection("scribe"),
    forceFsync(false)
    {
    // we can't open the connection until we get configured
}

MongoDBStore::~MongoDBStore() {
  // TODO: add
}

void MongoDBStore::configure(pStoreConf configuration, pStoreConf parent) {
  Store::configure(configuration, parent);
  
  // Defaults should be fine if not set
  configuration->getString("remote_host", remoteHost);
  configuration->getInt("remote_port", remotePort);
  configuration->getString("database", database);
  configuration->getString("collection", collection);
  
  string temp;
  if (configuration->getString("force_fsync", temp)) {
    if (0 == temp.compare("yes")) {
      forceFsync = true;
    }
  }
}

void MongoDBStore::periodicCheck() {
  if (verifyConnection() == false) {
    LOG_OPER("[%s] Connection is in failed state during periodic check", categoryHandled.c_str());
  }
}

bool MongoDBStore::open() {
  
  // Already has a connection
  if (verifyConnection() == true) {
    return true;
  } 
  
  try {
    std::string server = remoteHost + ":" + boost::lexical_cast<std::string>(remotePort);
    connection.connect(server);
    hasConnection = true;
    
    LOG_OPER("[%s] MongoDB connected to server: %s", categoryHandled.c_str(), server.c_str());
  } catch (std::exception &e) {
    LOG_OPER("[%s] MongoDB failed to connect: %s", categoryHandled.c_str(), e.what());
    return false;
  }
  
  return true;
}

void MongoDBStore::close() {
  // Nothing to do
}

bool MongoDBStore::isOpen() {
  return verifyConnection();
}

shared_ptr<Store> MongoDBStore::copy(const std::string &category) {
  MongoDBStore *store = new MongoDBStore(storeQueue, category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->remoteHost = remoteHost;
  store->remotePort = remotePort;
  store->database   = database;
  store->collection = collection;
  store->forceFsync = forceFsync;

  return copied;
}

bool MongoDBStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  
  if (!isOpen()) {
    if (!open()) {
      LOG_OPER("[%s] Connection failed in MongoDBStore::handleMessages()",
               categoryHandled.c_str());
      return false;
    }
  }

  bool success = true;
  unsigned long messagesHandled = 0;
  
  for (logentry_vector_t::iterator iter = messages->begin();
       iter != messages->end();
       ++iter) {
    
    try {
      connection.insert(database + "." + collection, 
                        BSON(GENOID << "entry"    << (*iter)->message.data()
                                    << "category" << categoryHandled));

      ++messagesHandled;      
         
      } catch (const std::exception& e) {
         success = false;
         
         LOG_OPER("[%s] MongoDB store failed to write. Exception: %s",
                  categoryHandled.c_str(), e.what());
         
         // Something went wrong but we have handled something probably
         // Delete messages we have handled already     
         if (messagesHandled) {
           messages->erase(messages->begin(), iter);
         }
      }
  }
  
  return success;
}

void MongoDBStore::flush() {
  if (forceFsync && verifyConnection()) {
    BSONObj info;
    BSONObj cmd = BSON("fsync" << 1);

    if (connection.runCommand("admin", cmd, info) == false) {
      LOG_OPER("[%s] Fsync failed: %s", categoryHandled.c_str(), info.getStringField("errmsg"));
      return;
    }
  }
}

bool MongoDBStore::verifyConnection() {
  if (!hasConnection || connection.isFailed()) {
    LOG_OPER("[%s] No active connection", categoryHandled.c_str());
    return false;
  }
  return true;
}

#endif