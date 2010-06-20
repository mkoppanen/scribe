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
    connection(true),
    hasConnection(false),
    remoteHost("localhost"),
    remotePort(27017),
    database("scribe"),
    collection("scribe"),
    forceFsync(false),
    addTimestamp(false),
    categoryAsCollection(false)
    {
      // Connect when we have configuration
}

MongoDBStore::~MongoDBStore() {}

void MongoDBStore::configure(pStoreConf configuration, pStoreConf parent) {
  Store::configure(configuration, parent);

  // Defaults should be fine if not set
  configuration->getString("remote_host", remoteHost);
  configuration->getInt("remote_port", remotePort);
  configuration->getString("database", database);
  configuration->getString("collection", collection);
  
  string tmp;
  if (configuration->getString("force_fsync", tmp)) {
    if (0 == tmp.compare("yes")) {
      forceFsync = true;
    }
  }
  
  if (configuration->getString("add_timestamp", tmp)) {
    if (0 == tmp.compare("yes")) {
      addTimestamp = true;
    }
  }
  
  /*
    If set uses the category as collection name. 
    Takes precedence over configured collection name
  */
  if (configuration->getString("use_category_as_collection", tmp)) {
    if (0 == tmp.compare("yes")) {
      categoryAsCollection = true;
    }
  }
}

void MongoDBStore::periodicCheck() {
  if (verifyConnection() == false) {
    LOG_OPER("[%s] MongoDB connection in failed state during periodic check", categoryHandled.c_str());
  }
}

bool MongoDBStore::open() {
  try {
    std::string server = remoteHost + ":" + boost::lexical_cast<std::string>(remotePort);
    connection.connect(server);
    connection.resetError();
    hasConnection = true;
    LOG_OPER("[%s] MongoDB connected to server: %s", categoryHandled.c_str(), server.c_str());
    return true;
  } catch (std::exception &e) {
    LOG_OPER("[%s] MongoDB failed to connect: %s", categoryHandled.c_str(), e.what());
    return false;
  }
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

  store->remoteHost           = remoteHost;
  store->remotePort           = remotePort;
  store->database             = database;
  store->collection           = collection;
  store->forceFsync           = forceFsync;
  store->addTimestamp         = addTimestamp;
  store->categoryAsCollection = categoryAsCollection;
  return copied;
}

bool MongoDBStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  
  if (verifyConnection() == false) {
      return false;
  }

  bool success = true;
  unsigned long messagesHandled = 0;
  time_t curTime;
  
  if (addTimestamp) { 
    curTime = time(0);
  }
  
  string ns = database + "." + (categoryAsCollection ? categoryHandled : collection);
    
  for (logentry_vector_t::iterator iter = messages->begin();
       iter != messages->end();
       ++iter) {
    
    try {
      
      BSONObjBuilder b;
      
      b.append("entry", (*iter)->message.data());
      b.append("category", categoryHandled);    
      
      if (addTimestamp) {
        b.appendTimeT("timestamp", curTime);
      }
      
      connection.insert(ns, b.obj());
      ++messagesHandled;      
         
    } catch (const std::exception& e) {
      success = false;

      LOG_OPER("[%s] MongoDB store failed to write. Exception: %s",
              categoryHandled.c_str(), e.what());
    }
  } 
  
  LOG_OPER("[%s] MongoDB stored <%ld> messages out of <%ld> into <%s>",
           categoryHandled.c_str(), messagesHandled, messages->size(), ns.c_str());
  
  if (!success && messagesHandled) {
      // Something went wrong but we have handled something probably
      // Delete messages we have handled already     
      messages->erase(messages->begin(), messages->begin() + messagesHandled);
  }
  return success;
}

void MongoDBStore::flush() {
  if (forceFsync && verifyConnection()) {
    try {    
      BSONObj info;
      BSONObj cmd = BSON("fsync" << 1);

      if (connection.isFailed() || connection.runCommand("admin", cmd, info) == false) {
        LOG_OPER("[%s] Fsync failed: %s", categoryHandled.c_str(), info.getStringField("errmsg"));
        return;
      }
    } catch (std::exception &e) {
      LOG_OPER("[%s] Fsync failed: %s", categoryHandled.c_str(), e.what());
    }
  }
}

bool MongoDBStore::verifyConnection() {
  if (!hasConnection || connection.isFailed()) {
    LOG_OPER("[%s] MongoDB No active connection or connection failed, reconnecting", categoryHandled.c_str());
    return open();
  }
  return true;
}

#endif