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
    categoryAsCollection(false),
    safeInsert(false)
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
  
  if (configuration->getString("safe_insert", tmp)) {
    if (0 == tmp.compare("yes")) {
      safeInsert = true;
    }
  }
}

void MongoDBStore::periodicCheck() { 
  if (false == verifyConnection(true)) {
    LOG_OPER("[%s] MongoDB connection in failed state during periodic check", categoryHandled.c_str());
  }
}

bool MongoDBStore::open() {
  if (true == verifyConnection(false)) {
    return true;
  }
  
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
  return verifyConnection(true);
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
  store->safeInsert           = safeInsert;
  return copied;
}

bool MongoDBStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  
  if (false == verifyConnection(true)) {
      return false;
  }

  bool success = true;
  unsigned long messagesHandled = 0;
  time_t curTime;
  
  if (addTimestamp) { 
    curTime = time(0);
  }
  
  string ns = database + "." + (categoryAsCollection ? categoryToCollection(categoryHandled) : collection);

  try {

    for (logentry_vector_t::iterator iter = messages->begin();
        iter != messages->end();
        ++iter) {

      BSONObj p;
      BSONObjBuilder b;
      
      b << GENOID << "entry"    << (*iter)->message.data() 
                  << "category" << categoryHandled;

      if (addTimestamp) {
        b.appendTimeT("timestamp", curTime);
      }

      p = b.done();
      connection.insert(ns, p);
      connection.ensureIndex(ns, BSON("category" << 1));

      if (safeInsert) {
        BSONElement e;
        BSONObj fields;
        
        // Flush also checks for forceFsync flag
        flush();
        
        if (p.getObjectID(e) == true) {
          BSONObj ret = connection.findOne(ns, QUERY("_id" << e));
          
          if (ret.isEmpty()) {
            throw std::runtime_error("safe_insert failed, the document was not stored properly");
          }
        }
      }
      messagesHandled += 1;
    } 
  
  } catch (const std::exception& e) {
    success = false;

    LOG_OPER("[%s] MongoDB store failed to write. Exception: %s",
            categoryHandled.c_str(), e.what());       
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
  if (true == forceFsync && true == verifyConnection(true)) {
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

bool MongoDBStore::verifyConnection(bool reconnectIfFailed) {
  if (false == hasConnection || true == connection.isFailed()) {
    if (reconnectIfFailed) {
      LOG_OPER("[%s] MongoDB No active connection or connection failed, reconnecting", categoryHandled.c_str());
      return open();
    }
    return false;
  }
  return true;
}

/*
  Set allowed set of characters for collections and
  replace anything else with an underscore
*/
std::string MongoDBStore::categoryToCollection(const std::string& str) {
  string retval = str;
  std::string pattern   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";
  string::size_type pos = retval.find_first_not_of(pattern);

  while (pos != string::npos) {
    retval[pos] = '_';
    pos = retval.find_first_not_of(pattern, pos + 1);
  }
  return retval;
}

#endif