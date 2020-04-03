/*
 * Copyright (C) 2005-present, 58.com.  All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wuba.wpaxos.store.db;


import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.store.FileID;

/**
 * paxoslog索引
 */
public interface IndexDB {
	/**
	 * 
	 * @param maxInstanceID
	 */
	void correctMaxInstanceID(long maxInstanceID);

	boolean putIndex(WriteOptions writeOptions, long instanceID,  FileID value);

	FileID getIndex(long instanceID);

	void deleteOneIndex(long instanceId);

	boolean init() ;

	byte[] getMaxInstanceID();

	void setMaxInstanceID(WriteOptions writeOptions, long instanceId);

	void setMastervariables(WriteOptions writeOptions, byte[] buffer);

	byte[] getMastervariables();

	byte[] getSystemvariables();

	void setSystemvariables(WriteOptions writeOptions, byte[] buffer);

	void setMinChosenInstanceID(WriteOptions writeOptions, long instanceId);

	byte[] getMinChosenInstanceID();

	void destroy();

	void deleteExpire(WriteOptions writeOptions, long maxInstanceId);
}
