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
package com.wuba.wpaxos.store;

import java.io.IOException;

import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.config.WriteState;

public interface DataBase {

	/**
	 * init one group data storage
	 * @param dbPath
	 * @param myGroupIdx
	 * @return true if success
	 * @throws IOException
	 */
	boolean init(String dbPath, int myGroupIdx) throws IOException;

	/**
	 * clear all data when sync checkpoint
	 * @throws IOException
	 */
	void clearAllLog() throws IOException;

	/**
	 * get instance value
	 * @param instanceId
	 * @return value buf
	 */
	byte[] get(long instanceId);

	/**
	 * write <instanceId, value> pair to paxos log and index
	 * @param writeOptions sync or async
	 * @param instanceId
	 * @param value
	 * @param writeState  whether hasPayLoad
	 * @return 0 if success
	 * @throws Exception
	 */
	int put(WriteOptions writeOptions, long instanceId, byte[] value, WriteState writeState) throws Exception;

	/**
	 * get max instanceId, it will be update when write <instanceId, value> pair
	 * @return
	 */
	long getMaxInstanceId();

	/**
	 * write minChosenInstanceId to indexDB
	 * @param writeOptions
	 * @param instanceId
	 */
	void setMinChosenInstanceId(WriteOptions writeOptions, long instanceId);

	/**
	 * get minChosenInstanceId from indexDB
	 * @return
	 */
	long getMinChosenInstanceId();

	/**
	 * update checkpoint instanceId if it's less than newMinChosenInstanceId
	 * @param groupId
	 * @param newMinChosenInstanceId
	 */
	void updateCpByMinChosenInstanceID(int groupId, long newMinChosenInstanceId);

	/**
	 * write group membership info to indexDB
	 * @param writeOptions
	 * @param buffer
	 */
	void setSystemVariables(WriteOptions writeOptions, byte[] buffer);

	/**
	 * get group membership info from indexDB
	 * @return
	 */
	byte[] getSystemVariables();

	/**
	 * write group master info to indexDB
	 * @param writeOptions
	 * @param buffer
	 */
	void setMasterVariables(WriteOptions writeOptions, byte[] buffer);

	/**
	 * get group master info from indexDB
	 * @return
	 */
	byte[] getMasterVariables();

	/**
	 * delete all paxos log instance id less than maxInstanceId
	 * @param writeOptions
	 * @param maxInstanceId
	 */
	void delExpire(WriteOptions writeOptions, long maxInstanceId);

	/**
	 * delete one instance index 
	 * @param writeOptions
	 * @param instanceId
	 */
	void delIndex(WriteOptions writeOptions, long instanceId);

	/**
	 * delete index instance id less than maxInstanceId
	 * @param writeOptions
	 * @param maxInstanceId
	 */
	void delIndexExpire(WriteOptions writeOptions, long maxInstanceId);
	
	/**
	 * whether database is available to write/read
	 * @return
	 */
	boolean isAvailable();
}
