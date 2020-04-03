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

import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.config.WriteState;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/**
 * paxos log storage interface
 */
public interface LogStorage {

	/**
	 * log storage init
	 * @param option
	 * @return
	 * @throws Exception
	 */
	public boolean init(Options option) throws Exception;

	/**
	 * get one group paxos log storage root path
	 * @param groupIdx
	 * @return
	 */
	public String getLogStorageDirPath(int groupIdx);

	/**
	 * get one group <instanceID, value> pair
	 * @param groupIdx
	 * @param instanceID
	 * @param valueWrap
	 * @return
	 */
	public int get(int groupIdx, long instanceID, JavaOriTypeWrapper<byte[]> valueWrap);

	/**
	 * put one group <instanceID, value> pair
	 * @param writeOptions
	 * @param groupIdx
	 * @param instanceID
	 * @param sValue
	 * @param writeState
	 * @return
	 */
	public int put(WriteOptions writeOptions, int groupIdx, long instanceID, byte[] sValue, WriteState writeState);

	/**
	 * delete one group instanceID value
	 * @param writeOptions
	 * @param groupIdx
	 * @param instanceID
	 * @return
	 */
	int delOne(WriteOptions writeOptions, int groupIdx, long instanceID);

	/**
	 * delete one group instances less than maxInstanceId
	 * @param writeOptions
	 * @param groupIdx
	 * @param maxInstanceId
	 * @return
	 */
	int delExpire(WriteOptions writeOptions, int groupIdx, long maxInstanceId);

	/**
	 * 获取最大instance id，设置到instanceID中
	 *
	 * @param groupIdx
	 * @return
	 */
	int getMaxInstanceID(int groupIdx, JavaOriTypeWrapper<Long> instanceID);

	/**
	 * set one group min chosen instanceID
	 * @param writeOptions
	 * @param groupIdx
	 * @param minInstanceID
	 * @return
	 */
	int setMinChosenInstanceID(WriteOptions writeOptions, int groupIdx, long minInstanceID);

	/**
	 * get one group min chosen instanceID
	 * @param groupIdx
	 * @return
	 */
	long getMinChosenInstanceID(int groupIdx);

	/**
	 * clear one group all data
	 * @param groupIdx
	 * @return
	 */
	int clearAllLog(int groupIdx);

	/**
	 * set and store one group system variables
	 * @param writeOptions
	 * @param groupIdx
	 * @param buffer
	 * @return
	 */
	int setSystemVariables(WriteOptions writeOptions, int groupIdx, byte[] buffer);

	/**
	 * get one group system variables
	 * @param groupIdx
	 * @return
	 */
	byte[] getSystemVariables(int groupIdx);

	/**
	 * set and store one group master variables
	 * @param writeOptions
	 * @param groupIdx
	 * @param buffer
	 * @return
	 */
	int setMasterVariables(WriteOptions writeOptions, int groupIdx, byte[] buffer);
	
	/**
	 * get one group master variables
	 * @param groupIdx
	 * @return
	 */
	byte[] getMasterVariables(int groupIdx);

	/**
	 * paxos log storage start
	 */
	void start();

	/**
	 * paxos log storage shutdown
	 */
	void shutdown();

	/**
	 * delete one group instanceId value
	 * @param groupId
	 * @param instanceId
	 */
	void deleteOneIndex(int groupId, long instanceId);

	/**
	 * delete one group instanceId value
	 * @param groupId
	 * @param maxInstanceId
	 */
	void deleteExpireIndex(int groupId, long maxInstanceId);
	
	/**
	 * the group is avalable for read or write
	 * @param groupId
	 * @return
	 */
	boolean isAvailable(int groupId);

}
