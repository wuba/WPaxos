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
package com.wuba.wpaxos.sample.kv;

import com.wuba.wpaxos.sample.kv.rocksdb.RocksDBHolder;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CheckpointManager {
	private final Logger logger = LogManager.getLogger(KVTestServer.class);
	private Map<Integer, Boolean> checkPointLockFlag = new ConcurrentHashMap<>();
	private Map<Integer, Long> checkPointInstanceId = new ConcurrentHashMap<>();
	private static CheckpointManager checkpointManager = new CheckpointManager();

	private CheckpointManager() {
	}

	public static CheckpointManager getInstance() {
		return checkpointManager;
	}

	public boolean executeForCheckpoint(int groupIdx, long instanceID, byte[] paxosValue) {
		checkPointInstanceId.put(groupIdx, instanceID);
		return true;
	}

	public long getCheckpointInstanceID(int groupId) {
		Long instanceId = checkPointInstanceId.get(groupId);
		logger.info("get checkpoint groupid {} instanceid {}", groupId, instanceId);
		if (instanceId == null) {
			return -1;
		}
		return instanceId;
	}

	public int lockCheckpointState(int groupId) {
		logger.info("lock checkpoint");
		if (checkPointLockFlag.containsKey(groupId) && checkPointLockFlag.get(groupId)) {
			return 1;
		}
		checkPointLockFlag.put(groupId, true);
		return 0;
	}

	public int getCheckpointState(int groupId, JavaOriTypeWrapper<String> dirPath, List<String> fileList) {
		boolean makeCheckPoint = RocksDBHolder.makeCheckPoint(groupId, dirPath, fileList);
		logger.info("get checkpoint path {} result {} ", dirPath.getValue(), makeCheckPoint);
		return makeCheckPoint ? 0 : 1;
	}

	public void unLockCheckpointState(int groupId) {
		logger.info("unlock checkpoint");
		RocksDBHolder.cleanCheckPoint(groupId);
		checkPointLockFlag.put(groupId, false);
	}

	public int loadCheckpointState(int groupId, String checkpointTmpFileDirPath, List<String> fileList,
	                               long checkpointInstanceID) {
		logger.info("load checkpoint {}", checkpointTmpFileDirPath);
		if (RocksDBHolder.learnCheckPoint(groupId, checkpointTmpFileDirPath,fileList)) {
			logger.info("put checkpoint instanceid {}",checkpointInstanceID);
			checkPointInstanceId.put(groupId, checkpointInstanceID);
			return 0;
		}
		return 1;
	}

	public void fixCheckpointByMinChosenInstanceId(int groupId, long minChosenInstanceID) {
		logger.info("fix checkpoint by minChosenInstanceId,groupid {} minChosenInstanceID {} ", groupId, minChosenInstanceID);
		if (!checkPointInstanceId.containsKey(groupId) || checkPointInstanceId.get(groupId) < minChosenInstanceID) {
			checkPointInstanceId.put(groupId, minChosenInstanceID);
		}
	}
}
