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
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.storemachine.StateMachine;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDBException;

import java.util.List;

public class KVTestSM implements StateMachine {
	private final Logger logger = LogManager.getLogger(KVTestSM.class);

	private int groupId;

	public KVTestSM(int group) {
		this.groupId = group;
	}

	@Override
	public int getSMID() {
		return 2;
	}

	@Override
	public boolean execute(int groupIdx, long instanceID, byte[] paxosValue, SMCtx smCtx) {
		KVOPValue kvopValue = KVOPValue.fromBytes(paxosValue);
		KVTestResult result = new KVTestResult();
		if (smCtx != null && smCtx.getpCtx() != null) {
			result = (KVTestResult) smCtx.getpCtx();
		}
		if (kvopValue.getOp() == KVOperation.WRITE.getOp()) {
			try {
				byte[] bytes = RocksDBHolder.get(kvopValue.getKey().getBytes(), groupIdx);
				if (bytes != null) {
					result.setRes(KVResult.KEY_EXIST);
				} else {
					RocksDBHolder.put(kvopValue.getKey().getBytes(), kvopValue.getValue().getBytes(), groupIdx);
					result.setRes(KVResult.SUCCESS);
				}
			} catch (RocksDBException e) {
				logger.error(e.getMessage(), e);
				result.setRes(KVResult.ROCKSDB_ERROR);
			}
		} else if (kvopValue.getOp() == KVOperation.READ.getOp()) {
			try {
				byte[] bytes = RocksDBHolder.get(kvopValue.getKey().getBytes(), groupIdx);
				if (bytes == null) {
					result.setRes(KVResult.KEY_NOT_EXIST);
				} else {
					result.setRes(KVResult.SUCCESS);
					result.setValue(new String(bytes));
				}
			} catch (RocksDBException e) {
				logger.error(e.getMessage(), e);
				result.setRes(KVResult.ROCKSDB_ERROR);
			}

		} else if (kvopValue.getOp() == KVOperation.DELETE.getOp()) {
			try {
				byte[] bytes = RocksDBHolder.get(kvopValue.getKey().getBytes(), groupIdx);
				if (bytes == null) {
					result.setRes(KVResult.KEY_NOT_EXIST);
				} else {
					RocksDBHolder.delete(kvopValue.getKey().getBytes(), groupIdx);
					result.setRes(KVResult.SUCCESS);
				}
			} catch (RocksDBException e) {
				logger.error(e.getMessage(), e);
				result.setRes(KVResult.ROCKSDB_ERROR);
			}
		}
		executeForCheckpoint(groupIdx, instanceID, paxosValue);
		return true;
	}

	@Override
	public boolean executeForCheckpoint(int groupIdx, long instanceID, byte[] paxosValue) {
		return CheckpointManager.getInstance().executeForCheckpoint(groupIdx, instanceID, paxosValue);
	}

	@Override
	public long getCheckpointInstanceID(int groupIdx) {
		// TODO Auto-generated method stub
		return CheckpointManager.getInstance().getCheckpointInstanceID(groupId);
	}

	@Override
	public int lockCheckpointState() {
		// TODO Auto-generated method stub
		return CheckpointManager.getInstance().lockCheckpointState(groupId);
	}

	@Override
	public int getCheckpointState(int groupIdx,
	                              JavaOriTypeWrapper<String> dirPath, List<String> fileList) {
		// TODO Auto-generated method stub
		return CheckpointManager.getInstance().getCheckpointState(groupIdx, dirPath, fileList);
	}

	@Override
	public void unLockCheckpointState() {
		// TODO Auto-generated method stub
		CheckpointManager.getInstance().unLockCheckpointState(groupId);
	}

	@Override
	public int loadCheckpointState(int groupIdx,
	                               String checkpointTmpFileDirPath, List<String> fileList,
	                               long checkpointInstanceID) {
		// TODO Auto-generated method stub
		return CheckpointManager.getInstance().loadCheckpointState(groupIdx, checkpointTmpFileDirPath, fileList, checkpointInstanceID);
	}

	@Override
	public byte[] beforePropose(int groupIdx, byte[] sValue) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean needCallBeforePropose() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void fixCheckpointByMinChosenInstanceId(long minChosenInstanceID) {
		CheckpointManager.getInstance().fixCheckpointByMinChosenInstanceId(groupId, minChosenInstanceID);
	}
}
