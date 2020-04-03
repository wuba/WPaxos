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
package com.wuba.wpaxos.helper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.base.BallotNumber;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.config.Def;
import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.proto.AcceptorStateData;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.store.PaxosLog;
import com.wuba.wpaxos.utils.Crc32;

/**
 * leaner状态封装
 */
public class LearnerState {
	private static final Logger logger = LogManager.getLogger(LearnerState.class); 
	private byte[] learnedValue;
	private boolean isLearned;
	private int newChecksum;
	private Config config;
	private PaxosLog paxosLog;

	public LearnerState(Config config, LogStorage logStorage) {
		this.paxosLog = new PaxosLog(logStorage);
		this.config = config;
		init();
	}

	public void init() {
		this.learnedValue = new byte[]{};
		this.isLearned = false;
		this.newChecksum = 0;
	}

	public int learnValue(long instanceID, BallotNumber learnedBallot, byte[] value, int lastChecksum) {
		if(instanceID > 0 && lastChecksum == 0) {
			this.newChecksum = 0;
		} else if(value.length > 0) {
			this.newChecksum = Crc32.crc32(lastChecksum, value, value.length, Def.CRC32SKIP);
		}
		
		AcceptorStateData state = new AcceptorStateData();
		state.setInstanceID(instanceID);
		state.setAcceptedValue(value);
		state.setPromiseID(learnedBallot.getProposalID());
		state.setPromiseNodeID(learnedBallot.getNodeId());
		state.setAcceptedID(learnedBallot.getProposalID());
		state.setAcceptedNodeID(learnedBallot.getNodeId());
		state.setCheckSum(this.newChecksum);
		
		WriteOptions writeOptions = new WriteOptions();
		writeOptions.setSync(false);
		
		int ret = this.paxosLog.writeState(writeOptions, this.config.getMyGroupIdx(), instanceID, state);
		
		if(ret != 0) {
			logger.error("LogStorage.WriteLog fail, InstanceID {}, ValueLen {}, ret {}.", instanceID, value.length, ret);
			return ret;
		}
		
		learnValueWithoutWrite(instanceID, value, this.newChecksum);
		return 0;
	}

	public void learnValueWithoutWrite(long instanceID, byte[] value, int newCheckSum) {
		this.learnedValue = value;
		this.isLearned = true;
		this.newChecksum = newCheckSum;
	}

	public byte[] getLearnValue() {
		return this.learnedValue;
	}

	public boolean getIsLearned() {
		return this.isLearned;
	}

	public int getNewChecksum() {
		return this.newChecksum;
	}
}

















