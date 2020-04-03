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
package com.wuba.wpaxos;

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
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/**
 * acceptor current state
 */
public class AcceptorState {
	private static final Logger logger = LogManager.getLogger(AcceptorState.class); 
	private BallotNumber promiseBallot = new BallotNumber(0, 0);
	private BallotNumber acceptedBallot = new BallotNumber(0, 0);
	private byte[] acceptedValue;
	private int checkSum;
	private Config config;
	private PaxosLog paxosLog;
	private int syncTimes;
	
	public AcceptorState(Config config, LogStorage logStorage) {
		this.paxosLog = new PaxosLog(logStorage);
		this.syncTimes = 0;
		this.config = config;
		this.acceptedBallot = new BallotNumber(0, 0);
		init();
	}
	
	public BallotNumber getAcceptedBallot() {
		return acceptedBallot;
	}
	
	public void setAcceptedBallot(BallotNumber acceptedBallot) {
		this.acceptedBallot = acceptedBallot;
	}
	
	public byte[] getAcceptedValue() {
		return acceptedValue;
	}
	
	public void setAcceptedValue(byte[] acceptedValue) {
		this.acceptedValue = acceptedValue;
	}

	public int getCheckSum() {
		return checkSum;
	}

	public void setCheckSum(int checkSum) {
		this.checkSum = checkSum;
	}
	
	/**
	 * 持久化存储
	 * @param instanceID
	 * @param lastChecksum
	 * @return
	 */
	public int persist(long instanceID, int lastChecksum) {
		if(instanceID > 0 && lastChecksum == 0) {
			this.checkSum = 0;
		} else if(this.acceptedValue != null && this.acceptedValue.length > 0) {
			this.checkSum = Crc32.crc32(lastChecksum, this.acceptedValue, this.acceptedValue.length, Def.CRC32SKIP);
		}
		
		AcceptorStateData state = new AcceptorStateData();
		state.setInstanceID(instanceID);
		state.setPromiseID(this.promiseBallot.getProposalID());
		state.setPromiseNodeID(this.promiseBallot.getNodeId());
		state.setAcceptedID(this.acceptedBallot.getProposalID());
		state.setAcceptedNodeID(this.acceptedBallot.getNodeId());
		state.setAcceptedValue(this.acceptedValue);
		state.setCheckSum(this.checkSum);
		
		WriteOptions writeOption = new WriteOptions();
		writeOption.setSync(this.config.logSync());
		if(writeOption.isSync()) {
			this.syncTimes ++;
			if(this.syncTimes > this.config.syncInterval()) {
				this.syncTimes = 0;
			} else {
				writeOption.setSync(false);
			}
		}
		
		int ret = this.paxosLog.writeState(writeOption, this.config.getMyGroupIdx(), instanceID, state);
		if(ret != 0) {
			return ret;	
		}
		 
		logger.debug("GroupIdx {} InstanceID {} PromiseID {} PromiseNodeID {} AccectpedID {} AcceptedNodeID {} Checksum {}.", 
				 this.config.getMyGroupIdx(), instanceID, this.promiseBallot.getProposalID(), this.promiseBallot.getNodeId(), this.acceptedBallot.getProposalID(), this.acceptedBallot.getNodeId(), this.checkSum); 
		 
		return 0;
	}
	
	/**
	 * acceptor启动 state初始化。
	 * @param instanceID
	 * @return
	 * @throws Exception 
	 */
	public int load(JavaOriTypeWrapper<Long> instanceID) {
		int ret = this.paxosLog.getMaxInstanceIDFromLog(this.config.getMyGroupIdx(), instanceID);
		if(ret != 0 && ret != 1) {
			logger.error("Load max instance id fail, ret={}.", ret);
			return ret;
		}
		
		if(ret == 1) {
			logger.error("empty database");
			instanceID.setValue(0L);
			return 0;
		}
		
		AcceptorStateData state = new AcceptorStateData();
		ret = this.paxosLog.readState(this.config.getMyGroupIdx(), instanceID.getValue(), state);
		
		if(ret != 0) {
			return ret;
		}
		
		this.promiseBallot.setProposalID(state.getPromiseID());
		this.promiseBallot.setNodeId(state.getPromiseNodeID());
		this.acceptedBallot.setProposalID(state.getAcceptedID());
		this.acceptedBallot.setNodeId(state.getAcceptedNodeID());
		this.acceptedValue = state.getAcceptedValue();
		this.checkSum = state.getCheckSum();
		
		logger.info("GroupIdx {} InstanceID {} PromiseID {} PromiseNodeID {} AccectpedID {} AcceptedNodeID {} ValueLen {} Checksum {}.", this.config.getMyGroupIdx(), 
				instanceID, this.promiseBallot.getProposalID(), this.promiseBallot.getNodeId(), this.acceptedBallot.getProposalID(), this.acceptedBallot.getNodeId(), (this.acceptedValue == null ? 0 : this.acceptedValue.length), this.checkSum); 
		
		return 0;
	}
	
	public void init() {
		this.acceptedBallot.reset();
		this.acceptedValue = new byte[]{};
		this.checkSum = 0;
	}
	
	public BallotNumber getPromiseBallot() {
	    return this.promiseBallot;
	}
	
	public void setPromiseBallot(BallotNumber promiseBallot) {
	    this.promiseBallot = promiseBallot;
	}
}













