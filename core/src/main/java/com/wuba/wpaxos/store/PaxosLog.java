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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.config.WriteState;
import com.wuba.wpaxos.exception.SerializeException;
import com.wuba.wpaxos.proto.AcceptorStateData;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/**
 * paxos log 读写封装
 */
public class PaxosLog {
	private static final Logger logger = LogManager.getLogger(PaxosLog.class);
	
	private LogStorage logStorage;
	
    public PaxosLog(LogStorage logStorage) {
		super();
		this.logStorage = logStorage;
	}

	public int writeLog(WriteOptions writeOptions, int groupIdx, long instanceID, byte[] value) {
		AcceptorStateData state = new AcceptorStateData();
		state.setInstanceID(instanceID);
		state.setAcceptedValue(value);
		state.setPromiseID(0);
		state.setPromiseNodeID(0);
		state.setAcceptedID(0);
		state.setAcceptedNodeID(0);
		
		int ret = writeState(writeOptions, groupIdx, instanceID, state);
		return ret;
	}

    public int readLog(int groupIdx, long instanceID, byte[] value) {
    	AcceptorStateData state = new AcceptorStateData();
    	int ret = readState(groupIdx, instanceID, state);
    	
    	if(ret != 0) {
    		logger.error("ReadState from db fail, groupidx={}, instanceid={}, ret={}.", groupIdx, instanceID, ret);
            return ret;
    	}
    	
    	logger.info("OK, groupidx={}, InstanceID={}, value={}.", groupIdx, instanceID, state.getAcceptedValue());
    	return ret;
    }
    
    public int getMaxInstanceIDFromLog(int groupIdx, JavaOriTypeWrapper<Long> instanceID) {
    	int ret = this.logStorage.getMaxInstanceID(groupIdx, instanceID);
    	if(ret != 0 && ret != 1) {
    		logger.error("DB.GetMax fail, groupidx={}, ret={}.", groupIdx, ret);
    	} else if(ret == 1) {
    		logger.debug("MaxInstanceID not exist, groupidx={}.", groupIdx);
    	} else {
    		logger.info("OK, MaxInstanceID={}, groupidsx={}.", (instanceID == null ? null : instanceID.getValue()), groupIdx);
    	}
    	return ret;
    }

    public int writeState(WriteOptions writeOptions, int groupIdx, long instanceID, AcceptorStateData state) {
    	byte[] buffer = null;
		try {
			buffer = state.serializeToBytes();
		} catch (SerializeException e) {
			logger.error(e.getMessage(), e);
			return -1;
		}
		
		WriteState writeState = new WriteState();
		byte[] acceptValue = state.getAcceptedValue();
		if (acceptValue != null && acceptValue.length > 0) {
			writeState.setHasPayLoad(true);
		} else {
			writeState.setHasPayLoad(false);
		}
		
    	int ret = this.logStorage.put(writeOptions, groupIdx, instanceID, buffer, writeState);
    	if(ret != 0) {
    		logger.error("DB.Put fail, groupidx={}, bufferlen={}, ret={}.", groupIdx, buffer.length, ret);
    		return ret;
    	}
    	return 0;
    }

    public int readState(int groupIdx, long instanceID, AcceptorStateData state) {
    	JavaOriTypeWrapper<byte[]> bufferWrap = new JavaOriTypeWrapper<byte[]>();
    	int ret = this.logStorage.get(groupIdx, instanceID, bufferWrap);
    	byte[] buffer = bufferWrap.getValue();
    	
    	if(ret != 0 && ret != 1) {
    		logger.error("DB.Get fail, groupidx={}, ret={}.", groupIdx, ret);
    		return ret;
    	} else if(ret == 1) {
    		logger.info("DB.Get not found, groupidx={}, instanceID={}.", groupIdx, instanceID);
    		return 1;
    	}
    	
    	try {
			state.parseFromBytes(buffer, buffer.length);
		} catch (Exception e) {
			logger.error("State.ParseFromArray fail, bufferlen={}.", buffer.length);
			return -1;
		}
    	return 0;
    }
}













