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
package com.wuba.wpaxos.master;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.exception.SerializeException;
import com.wuba.wpaxos.proto.MasterVariables;
import com.wuba.wpaxos.store.LogStorage;

/**
 * master info persist
 */
public class MasterVariablesStore {
	private final Logger logger = LogManager.getLogger(MasterVariablesStore.class); 
	private LogStorage logStorage;

	public MasterVariablesStore(LogStorage logStorage) {
		super();
		this.logStorage = logStorage;
	}
	
	public int write(WriteOptions writeOptions, int groupIdx, MasterVariables variables) {
		
		byte[] buffer = null;
		try {
			buffer = variables.serializeToBytes();
		} catch (SerializeException e) {
			logger.error("MasterVariables serialize error.", e);
		}
		if (buffer == null) {
			logger.error("Variables serialize failed.");
			return -1;
		}
		int ret = logStorage.setMasterVariables(writeOptions, groupIdx, buffer);
		if (ret != 0) {
			logger.error("DB put failed, groupidx {}, bufferLen {} ret {}.", groupIdx, buffer.length, ret);
			return ret;
		}
		
		return 0;
	}
	
	public MasterVariables read(int groupIdx) {
		
		byte[] buffer = logStorage.getMasterVariables(groupIdx);
		if (buffer == null) {
			logger.error("DB get failed, groupIdx {}.", groupIdx);
			return null;
		}
		
		MasterVariables variables = new MasterVariables();
		try {
			variables.parseFromBytes(buffer, buffer.length);
		} catch(Exception e) {
			logger.error("MasterVariables parse error.", e);
			return null;
		}
		
		return variables;
	}
}
