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
import com.wuba.wpaxos.exception.SerializeException;
import com.wuba.wpaxos.proto.SystemVariables;

/**
 * group membership storage
 */
public class SystemVariablesStore {
	private static final Logger logger = LogManager.getLogger(SystemVariablesStore.class);
	
	private LogStorage logStorage;
	
    public SystemVariablesStore(LogStorage logStorage) {
		super();
		this.logStorage = logStorage;
    }
    
	public LogStorage getLogStorage() {
		return logStorage;
	}

	public void setLogStorage(LogStorage logStorage) {
		this.logStorage = logStorage;
	}

	public int write(WriteOptions writeOptions,  int groupIdx, SystemVariables variables) {
		byte[] buf = null;
		try {
			buf = variables.serializeToBytes();
		} catch (SerializeException e) {
			logger.error("System variables serialize failed.", e);
			return -1;
		}
		
		int ret = this.logStorage.setSystemVariables(writeOptions, groupIdx, buf);
		if (ret != 0) {
			logger.error("DB.Put failed, groupIdx {}, bufferlen {} ret {}.", groupIdx, buf.length, ret);
			return ret;
		}
		
		return 0;
	}
    
    public SystemVariables read(int groupIdx) {
    	byte[] buf = this.logStorage.getSystemVariables(groupIdx);
    	if(buf == null || buf.length == 0) return null;
    	
		SystemVariables variables = new SystemVariables();
		try {
			variables.parseFromBytes(buf, buf.length);
		} catch (SerializeException e) {
			logger.error("SystemVariables read error", e);
			return null;
		}
		return variables;	
    }
}
