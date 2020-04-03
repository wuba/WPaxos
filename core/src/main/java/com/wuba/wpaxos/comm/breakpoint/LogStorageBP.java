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
package com.wuba.wpaxos.comm.breakpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 数据存储断点日志跟踪
 */
public class LogStorageBP {

	private static final Logger logger = LogManager.getLogger(LogStorageBP.class);

	public void fileIDToValueFail(int groupId, long instanceID) {
		logger.debug("LogStorageBP fileIDToValueFail, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void valueToFileIDFail(int groupId, long instanceID) {
		logger.debug("LogStorageBP valueToFileIDFail, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void indexDBPutFail(int groupId, long instanceID) {
		logger.debug("LogStorageBP indexDBPutFail, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void indexDBPutOK(int useTimeMs, int groupId, long instanceID) {
		logger.debug("LogStorageBP indexDBPutOK useTimeMs : {}, group : {}, instanceID {}.", useTimeMs, groupId, instanceID);
	}

	public void appendDataFail(int groupId, long instanceID) {
		logger.debug("LogStorageBP appendDataFail, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void appendDataOK(int writeLen, int useTimeMs, int groupId, long instanceID) {
		logger.debug("LogStorageBP appendDataOK writeLen : {}, useTimeMs : {}, group : {}, instanceID {}.", writeLen, useTimeMs, groupId, instanceID);
	}

	public void getFileChecksumNotEquel(int groupId, long instanceID) {
		logger.debug("LogStorageBP getFileChecksumNotEquel, group : {}, instanceID {}.", groupId, instanceID);
	}

}
