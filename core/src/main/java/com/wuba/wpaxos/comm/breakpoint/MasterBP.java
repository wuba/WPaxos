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
 * master相关日志跟踪
 */
public class MasterBP {

	private static final Logger logger = LogManager.getLogger(MasterBP.class);

	public void tryBeMaster(int groupId) {
		logger.debug("MasterBP tryBeMaster, group : {}.", groupId);
	}

	public void tryBeMasterProposeFail(int groupId) {
		logger.debug("MasterBP tryBeMasterProposeFail, group : {},", groupId);
	}

	public void successBeMaster(int groupId) {
		logger.debug("MasterBP successBeMaster, group : {}.", groupId);
	}

	public void otherBeMaster(int groupId) {
		logger.debug("MasterBP otherBeMaster, group : {}.", groupId);
	}

	public void dropMaster(int groupId) {
		logger.debug("MasterBP dropMaster, group : {}.", groupId);
	}

	public void toBeMaster(int groupId) {
		logger.debug("MasterBP toBeMaster, group : {}.", groupId);
	}

	public void toBeMasterFail(int groupId) {
		logger.debug("MasterBP toBeMaster Fail, group : {}.", groupId);
	}

	public void masterSMInconsistent(int groupId) {
		logger.debug("MasterBP masterSMInconsistent, group : {}.", groupId);
	}

}
