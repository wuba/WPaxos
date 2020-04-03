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
 * commiter断点执行日志跟踪
 */
public class CommiterBP {

	private static final Logger logger = LogManager.getLogger(CommiterBP.class);

	public void newValue(int groupId) {
		logger.debug("CommiterBP newValue, group : {}.", groupId);
	}

	public void newValueConflict(int groupId, long instanceID) {
		logger.debug("CommiterBP newValueConflict, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void newValueGetLockTimeout(int groupId) {
		logger.debug("CommiterBP newValueGetLockTimeout, group : {}.", groupId);
	}

	public void newValueGetLockReject(int groupId) {
		logger.debug("CommiterBP newValueGetLockReject, group : {}.", groupId);
	}

	public void newValueGetLockOK(int useTimeMs, int groupId, long instanceID) {
		logger.debug("CommiterBP newValueGetLockOK useTimeMs : {}, groupId : {}, instanceID ：{}.",useTimeMs, groupId,instanceID);
	}

	public void newValueCommitOK(int useTimeMs, int groupId) {
		logger.debug("CommiterBP newValueCommitOK useTimeMs {}, group {}.",useTimeMs, groupId);
	}

	public void newValueCommitFail(int groupId, long instanceID) {
		logger.debug("CommiterBP newValueCommitFail, group : {}, instanceID ：{}.", groupId, instanceID);
	}

	public void batchPropose(int groupId) {
		logger.debug("CommiterBP batchPropose, group : {}.", groupId);
	}

	public void batchProposeOK(int groupId) {
		logger.debug("CommiterBP batchProposeOK, group : {}.", groupId);
	}

	public void batchProposeFail(int groupId) {
		logger.debug("CommiterBP batchProposeFail, group : {}.", groupId);
	}

	public void batchProposeWaitTimeMs(int waitTimeMs, int groupId) {
		logger.debug("CommiterBP batchProposeWaitTimeMs waitTimeMs {} , groupId : {}.", waitTimeMs, groupId);
	}

	public void batchProposeDoPropose(int batchCount, int groupId) {
		logger.debug("CommiterBP batchProposeDoPropose batchCount {}, groupId : {}.", batchCount, groupId);
	}

}
