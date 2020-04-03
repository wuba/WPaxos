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
 * instance相关断点跟踪
 */
public class InstanceBP {

	private static final Logger logger = LogManager.getLogger(InstanceBP.class);

	public void newInstance(int groupId, long instanceID) {
		logger.debug("InstanceBP newInstance, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void sendMessage(int groupId, long instanceID) {
		logger.debug("InstanceBP sendMessage, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void broadcastMessage(int groupId, long instanceID) {
		logger.debug("InstanceBP broadcastMessage, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onNewValueCommitTimeout(int groupId, long instanceID) {
		logger.debug("InstanceBP onNewValueCommitTimeout, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onReceive(int groupId, long instanceID) {
		logger.debug("InstanceBP onReceive, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onReceiveParseError(int groupId, long instanceID) {
		logger.debug("InstanceBP onReceiveParseError, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onReceivePaxosMsg(int groupId, long instanceID) {
		logger.debug("InstanceBP onReceivePaxosMsg, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onReceivePaxosMsgNodeIDNotValid(int groupId, long instanceID) {
		logger.debug("InstanceBP onReceivePaxosMsgNodeIDNotValid, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onReceivePaxosMsgTypeNotValid(int groupId, long instanceID) {
		logger.debug("InstanceBP onReceivePaxosMsgTypeNotValid, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onReceivePaxosProposerMsgInotsame(int groupId, long instanceID) {
		logger.debug("InstanceBP onReceivePaxosProposerMsgInotsame, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onReceivePaxosAcceptorMsgInotsame(int groupId, long instanceID) {
		logger.debug("InstanceBP onReceivePaxosAcceptorMsgInotsame, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onReceivePaxosAcceptorMsgAddRetry(int groupId, long instanceID) {
		logger.debug("InstanceBP onReceivePaxosAcceptorMsgAddRetry, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onInstanceLearned(int groupId, long instanceID) {
		logger.debug("InstanceBP onInstanceLearned, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onInstanceLearnedNotMyCommit(int groupId, long instanceID) {
		logger.debug("InstanceBP onInstanceLearnedNotMyCommit, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onInstanceLearnedIsMyCommit(int useTimeMs, int groupId, long instanceID) {
		logger.debug("InstanceBP onInstanceLearnedIsMyCommit useTimeMs {}, group : {}, instanceID {}.",useTimeMs, groupId, instanceID);
	}

	public void onInstanceLearnedSMExecuteFail(int groupId, long instanceID) {
		logger.debug("InstanceBP onInstanceLearnedSMExecuteFail, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void checksumLogicFail(int groupId, long instanceID) {
		logger.debug("InstanceBP checksumLogicFail, group : {}, instanceID {}.", groupId, instanceID);
	}

}
