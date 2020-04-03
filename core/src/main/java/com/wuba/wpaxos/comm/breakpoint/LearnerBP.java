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
 * learner断点日志跟踪
 */
public class LearnerBP {

	private static final Logger logger = LogManager.getLogger(LearnerBP.class);

	public void askforLearn(int groupId) {
		logger.debug("LearnerBP askforLearn, groupId: {}.", groupId);
	}

	public void onAskforLearn(int groupId) {
		logger.debug("LearnerBP onAskforLearn, groupId: {}.", groupId);
	}

	public void onAskforLearnGetLockFail(int groupId) {
		logger.debug("LearnerBP onAskforLearnGetLockFail, groupId: {}.", groupId);
	}

	public void sendNowInstanceID(int groupId) {
		logger.debug("LearnerBP sendNowInstanceID, groupId: {}.", groupId);
	}

	public void onSendNowInstanceID(int groupId) {
		logger.debug("LearnerBP onSendNowInstanceID, groupId: {}.", groupId);
	}

	public void comfirmAskForLearn(int groupId) {
		logger.debug("LearnerBP comfirmAskForLearn, groupId: {}.", groupId);
	}

	public void onComfirmAskForLearn(int groupId) {
		logger.debug("LearnerBP onComfirmAskForLearn, groupId: {}.", groupId);
	}

	public void onComfirmAskForLearnGetLockFail(int groupId) {
		logger.debug("LearnerBP onComfirmAskForLearnGetLockFail, groupId: {}.", groupId);
	}

	public void sendLearnValue(int groupId) {
		logger.debug("LearnerBP sendLearnValue, groupId: {}.", groupId);
	}

	public void onSendLearnValue(int groupId) {
		logger.debug("LearnerBP onSendLearnValue, groupId: " + groupId);
	}

	public void sendLearnValueAck(int groupId) {
		logger.debug("LearnerBP sendLearnValueAck, groupId: " + groupId);
	}

	public void onSendLearnValueAck(int groupId) {
		logger.debug("LearnerBP onSendLearnValueAck, groupId: " + groupId);
	}

	public void proposerSendSuccess(int groupId) {
		logger.debug("LearnerBP proposerSendSuccess, groupId: " + groupId);
	}

	public void onProposerSendSuccess(int groupId) {
		logger.debug("LearnerBP onProposerSendSuccess, groupId : " + groupId);
	}

	public void onProposerSendSuccessNotAcceptYet(int groupId) {
		logger.debug("LearnerBP onProposerSendSuccessNotAcceptYet, groupId: " + groupId);
	}

	public void onProposerSendSuccessBallotNotSame(int groupId) {
		logger.debug("LearnerBP onProposerSendSuccessBallotNotSame, groupId: " + groupId);
	}

	public void onProposerSendSuccessSuccessLearn(int groupId) {
		logger.debug("LearnerBP onProposerSendSuccessSuccessLearn, groupId: " + groupId);
	}

	public void senderAckTimeout(int groupId) {
		logger.debug("LearnerBP senderAckTimeout, groupId: " + groupId);
	}

	public void senderAckDelay(int groupId) {
		logger.debug("LearnerBP senderAckDelay, groupId: " + groupId);
	}

	public void senderSendOnePaxosLog(int groupId) {
		logger.debug("LearnerBP senderSendOnePaxosLog, groupId: " + groupId);
	}

}
