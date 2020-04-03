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
 * proposer断点日志跟踪
 */
public class ProposerBP {

	private static final Logger logger = LogManager.getLogger(ProposerBP.class);

	public void newProposal(String value, int groupId, long instanceID) {
		logger.debug("ProposerBP newProposal value length {}, group : {}, instaceID {}.", (value == null ? 0 : value.length()), groupId, instanceID);
	}

	public void newProposalSkipPrepare(int groupId, long instanceID) {
		logger.debug("ProposerBP newProposalSkipPrepare, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void prepare(int groupId, long instanceID) {
		logger.debug("ProposerBP prepare, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onPrepareReply(int groupId, long instanceID) {
		logger.debug("ProposerBP onPrepareReply, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onPrepareReplyButNotPreparing(int groupId, long instanceID) {
		logger.debug("ProposerBP onPrepareReplyButNotPreparing, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onPrepareReplyNotSameProposalIDMsg(int groupId, long instanceID) {
		logger.debug("ProposerBP onPrepareReplyNotSameProposalIDMsg, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void preparePass(int useTimeMs, int groupId, long instanceID) {
		logger.debug("ProposerBP preparePass useTimeMs {}, group : {}, instanceID {}. ", useTimeMs, groupId, instanceID);
	}

	public void prepareNotPass(int groupId, long instanceID) {
		logger.debug("ProposerBP prepareNotPass, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void accept(int groupId, long instanceID) {
		logger.debug("ProposerBP accept, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onAcceptReply(int groupId, long instanceID) {
		logger.debug("ProposerBP onAcceptReply, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onAcceptReplyButNotAccepting(int groupId, long instanceID) {
		logger.debug("ProposerBP onAcceptReplyButNotAccepting, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void onAcceptReplyNotSameProposalIDMsg(int groupId, long instanceID) {
		logger.debug("ProposerBP onAcceptReplyNotSameProposalIDMsg, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void acceptPass(int useTimeMs, int groupId, long instanceID) {
		logger.debug("ProposerBP acceptPass useTimeMs {}, group : {}, instanceID {}.", useTimeMs, groupId, instanceID);
	}

	public void acceptNotPass(int groupId, long instanceID) {
		logger.debug("ProposerBP acceptNotPass, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void prepareTimeout(int groupId, long instanceID) {
		logger.debug("ProposerBP prepareTimeout, group : {}, instanceID {}.", groupId, instanceID);
	}

	public void acceptTimeout(int groupId, long instanceID) {
		logger.debug("ProposerBP acceptTimeout, group : {}, instanceID {}.", groupId, instanceID);
	}

}
