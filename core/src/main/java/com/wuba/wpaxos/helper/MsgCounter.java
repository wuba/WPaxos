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
package com.wuba.wpaxos.helper;

import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.config.Config;

/**
 * propose提交过程，集群节点ack状态统计
 */
public class MsgCounter {
	private static final Logger logger = LogManager.getLogger(MsgCounter.class);
	public Config config;
	public Set<Long> receiveMsgNodeID = new HashSet<Long>();
	public Set<Long> rejectMsgNodeID = new HashSet<Long>();
	public Set<Long> promiseOrAcceptMsgNodeID = new HashSet<Long>();

	public MsgCounter(Config config) {
		this.config = config;
		this.startNewRound();
	}

	public void addReceive(Long nodeID) {
		if(!this.receiveMsgNodeID.contains(nodeID)) {
			this.receiveMsgNodeID.add(nodeID);
		}
	}

	public void addReject(long nodeID) {
		if(!this.rejectMsgNodeID.contains(nodeID)) {
			this.rejectMsgNodeID.add(nodeID);
		}
	}

	public void addPromiseOrAccept(long nodeID) {
		if(!this.promiseOrAcceptMsgNodeID.contains(nodeID)) {
			this.promiseOrAcceptMsgNodeID.add(nodeID);
		}
	}

	public boolean isPassedOnThisRound() {
		logger.debug("passedOn size {}.", this.promiseOrAcceptMsgNodeID.size());
		return this.promiseOrAcceptMsgNodeID.size() >= this.config.getMajorityCount();
	}

	public boolean isRejectedOnThisRound() {
		return this.rejectMsgNodeID.size() >= this.config.getMajorityCount();
	}

	public boolean isAllReceiveOnThisRound() {
		return this.receiveMsgNodeID.size() == this.config.getNodeCount();
	}

	public void startNewRound() {
		if(this.receiveMsgNodeID != null) {
			this.receiveMsgNodeID.clear();
		}
		if(this.rejectMsgNodeID != null) {
			this.rejectMsgNodeID.clear();
		}
		if(this.promiseOrAcceptMsgNodeID != null) {
			this.promiseOrAcceptMsgNodeID.clear();
		}
	}
}
