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
package com.wuba.wpaxos.comm.enums;

/**
 * paxosMsg协议类型
 */
public enum PaxosMsgType {
	paxosPrepare(1),
	paxosPrepareReply(2),
	paxosAccept(3),
	paxosAcceptReply(4),
	paxosLearnerAskforLearn(5),
	paxosLearnerSendLearnValue(6),
	paxosLearnerProposerSendSuccess(7),
	paxosProposalSendNewValue(8),
	paxosLearnerSendNowInstanceID(9),
	paxosLearnerComfirmAskforLearn(10),
	paxosLearnerSendLearnValueAck(11),
	paxosLearnerAskforCheckpoint(12),
	paxosLearnerOnAskforCheckpoint(13);
	
	private int value;
	
	private PaxosMsgType(int value) {
		this.value = value;
	}

	public int getValue() {
		return value;
	}
}
