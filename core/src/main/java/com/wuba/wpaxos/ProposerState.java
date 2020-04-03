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
package com.wuba.wpaxos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.base.BallotNumber;
import com.wuba.wpaxos.config.Config;

/**
 * proposer current state
 */
public class ProposerState {
	private static final Logger logger = LogManager.getLogger(ProposerState.class); 
	private long proposalID;
	private long highestOtherProposalID;
	private byte[] value;
	private BallotNumber highestOtherPreAcceptBallot = new BallotNumber(0, 0);
	private Config config;

	public ProposerState(Config config) {
		this.config = config;
		this.proposalID = 1;
		init();
	}

	public void init() {
		this.highestOtherProposalID = 0;
		this.value = new byte[]{};
	}

	public void setStartProposalID(long proposalID) {
		this.proposalID = proposalID;
	}

	public void newPrepare() {
		logger.info("START ProposalID {} HighestOther {} MyNodeID {}.", this.proposalID, this.highestOtherProposalID, this.config.getMyNodeID());
	
		long maxProposalId = this.proposalID > this.highestOtherProposalID ? this.proposalID : this.highestOtherProposalID;
		this.proposalID = maxProposalId + 1;
	}

	public void addPreAcceptValue(BallotNumber otherPreAcceptBallot, byte[] otherPreAcceptValue) {
		if(otherPreAcceptBallot.isNull()) {
			return ;
		}
		
		if (otherPreAcceptBallot.gt(this.highestOtherPreAcceptBallot)) {
			this.highestOtherPreAcceptBallot = otherPreAcceptBallot;
			this.value = otherPreAcceptValue;
		}
	}

	public long getProposalID() {
		return proposalID;
	}

	public byte[] getValue() {
		return this.value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public void setOtherProposalID(long otherProposalID) {
		if(otherProposalID > this.highestOtherProposalID) {
			this.highestOtherProposalID = otherProposalID;
		}
	}

	public void resetHighestOtherPreAcceptBallot() {
		this.highestOtherPreAcceptBallot.reset();
	}
}
