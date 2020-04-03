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
package com.wuba.wpaxos.base;

/**
 * global unique proposalID
 */
public class BallotNumber {
	private long proposalID = 0;
	private long nodeId = 0;
	
	public BallotNumber(long proposalID, long nodeId) {
		super();
		this.proposalID = proposalID;
		this.nodeId = nodeId;
	}
	
	public boolean ge(BallotNumber other) {
		if (this.proposalID == other.getProposalID()) {
			return this.nodeId >= other.getNodeId();
		} else {
			return this.proposalID >= other.getProposalID();
		}
	}
	
	public boolean gt(BallotNumber other) {
		if (this.proposalID == other.getProposalID()) {
			return this.nodeId > other.getNodeId();
		} else {
			return this.proposalID > other.getProposalID();
		}
	}
	
	public void reset() {
		this.proposalID = 0;
		this.nodeId = 0;
	}
	
	public boolean isNull() {
		return this.proposalID == 0;
	}
	
	public boolean unequals(Object obj) {
		return !equals(obj);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BallotNumber other = (BallotNumber) obj;
		if (nodeId != other.nodeId)
			return false;
		if (proposalID != other.proposalID)
			return false;
		return true;
	}

	public long getProposalID() {
		return proposalID;
	}
	
	public void setProposalID(long proposalID) {
		this.proposalID = proposalID;
	}
	
	public long getNodeId() {
		return nodeId;
	}
	
	public void setNodeId(long nodeId) {
		this.nodeId = nodeId;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (nodeId ^ (nodeId >>> 32));
		result = prime * result + (int) (proposalID ^ (proposalID >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "BallotNumber [proposalID=" + proposalID + ", nodeId=" + nodeId + "]";
	}
}
