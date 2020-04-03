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
package com.wuba.wpaxos.proto;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.wuba.wpaxos.exception.SerializeException;
import com.wuba.wpaxos.proto.pb.PaxosProto;

/**
 * PaxosMsg proto
 */
public class PaxosMsg implements Proto {
	private int msgType;
	private long instanceID;
	private long nodeID;
	private long proposalID;
	private long proposalNodeID;
	private long preAcceptID;
	private long preAcceptNodeID;
	private long rejectByPromiseID;
	private long nowInstanceID;
	private long minChosenInstanceID;
	private int lastChecksum;
	private int flag;
	private long timestamp;
	private byte[] systemVariables;
	private byte[] masterVariables;
	private byte[] value;
	
	public PaxosMsg() {
		super();
		this.timestamp = System.currentTimeMillis();
	}
	
	public int getMsgType() {
		return msgType;
	}
	
	public void setMsgType(int msgType) {
		this.msgType = msgType;
	}
	
	public long getInstanceID() {
		return instanceID;
	}
	
	public void setInstanceID(long instanceID) {
		this.instanceID = instanceID;
	}
	
	public long getNodeID() {
		return nodeID;
	}
	
	public void setNodeID(long nodeID) {
		this.nodeID = nodeID;
	}
	
	public long getProposalID() {
		return proposalID;
	}
	
	public void setProposalID(long proposalID) {
		this.proposalID = proposalID;
	}
	
	public long getProposalNodeID() {
		return proposalNodeID;
	}
	
	public void setProposalNodeID(long proposalNodeID) {
		this.proposalNodeID = proposalNodeID;
	}
	
	public byte[] getValue() {
		return value;
	}
	
	public void setValue(byte[] value) {
		this.value = value;
	}
	
	public long getPreAcceptID() {
		return preAcceptID;
	}
	
	public void setPreAcceptID(long preAcceptID) {
		this.preAcceptID = preAcceptID;
	}
	
	public long getPreAcceptNodeID() {
		return preAcceptNodeID;
	}
	
	public void setPreAcceptNodeID(long preAcceptNodeID) {
		this.preAcceptNodeID = preAcceptNodeID;
	}
	
	public long getRejectByPromiseID() {
		return rejectByPromiseID;
	}
	
	public void setRejectByPromiseID(long rejectByPromiseID) {
		this.rejectByPromiseID = rejectByPromiseID;
	}
	
	public long getNowInstanceID() {
		return nowInstanceID;
	}
	
	public void setNowInstanceID(long nowInstanceID) {
		this.nowInstanceID = nowInstanceID;
	}
	
	public long getMinChosenInstanceID() {
		return minChosenInstanceID;
	}
	
	public void setMinchosenInstanceID(long minChosenInstanceID) {
		this.minChosenInstanceID = minChosenInstanceID;
	}
	
	public int getLastChecksum() {
		return lastChecksum;
	}
	
	public void setLastChecksum(int lastChecksum) {
		this.lastChecksum = lastChecksum;
	}
	
	public int getFlag() {
		return flag;
	}
	
	public void setFlag(int flag) {
		this.flag = flag;
	}
	
	public byte[] getSystemVariables() {
		return systemVariables;
	}
	
	public void setSystemVariables(byte[] systemVariables) {
		this.systemVariables = systemVariables;
	}
	
	public byte[] getMasterVariables() {
		return masterVariables;
	}
	
	public void setMasterVariables(byte[] masterVariables) {
		this.masterVariables = masterVariables;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public byte[] serializeToBytes() throws SerializeException {
		try {
			PaxosProto.PaxosMsg.Builder builder = PaxosProto.PaxosMsg.newBuilder();
			builder.setMsgType(this.msgType);
			builder.setInstanceID(this.instanceID);
			builder.setNodeID(this.nodeID);
			builder.setProposalID(this.proposalID);
			builder.setProposalNodeID(this.proposalNodeID);
			if (this.value != null) {
				builder.setValue(ByteString.copyFrom(this.value));
			}
			builder.setPreAcceptID(this.preAcceptID);
			builder.setPreAcceptNodeID(this.preAcceptNodeID);
			builder.setRejectByPromiseID(this.rejectByPromiseID);
			builder.setNowInstanceID(this.nowInstanceID);
			builder.setMinChosenInstanceID(this.minChosenInstanceID);
			builder.setLastChecksum(this.lastChecksum);
			builder.setFlag(this.flag);
			builder.setTimeStamp(this.timestamp);
			if (null != this.systemVariables) {
				builder.setSystemVariables(ByteString.copyFrom(this.systemVariables));
			}
			
			if (null != this.masterVariables) {
				builder.setMasterVariables(ByteString.copyFrom(this.masterVariables));
			}
			PaxosProto.PaxosMsg paxosMsg = builder.build();
			return paxosMsg.toByteArray();
		} catch (Exception e) {
			throw new SerializeException("PaxosMsg serializeToBytes failed.", e);
		}
	}

	@Override
	public void parseFromBytes(byte[] buf, int len) throws SerializeException {
		try {
			PaxosProto.PaxosMsg paxosMsg = PaxosProto.PaxosMsg.parseFrom(buf);
			this.setMsgType(paxosMsg.getMsgType());
			this.setInstanceID(paxosMsg.getInstanceID());
			this.setNodeID(paxosMsg.getNodeID());
			this.setProposalID(paxosMsg.getProposalID());
			this.setProposalNodeID(paxosMsg.getProposalNodeID());
			this.setValue(paxosMsg.getValue().toByteArray());
			this.setPreAcceptID(paxosMsg.getPreAcceptID());
			this.setPreAcceptNodeID(paxosMsg.getPreAcceptNodeID());
			this.setRejectByPromiseID(paxosMsg.getRejectByPromiseID());
			this.setNowInstanceID(paxosMsg.getNowInstanceID());
			this.setMinchosenInstanceID(paxosMsg.getMinChosenInstanceID());
			this.setLastChecksum(paxosMsg.getLastChecksum());
			this.setFlag(paxosMsg.getFlag());
			this.setTimestamp(paxosMsg.getTimeStamp());
			this.setSystemVariables(paxosMsg.getSystemVariables().toByteArray());
			this.setMasterVariables(paxosMsg.getMasterVariables().toByteArray());
		} catch (InvalidProtocolBufferException e) {
			throw new SerializeException("Parse PaxosMsg failed.", e);
		}
	}
}
