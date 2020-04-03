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
import com.wuba.wpaxos.exception.SerializeException;
import com.wuba.wpaxos.proto.pb.PaxosProto;

/**
 * CheckpointMsg proto
 */
public class CheckpointMsg implements Proto {
	private int msgType;
	private long nodeID;
	private int flag;
	private long uuid;
	private long sequenceID;
	private long checkpointInstanceID;
	private int checksum;
	private String filePath;
	private int smID;
	private long offset;
	private byte[] buffer;
	
	public int getMsgType() {
		return msgType;
	}
	
	public void setMsgType(int msgType) {
		this.msgType = msgType;
	}
	
	public long getNodeID() {
		return nodeID;
	}
	
	public void setNodeID(long nodeID) {
		this.nodeID = nodeID;
	}
	
	public int getFlag() {
		return flag;
	}
	
	public void setFlag(int flag) {
		this.flag = flag;
	}
	
	public long getUuid() {
		return uuid;
	}
	
	public void setUuid(long uuid) {
		this.uuid = uuid;
	}
	
	public long getSequenceID() {
		return sequenceID;
	}
	
	public void setSequenceID(long sequenceID) {
		this.sequenceID = sequenceID;
	}
	
	public long getCheckpointInstanceID() {
		return checkpointInstanceID;
	}
	
	public void setCheckpointInstanceID(long checkpointInstanceID) {
		this.checkpointInstanceID = checkpointInstanceID;
	}
	
	public int getChecksum() {
		return checksum;
	}
	
	public void setChecksum(int checksum) {
		this.checksum = checksum;
	}
	
	public String getFilePath() {
		return filePath;
	}
	
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	
	public int getSmID() {
		return smID;
	}
	
	public void setSmID(int smID) {
		this.smID = smID;
	}
	
	public long getOffset() {
		return offset;
	}
	
	public void setOffset(long offset) {
		this.offset = offset;
	}
	
	public byte[] getBuffer() {
		return buffer;
	}
	
	public void setBuffer(byte[] buffer) {
		this.buffer = buffer;
	}

	@Override
	public byte[] serializeToBytes() throws SerializeException {
		try {
			PaxosProto.CheckpointMsg.Builder builder = PaxosProto.CheckpointMsg.newBuilder();
			builder.setMsgType(this.msgType);
			builder.setNodeID(this.nodeID);
			builder.setFlag(this.flag);
			builder.setUUID(this.uuid);
			builder.setSequence(this.sequenceID);
			builder.setCheckpointInstanceID(this.checkpointInstanceID);
			builder.setChecksum(this.checksum);
			if(this.filePath != null) {
				builder.setFilePath(this.filePath);
			}
			builder.setSMID(this.smID);
			builder.setOffset(this.offset);
			
			if(this.getBuffer() != null) {
				builder.setBuffer(ByteString.copyFrom(this.getBuffer()));
			}
			PaxosProto.CheckpointMsg checkpointMsg = builder.build();
			
			return checkpointMsg.toByteArray();
		} catch(Exception e) {
			throw new SerializeException("CheckpointMsg serializeToBytes failed.", e);
		}
	}

	@Override
	public void parseFromBytes(byte[] buf, int len) throws SerializeException {
		try {
			PaxosProto.CheckpointMsg checkpointMsg = PaxosProto.CheckpointMsg.parseFrom(buf);
			this.setMsgType(checkpointMsg.getMsgType());
			this.setNodeID(checkpointMsg.getNodeID());
			this.setFlag(checkpointMsg.getFlag());
			this.setUuid(checkpointMsg.getUUID());
			this.setSequenceID(checkpointMsg.getSequence());
			this.setCheckpointInstanceID(checkpointMsg.getCheckpointInstanceID());
			this.setChecksum(checkpointMsg.getChecksum());
			this.setFilePath(checkpointMsg.getFilePath());
			this.setSmID(checkpointMsg.getSMID());
			this.setOffset(checkpointMsg.getOffset());
			this.setBuffer(checkpointMsg.getBuffer().toByteArray());
		} catch(Exception e) {
			throw new SerializeException("Parse CheckpointMsg failed.", e);
		}
	}
}
