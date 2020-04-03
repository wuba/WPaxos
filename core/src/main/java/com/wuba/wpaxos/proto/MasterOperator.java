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

import com.wuba.wpaxos.exception.SerializeException;
import com.wuba.wpaxos.proto.pb.MasterSmProto;

/**
 * MasterOperator proto
 */
public class MasterOperator implements Proto  {
	private long nodeID;
	private long version;
	private int timeout;
	private int operator;
	private int sid;
	private long lastversion;
	
	public long getNodeID() {
		return nodeID;
	}

	public void setNodeID(long nodeID) {
		this.nodeID = nodeID;
	}

	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public int getOperator() {
		return operator;
	}

	public void setOperator(int operator) {
		this.operator = operator;
	}

	public int getSid() {
		return sid;
	}

	public void setSid(int sid) {
		this.sid = sid;
	}

	public long getLastversion() {
		return lastversion;
	}

	public void setLastversion(long lastversion) {
		this.lastversion = lastversion;
	}

	@Override
	public byte[] serializeToBytes() throws SerializeException {
		try {
			MasterSmProto.MasterOperator.Builder builder = MasterSmProto.MasterOperator.newBuilder();
			builder.setNodeid(this.nodeID);
			builder.setVersion(this.version);
			builder.setTimeout(this.timeout);
			builder.setOperator(this.operator);
			builder.setSid(this.sid);
			builder.setLastversion(this.lastversion);
			MasterSmProto.MasterOperator masterOperator = builder.build();
			
			return masterOperator.toByteArray();
		} catch(Exception e) {
			throw new SerializeException("MasterOperator serializeToBytes failed.", e);
		}
	}

	@Override
	public void parseFromBytes(byte[] buf, int len) throws SerializeException {
		try {
			MasterSmProto.MasterOperator masterOperator = MasterSmProto.MasterOperator.parseFrom(buf);
			this.setNodeID(masterOperator.getNodeid());
			this.setVersion(masterOperator.getVersion());
			this.setTimeout(masterOperator.getTimeout());
			this.setOperator(masterOperator.getOperator());
			this.setSid(masterOperator.getSid());
			this.setLastversion(masterOperator.getLastversion());
		} catch(Exception e) {
			throw new SerializeException("Parse MasterOperator failed.", e);
		}
	}
}
