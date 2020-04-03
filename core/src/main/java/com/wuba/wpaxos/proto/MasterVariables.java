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
import com.wuba.wpaxos.proto.pb.PaxosProto;

/**
 * MasterVariables proto
 */
public class MasterVariables implements Proto {
	private long masterNodeID;
	private long version;
	private int leaseTime;
	
	public long getMasterNodeID() {
		return masterNodeID;
	}
	
	public void setMasterNodeID(long masterNodeID) {
		this.masterNodeID = masterNodeID;
	}
	
	public long getVersion() {
		return version;
	}
	
	public void setVersion(long version) {
		this.version = version;
	}
	
	public int getLeaseTime() {
		return leaseTime;
	}
	
	public void setLeaseTime(int leaseTime) {
		this.leaseTime = leaseTime;
	}

	@Override
	public byte[] serializeToBytes() throws SerializeException {
		try {
			PaxosProto.MasterVariables.Builder builder = PaxosProto.MasterVariables.newBuilder();
			builder.setMasterNodeid(this.masterNodeID);
			builder.setVersion(this.version);
			builder.setLeaseTime(this.leaseTime);
			PaxosProto.MasterVariables masterVariables = builder.build();
			
			return masterVariables.toByteArray();
		} catch(Exception e) {
			throw new SerializeException("MasterVariables serializeToBytes failed.", e);
		}
	}

	@Override
	public void parseFromBytes(byte[] buf, int len) throws SerializeException {
		try {
			PaxosProto.MasterVariables masterVariables = PaxosProto.MasterVariables.parseFrom(buf);
			this.setMasterNodeID(masterVariables.getMasterNodeid());
			this.setLeaseTime(masterVariables.getLeaseTime());
			this.setVersion(masterVariables.getVersion());
		} catch(Exception e) {
			throw new SerializeException("Parse MasterVariables failed.", e);
		}
	}
}
