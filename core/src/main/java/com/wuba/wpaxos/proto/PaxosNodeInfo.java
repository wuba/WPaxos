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
 * PaxosNodeInfo proto
 */
public class PaxosNodeInfo implements Proto {
	private long rid;
	private long nodeID;
	
	public long getRid() {
		return rid;
	}
	
	public void setRid(long rid) {
		this.rid = rid;
	}
	
	public long getNodeID() {
		return nodeID;
	}
	
	public void setNodeID(long nodeID) {
		this.nodeID = nodeID;
	}

	@Override
	public byte[] serializeToBytes() throws SerializeException {
		try {
			PaxosProto.PaxosNodeInfo.Builder builder = PaxosProto.PaxosNodeInfo.newBuilder();
			builder.setRid(this.rid);
			builder.setNodeid(this.nodeID);
			PaxosProto.PaxosNodeInfo paxosNodeInfo = builder.build();
			
			return paxosNodeInfo.toByteArray();
		} catch(Exception e) {
			throw new SerializeException("PaxosNodeInfo serializeToBytes failed.", e);
		}
	}

	@Override
	public void parseFromBytes(byte[] buf, int len) throws SerializeException {
		try {
			PaxosProto.PaxosNodeInfo paxosNodeInfo = PaxosProto.PaxosNodeInfo.parseFrom(buf);
			this.setRid(paxosNodeInfo.getRid());
			this.setNodeID(paxosNodeInfo.getNodeid());
		} catch(Exception e) {
			throw new SerializeException("Parse PaxosNodeInfo failed.", e);
		}
	}

	@Override
	public String toString() {
		return "PaxosNodeInfo [rid=" + rid + ", nodeID=" + nodeID + "]";
	}
}
