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

import java.util.ArrayList;
import java.util.List;

import com.wuba.wpaxos.exception.SerializeException;
import com.wuba.wpaxos.proto.pb.PaxosProto;

/**
 * SystemVariables proto
 */
public class SystemVariables implements Proto {
	private long gid = 0; /*???*/
	private List<PaxosNodeInfo> memberShips = new ArrayList<PaxosNodeInfo>();
	private long version = -1;
	
	public long getGid() {
		return gid;
	}
	
	public void setGid(long gid) {
		this.gid = gid;
	}
	
	public List<PaxosNodeInfo> getMemberShips() {
		return memberShips;
	}

	public void setMemberShips(List<PaxosNodeInfo> memberShips) {
		this.memberShips = memberShips;
	}
	
	public void addMemberShip(PaxosNodeInfo pNodeInfo) {
		this.memberShips.add(pNodeInfo);
	}
	
	public int getMembershipSize() {
		return this.memberShips.size();
	}
	
	public void clearMembership() {
		this.memberShips.clear();
	}

	public long getVersion() {
		return version;
	}
	
	public void setVersion(long version) {
		this.version = version;
	}

	@Override
	public byte[] serializeToBytes() throws SerializeException {
		try {
			PaxosProto.SystemVariables.Builder builder = PaxosProto.SystemVariables.newBuilder();
			builder.setGid(this.gid);
			builder.setVersion(this.version);
			for (PaxosNodeInfo paxosNodeInfo : this.memberShips) {
				PaxosProto.PaxosNodeInfo.Builder bld = PaxosProto.PaxosNodeInfo.newBuilder();
				bld.setNodeid(paxosNodeInfo.getNodeID());
				bld.setRid(paxosNodeInfo.getRid());
				builder.addMemberShip(bld.build());
			}
			
			PaxosProto.SystemVariables systemVariables = builder.build();
			return systemVariables.toByteArray();
		} catch(Exception e) {
			throw new SerializeException("SystemVariables serializeToBytes failed.", e);
		}
	}

	@Override
	public void parseFromBytes(byte[] buf, int len) throws SerializeException {
		try {
			PaxosProto.SystemVariables systemVariables = PaxosProto.SystemVariables.parseFrom(buf);
			this.setGid(systemVariables.getGid());
			this.setVersion(systemVariables.getVersion());
			for (PaxosProto.PaxosNodeInfo paxosNodeInfo : systemVariables.getMemberShipList()) {
				PaxosNodeInfo pNodeInfo = new PaxosNodeInfo();
				pNodeInfo.setNodeID(paxosNodeInfo.getNodeid());
				pNodeInfo.setRid(paxosNodeInfo.getRid());
				this.memberShips.add(pNodeInfo);
			}
		} catch(Exception e) {
			throw new SerializeException("Parse SystemVariables failed.", e);
		}
	}
	
	@Override
	public SystemVariables clone() {
		SystemVariables systemVariables = new SystemVariables();
		systemVariables.setGid(this.gid);
		systemVariables.setVersion(this.version);
		List<PaxosNodeInfo> memberShips = new ArrayList<PaxosNodeInfo>();
		memberShips.addAll(this.getMemberShips());
		systemVariables.setMemberShips(memberShips);
		
		return systemVariables;
	}

	@Override
	public String toString() {
		return "SystemVariables [gid=" + gid + ", memberShips=" + memberShips + ", version=" + version + "]";
	}
	
}
