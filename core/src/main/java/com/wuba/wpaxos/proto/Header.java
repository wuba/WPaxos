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
 * Proto header
 */
public class Header implements Proto {
	public static final int HEADERLEN = 24;
	
	private long gid;
	
	private long rid;
	
	private int cmdid;
	
	private int version;

	public long getGid() {
		return gid;
	}

	public void setGid(long gid) {
		this.gid = gid;
	}

	public long getRid() {
		return rid;
	}

	public void setRid(long rid) {
		this.rid = rid;
	}

	public int getCmdid() {
		return cmdid;
	}

	public void setCmdid(int cmdid) {
		this.cmdid = cmdid;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	@Override
	public byte[] serializeToBytes() throws SerializeException {
		try {
			PaxosProto.Header.Builder builder = PaxosProto.Header.newBuilder();
			builder.setGid(this.gid);
			builder.setRid(this.rid);
			builder.setCmdid(this.cmdid);
			builder.setVersion(this.version);
			PaxosProto.Header header = builder.build();
			return header.toByteArray();
		} catch (Exception e) {
			throw new SerializeException("Header serializeToBytes failed.", e);
		}
	}

	@Override
	public void parseFromBytes(byte[] buf, int len) throws SerializeException {
		try {
			PaxosProto.Header header = PaxosProto.Header.parseFrom(buf);
			this.setGid(header.getGid());
			this.setRid(header.getRid());
			this.setCmdid(header.getCmdid());
			this.setVersion(header.getVersion());
		} catch(Exception e) {
			throw new SerializeException("Parse Header failed.", e);
		}
	}
}
