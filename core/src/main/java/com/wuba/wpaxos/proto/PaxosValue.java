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
 * PaxosValue
 */
public class PaxosValue implements Proto {
	private int smID;
	private byte[] value;
	
	public int getSmID() {
		return smID;
	}
	
	public void setSmID(int smID) {
		this.smID = smID;
	}
	
	public byte[] getValue() {
		return value;
	}
	
	public void setValue(byte[] value) {
		this.value = value;
	}

	@Override
	public byte[] serializeToBytes() throws SerializeException {
		try {
			PaxosProto.PaxosValue.Builder builder = PaxosProto.PaxosValue.newBuilder();
			builder.setSMID(this.smID);
			builder.setValue(ByteString.copyFrom(this.value));
			PaxosProto.PaxosValue paxosValue = builder.build();
			return paxosValue.toByteArray();
		} catch (Exception e) {
			throw new SerializeException("PaxosValue serializeToBytes failed.", e);
		}
	}

	@Override
	public void parseFromBytes(byte[] buf, int len) throws SerializeException {
		try {
			PaxosProto.PaxosValue paxosValue = PaxosProto.PaxosValue.parseFrom(buf);
			this.setSmID(paxosValue.getSMID());
			this.setValue(paxosValue.getValue().toByteArray());
			
		} catch(Exception e) {
			throw new SerializeException("Parse AcceptorStatedata failed.", e);
		}
	}
}
