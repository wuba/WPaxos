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

import com.google.protobuf.ByteString;
import com.wuba.wpaxos.exception.SerializeException;
import com.wuba.wpaxos.proto.pb.PaxosProto;

/**
 * BatchPaxosValue proto
 */
public class BatchPaxosValue implements Proto {
	private List<PaxosValue> batchList = new ArrayList<PaxosValue>();

	public int getBatchSize() {
		return batchList.size();
	}
	
	public PaxosValue getByIndex(int idx) {
		if (idx >= batchList.size()) {
			return null; 
		}
		return batchList.get(idx);
	}
	
	public void addPaxosValue(PaxosValue paxosValue) {
		this.batchList.add(paxosValue);
	}
	
	public List<PaxosValue> getBatchList() {
		return batchList;
	}

	public void setBatchList(List<PaxosValue> batchList) {
		this.batchList = batchList;
	}

	@Override
	public byte[] serializeToBytes() throws SerializeException {
		try {
			PaxosProto.BatchPaxosValues.Builder builder = PaxosProto.BatchPaxosValues.newBuilder();
			for (PaxosValue paxosValue : batchList) {
				PaxosProto.PaxosValue.Builder bld = PaxosProto.PaxosValue.newBuilder();
				bld.setSMID(paxosValue.getSmID());
				bld.setValue(ByteString.copyFrom(paxosValue.getValue()));
				PaxosProto.PaxosValue pValue = bld.build();
				builder.addValues(pValue);
			}
			return builder.build().toByteArray();
		} catch(Exception e) {
			throw new SerializeException("BatchPaxosValue serializeToBytes failed.", e);
		}
	}

	@Override
	public void parseFromBytes(byte[] buf, int len) throws SerializeException {
		try {
			PaxosProto.BatchPaxosValues batchPaxosValue = PaxosProto.BatchPaxosValues.parseFrom(buf);
			List<PaxosProto.PaxosValue> pValueList = batchPaxosValue.getValuesList();
			for (PaxosProto.PaxosValue pValue : pValueList) {
				PaxosValue paxosValue = new PaxosValue();
				paxosValue.setSmID(pValue.getSMID());
				paxosValue.setValue(pValue.getValue().toByteArray());
				this.batchList.add(paxosValue);
			}
		} catch(Exception e) {
			throw new SerializeException("Parse BatchPaxosValue failed.", e);
		}
		
	}
}
