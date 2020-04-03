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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.wuba.wpaxos.exception.SerializeException;
import com.wuba.wpaxos.utils.ByteConverter;

/**
 * AcceptorStateData proto
 */
public class AcceptorStateData implements Proto {
	
	public static final int HeaderLen = 44;
	
	// 0
	private long instanceID;
	
	// 8
	private long promiseID;
	
	// 16
	private long promiseNodeID;
	
	// 24
	private long acceptedID;
	
	// 32
	private long acceptedNodeID;
	
	// 40
	private int checkSum;
	
	// 44
	private byte[] acceptedValue;
	
	public long getInstanceID() {
		return instanceID;
	}
	
	public void setInstanceID(long instanceID) {
		this.instanceID = instanceID;
	}
	
	public long getPromiseID() {
		return promiseID;
	}
	
	public void setPromiseID(long promiseID) {
		this.promiseID = promiseID;
	}
	
	public long getPromiseNodeID() {
		return promiseNodeID;
	}
	
	public void setPromiseNodeID(long promiseNodeID) {
		this.promiseNodeID = promiseNodeID;
	}
	
	public long getAcceptedID() {
		return acceptedID;
	}

	public void setAcceptedID(long acceptedID) {
		this.acceptedID = acceptedID;
	}

	public long getAcceptedNodeID() {
		return acceptedNodeID;
	}
	
	public void setAcceptedNodeID(long acceptedNodeID) {
		this.acceptedNodeID = acceptedNodeID;
	}
	
	public byte[] getAcceptedValue() {
		return acceptedValue;
	}
	
	public void setAcceptedValue(byte[] acceptedValue) {
		this.acceptedValue = acceptedValue;
	}
	
	public int getCheckSum() {
		return checkSum;
	}
	
	public void setCheckSum(int checkSum) {
		this.checkSum = checkSum;
	}

	@Override
	public byte[] serializeToBytes() throws SerializeException {
		
		ByteArrayOutputStream stream = null;
		try {
			stream = new ByteArrayOutputStream();
			stream.write(ByteConverter.longToBytesLittleEndian(this.instanceID));
			stream.write(ByteConverter.longToBytesLittleEndian(this.promiseID));
			stream.write(ByteConverter.longToBytesLittleEndian(this.promiseNodeID));
			stream.write(ByteConverter.longToBytesLittleEndian(this.acceptedID));
			stream.write(ByteConverter.longToBytesLittleEndian(this.acceptedNodeID));
			stream.write(ByteConverter.intToBytesLittleEndian(this.checkSum));
			if (this.acceptedValue != null && this.acceptedValue.length > 0) {
				stream.write(this.acceptedValue);
			}
			return stream.toByteArray();
		} catch (Exception e) {
			throw new SerializeException("AcceptorStateData serializeToBytes failed.", e);
		} finally {
			if (stream != null) {
				try {
					stream.close();
				} catch (IOException e) {
					throw new SerializeException(e);
				}
			}
		}
	}

	@Override
	public void parseFromBytes(byte[] buf, int len) throws SerializeException {
		try {
			if (buf == null) {
				throw new SerializeException("AcceptorStateData parseFromBytes failed, buf null.");
			}
			
			int index = 0;
			
			long instanceID = ByteConverter.bytesToLongLittleEndian(buf, index);
			index += 8;
			
			long promiseID = ByteConverter.bytesToLongLittleEndian(buf, index);
			index += 8;
			
			long promiseNodeID = ByteConverter.bytesToLongLittleEndian(buf, index);
			index += 8;
			
			long acceptedID = ByteConverter.bytesToLongLittleEndian(buf, index);
			index += 8;
			
			long acceptedNodeID = ByteConverter.bytesToLongLittleEndian(buf, index);
			index += 8;
			
			int checkSum = ByteConverter.bytesToIntLittleEndian(buf, index);
			index += 4;
			
			if (buf.length > HeaderLen) {
				int totalLen = buf.length;
				int bodyLen = totalLen - HeaderLen;
				byte[] bodybytes = new byte[bodyLen];
				System.arraycopy(buf, index, bodybytes, 0, bodyLen);
				index += bodyLen;
				this.setAcceptedValue(bodybytes);
			}
			
			this.setInstanceID(instanceID);
			this.setPromiseID(promiseID);
			this.setPromiseNodeID(promiseNodeID);
			this.setAcceptedID(acceptedID);
			this.setAcceptedNodeID(acceptedNodeID);
			this.setCheckSum(checkSum);
		} catch (Exception e) {
			throw new SerializeException("Parse AcceptorStateData failed.", e);
		}
	}

	@Override
	public String toString() {
		return "AcceptorStateData [instanceID=" + instanceID + ", promiseID=" + promiseID + ", promiseNodeID="
				+ promiseNodeID + ", acceptedID=" + acceptedID + ", acceptedNodeID=" + acceptedNodeID
				+ ", checkSum=" + checkSum + "]";
	}
}
