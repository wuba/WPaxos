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
package com.wuba.wpaxos.base;

import java.io.ByteArrayOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.comm.enums.MsgCmd;
import com.wuba.wpaxos.proto.CheckpointMsg;
import com.wuba.wpaxos.proto.Header;
import com.wuba.wpaxos.proto.PaxosMsg;
import com.wuba.wpaxos.proto.Proto;
import com.wuba.wpaxos.utils.ByteConverter;
import com.wuba.wpaxos.utils.Crc32;

/**
 * 协议基础类
 */
public class BaseMsg {
	private static final Logger logger = LogManager.getLogger(BaseMsg.class); 
	private MsgCmd msgCmd;	
	private int totalLen;	
	private int groupIdx;	
	private int headerLen;	
	private Header header;	
	private int bodyLen;
	private Proto bodyProto;
	private int checkSum;
	public static final int GROUPIDX_OFFSET = 4;
	
	public int getTotalLen() {
		return totalLen;
	}

	public void setTotalLen(int totalLen) {
		this.totalLen = totalLen;
	}

	public MsgCmd getMsgCmd() {
		return msgCmd;
	}

	public void setMsgCmd(MsgCmd msgCmd) {
		this.msgCmd = msgCmd;
	}

	public int getGroupIdx() {
		return groupIdx;
	}
	
	public void setGroupIdx(int groupIdx) {
		this.groupIdx = groupIdx;
	}
	
	public int getHeaderLen() {
		return headerLen;
	}
	
	public void setHeaderLen(int headerLen) {
		this.headerLen = headerLen;
	}
	
	public Header getHeader() {
		return header;
	}
	
	public void setHeader(Header header) {
		this.header = header;
	}
	
	public Proto getBodyProto() {
		return bodyProto;
	}
	
	public void setBodyProto(Proto bodyProto) {
		this.bodyProto = bodyProto;
	}
	
	public int getCheckSum() {
		return checkSum;
	}
	
	public void setCheckSum(int checkSum) {
		this.checkSum = checkSum;
	}
	
	public int getBodyLen() {
		return bodyLen;
	}

	public void setBodyLen(int bodyLen) {
		this.bodyLen = bodyLen;
	}

	public byte[] toBytes() {
		ByteArrayOutputStream stream = null;
		try {
			byte[] headerBuf = this.header.serializeToBytes();
			byte[] bodyBuf = this.bodyProto.serializeToBytes();
			
			int groupIdxLen = 4;
			int headerLenLen = 4;
			int headerLen = headerBuf.length;
			int bodyLenLen = 4;
			int bodyLen = bodyBuf.length;
			int checksumLen = 4;
			this.totalLen = groupIdxLen + headerLenLen + headerLen + bodyLenLen + bodyLen + checksumLen;
			
			stream = new ByteArrayOutputStream();
			stream.write(ByteConverter.intToBytesLittleEndian(this.totalLen));
			stream.write(ByteConverter.intToBytesLittleEndian(this.groupIdx));
			stream.write(ByteConverter.intToBytesLittleEndian(headerBuf.length));
			stream.write(headerBuf);
			stream.write(ByteConverter.intToBytesLittleEndian(bodyBuf.length));
			stream.write(bodyBuf);
			int crc32 = Crc32.crc32(bodyBuf);
			stream.write(ByteConverter.intToByte(crc32));
			
			return stream.toByteArray();
		} catch (Exception e) {
			logger.error("BaseMsg toBytes error", e);
		}
		
		return null;
	}
	
	public static BaseMsg fromBytes(byte[] buf) {
		try {
			int index = 0;
			
			int totalLen = ByteConverter.bytesToIntLittleEndian(buf, index);
			index += 4;
			
			int groupIdx = ByteConverter.bytesToIntLittleEndian(buf, index);
			index += 4;
			
			int headerLen = ByteConverter.bytesToIntLittleEndian(buf, index);
			index += 4;
			
			byte[] headerBuf = new byte[headerLen];
			if (headerLen > 0) {
				System.arraycopy(buf, index, headerBuf, 0, headerLen);
			}
			index += headerLen;
			
			int bodyLen = ByteConverter.bytesToIntLittleEndian(buf, index);
			index += 4;
			
			byte[] body = new byte[bodyLen];
			if (bodyLen > 0) {
				System.arraycopy(buf, index, body, 0, bodyLen);
			}
			index += bodyLen;
			
			int checkSum = ByteConverter.bytesToIntLittleEndian(buf, index);
			index += 4;
			
			BaseMsg baseMsg = new BaseMsg();
			baseMsg.setTotalLen(totalLen);
			baseMsg.setGroupIdx(groupIdx);
			baseMsg.setHeaderLen(headerLen);
			Header header = new Header();
			header.parseFromBytes(headerBuf, headerBuf.length);
			baseMsg.setHeader(header);
			baseMsg.setBodyLen(bodyLen);
			baseMsg.setCheckSum(checkSum);
			
		    switch(header.getCmdid()) {
		    case 1:
		    	baseMsg.setMsgCmd(MsgCmd.paxosMsg);
		    	PaxosMsg paxosMsg = new PaxosMsg();
		    	paxosMsg.parseFromBytes(body, bodyLen);
		    	baseMsg.setBodyProto(paxosMsg);
		    	break;
		    case 2:
		    	baseMsg.setMsgCmd(MsgCmd.checkpointMsg);
		    	CheckpointMsg checkpointMsg = new CheckpointMsg();
		    	checkpointMsg.parseFromBytes(body, bodyLen);
		    	baseMsg.setBodyProto(checkpointMsg);
		    	break;
		    default:
		    	logger.error("BaseMsg frombytes failed.");
		    	break;
		    }
		    
		    return baseMsg;
		} catch (Exception e) {
			logger.error("BaseMsg fromBytes error.", e);
		}
		
		return null;
	}
}
