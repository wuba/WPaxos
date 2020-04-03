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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.Instance;
import com.wuba.wpaxos.comm.MsgTransport;
import com.wuba.wpaxos.comm.breakpoint.Breakpoint;
import com.wuba.wpaxos.comm.enums.MessageSendType;
import com.wuba.wpaxos.comm.enums.MsgCmd;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.proto.CheckpointMsg;
import com.wuba.wpaxos.proto.Header;
import com.wuba.wpaxos.proto.PaxosMsg;

/**
 * 多角色基础类
 */
public abstract class Base {
	private static final Logger logger = LogManager.getLogger(Base.class); 
	public long          instanceID;
	public boolean       isTestMode;
	public MsgTransport  msgTransport;
	public Instance      instance;
	public Config        pConfig;
	
	public Base(Config pConfig, MsgTransport msgTransport, Instance instance) {
		super();
		this.msgTransport = msgTransport;
		this.instance = instance;
		this.pConfig = pConfig;
		this.instanceID = 0;
		this.isTestMode = false;
	}
	
	public long getInstanceID() {
		return instanceID;
	}
	
	public void setInstanceID(long instanceID) {
		this.instanceID = instanceID;
	}
	
	public void newInstance() {
		this.instanceID ++;
		initForNewPaxosInstance();
	}
	
	public void newInstanceFromInstanceId(long instanceId) {
		this.instanceID = instanceId;
		initForNewPaxosInstance();
	}
	
	public abstract void initForNewPaxosInstance();
	
	public byte[] packMsg(PaxosMsg paxosMsg) {
		BaseMsg baseMsg = new BaseMsg();
		baseMsg.setBodyProto(paxosMsg);
		baseMsg.setMsgCmd(MsgCmd.paxosMsg);
		
		int groupdIdx = this.pConfig.getMyGroupIdx();
		baseMsg.setGroupIdx(groupdIdx);
		
		Header header = new Header();
		header.setGid(this.pConfig.getGid());
		header.setRid(0);
		header.setCmdid(baseMsg.getMsgCmd().getValue());
		header.setVersion(1);
		
		baseMsg.setHeaderLen(Header.HEADERLEN);
		baseMsg.setHeader(header);
		
		return packBaseMsg(baseMsg);
	}
	
	public byte[] packCheckpointMsg(CheckpointMsg checkPointMsg) {
		BaseMsg baseMsg = new BaseMsg();
		baseMsg.setBodyProto(checkPointMsg);
		baseMsg.setMsgCmd(MsgCmd.checkpointMsg);
		
		int groupdIdx = this.pConfig.getMyGroupIdx();
		baseMsg.setGroupIdx(groupdIdx);
		
		Header header = new Header();
		header.setGid(this.pConfig.getGid());
		header.setRid(0);
		header.setCmdid(baseMsg.getMsgCmd().getValue());
		header.setVersion(1);
		
		baseMsg.setHeaderLen(Header.HEADERLEN);
		baseMsg.setHeader(header);
		
		return packBaseMsg(baseMsg);
	}
	
	public int getLastChecksum() {
		return this.instance.getLastChecksum();
	}
	
	public byte[] packBaseMsg(BaseMsg baseMsg) {
		return baseMsg.toBytes();
	}
	
	public static BaseMsg unPackBaseMsg(byte[] vBuffer) {
		BaseMsg baseMsg = null;
		try {
			baseMsg = BaseMsg.fromBytes(vBuffer);
		} catch(Exception e) {
			logger.error("unPackBaseMsg error.", e);
		}
		
		return baseMsg;
	}
	
	public void setAsTestMode() {
		this.isTestMode = true;
	}
	
	public int sendMessage(long toNodeID, PaxosMsg paxosMsg, int sendType) {
		if (this.isTestMode) {
			return 0;
		}
		
		Breakpoint.getInstance().getInstanceBP().sendMessage(this.pConfig.getMyGroupIdx(), paxosMsg.getInstanceID());
		
		if (toNodeID == this.pConfig.getMyNodeID()) {
			return this.instance.onReceivePaxosMsg(paxosMsg, false);
		}
		
		byte[] buf = packMsg(paxosMsg);
		if (buf == null) {
			logger.error("packMsg failed.");
			return -1;
		}
		
		return this.msgTransport.sendMessage(this.pConfig.getMyGroupIdx(), toNodeID, buf, sendType);
	}
	
	/*sendType = Message_SendType_UDP*/
	public int sendMessage(long toNodeID, PaxosMsg paxosMsg) {
		 return sendMessage(toNodeID, paxosMsg, MessageSendType.UDP.getValue());
	}
	
	public int broadcastMessage(PaxosMsg paxosMsg, 
			int runSelfFirst/*BroadcastMessage_Type_RunSelf_First*/, 
			int sendType/*default Message_SendType_UDP*/) {
		if (this.isTestMode) {
			return 0;
		}
		
		Breakpoint.getInstance().getInstanceBP().broadcastMessage(this.pConfig.getMyGroupIdx(), paxosMsg.getInstanceID());
		
		if (runSelfFirst == BroadcastMessageType.BroadcastMessage_Type_RunSelf_First.getType()) {
			if (this.instance.onReceivePaxosMsg(paxosMsg, false) != 0) {
				return -1;
			}
		}
		
		byte[] buf = packMsg(paxosMsg);
		if (buf == null) {
			return -1;
		}
		
		int ret = this.msgTransport.broadcastMessage(this.pConfig.getMyGroupIdx(), buf, sendType);
		
		if (runSelfFirst == BroadcastMessageType.BroadcastMessage_Type_RunSelf_Final.getType()) {
			this.instance.onReceivePaxosMsg(paxosMsg, false);
		}
		
		return ret;
	}
	
	public int broadcastMessageToFollower(PaxosMsg paxosMsg, int sendType) {
		byte[] buf = packMsg(paxosMsg);
		if (buf == null) {
			return -1;
		}
		return this.msgTransport.broadcastMessageFollower(this.pConfig.getMyGroupIdx(), buf, sendType);	
	}
	
	/*sendType = Message_SendType_TCP*/
	public int broadcastMessageToFollower(PaxosMsg paxosMsg) {
		return broadcastMessageToFollower(paxosMsg, MessageSendType.TCP.getValue());
	}
	
	public int broadcastMessageToTempNode(PaxosMsg paxosMsg, int sendType) {
		byte[] buf = packMsg(paxosMsg);
		if (buf == null) {
			return -1;
		}
		
		return this.msgTransport.broadcastMessageTempNode(this.pConfig.getMyGroupIdx(), buf, sendType);
	}
	
	/*sendType = Message_SendType_UDP*/
	public int broadcastMessageToTempNode(PaxosMsg paxosMsg) {
		return broadcastMessageToTempNode(paxosMsg, MessageSendType.UDP.getValue());
	}
	
	public int sendMessage(long toNodeID, CheckpointMsg checkpointMsg, int sendType) {
		
		if (toNodeID == this.pConfig.getMyNodeID()) {
			return 0;
		}
		
		byte[] buf = packCheckpointMsg(checkpointMsg);
		if (buf == null) {
			logger.error("packMsg failed.");
			return -1;
		}
		
		return this.msgTransport.sendMessage(this.pConfig.getMyGroupIdx(), toNodeID, buf, sendType);
	}
	
	/*sendType = Message_SendType_TCP*/
	public int sendMessage(long toNodeID, CheckpointMsg checkpointMsg) {
		return sendMessage(toNodeID, checkpointMsg, MessageSendType.TCP.getValue());
	}
}
