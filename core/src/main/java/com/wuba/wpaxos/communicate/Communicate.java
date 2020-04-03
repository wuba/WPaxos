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
package com.wuba.wpaxos.communicate;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.comm.InsideOptions;
import com.wuba.wpaxos.comm.MsgTransport;
import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.comm.breakpoint.Breakpoint;
import com.wuba.wpaxos.comm.enums.MessageSendType;
import com.wuba.wpaxos.config.Config;

/**
 * 数据发送封装
 */
public class Communicate implements MsgTransport {
	private final Logger logger = LogManager.getLogger(Communicate.class); 
	private Config config;
	private NetWork network;
	private long myNodeID;
	private int UDPMaxSize = 4096; //TODO
	
	public Communicate(Config config, NetWork network, long myNodeID, int uDPMaxSize) {
		super();
		this.config = config;
		this.network = network;
		this.myNodeID = myNodeID;
		UDPMaxSize = uDPMaxSize;
	}

	public int getUDPMaxSize() {
		return UDPMaxSize;
	}

	public void setUDPMaxSize(int uDPMaxSize) {
		UDPMaxSize = uDPMaxSize;
	}
	
	public int send(int groupIdx, long nodeID, NodeInfo nodeInfo, byte[] vMessage, int sendType) {
		if (vMessage == null) {
			logger.error("send message null.");
			return 0;
		}
		int maxValueSize = InsideOptions.getInstance().getMaxBufferSize();
		if (vMessage.length > maxValueSize) {
			Breakpoint.getInstance().getNetworkBP().sendRejectByTooLargeSize(this.config.getMyGroupIdx());
			logger.error("Message size too large {}, max size {}, skip message.", vMessage.length, maxValueSize);
			return 0;
		}
		
		Breakpoint.getInstance().getNetworkBP().send(vMessage, this.config.getMyGroupIdx());
		
		if (vMessage.length > UDPMaxSize || sendType == MessageSendType.TCP.getValue()) {
			Breakpoint.getInstance().getNetworkBP().sendTcp(vMessage, this.config.getMyGroupIdx());
			return this.network.sendMessageTCP(groupIdx, nodeInfo.getIp(), nodeInfo.getPort(), vMessage);
		} else {
			Breakpoint.getInstance().getNetworkBP().sendUdp(vMessage, this.config.getMyGroupIdx());
			return this.network.sendMessageUDP(groupIdx, nodeInfo.getIp(), nodeInfo.getPort(), vMessage);
		}
	}

	@Override
	public int sendMessage(int groupIdx, long sendtoNodeID, byte[] vBuffer, int sendType) {
		return send(groupIdx, sendtoNodeID, new NodeInfo(sendtoNodeID), vBuffer, sendType);
	}

	@Override
	public int broadcastMessage(int groupIdx, byte[] vBuffer, int sendType) {
		Set<Long> nodeInfoSet = config.getSystemVSM().getMembershipMap();
		for (Long nodeID : nodeInfoSet) {
			if (nodeID != this.myNodeID) {
				send(groupIdx, nodeID, new NodeInfo(nodeID), vBuffer, sendType);
			}
		}
		
		return 0;
	}

	@Override
	public int broadcastMessageFollower(int groupIdx, byte[] vBuffer, int sendType) {
		Map<Long, Long> followerNodeMap = this.config.getMyFollowerMap();
		Iterator<Entry<Long, Long>> iter = followerNodeMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<Long, Long> item = iter.next();
			long nodeID = item.getKey();
			if (nodeID != this.myNodeID) {
				send(groupIdx, nodeID, new NodeInfo(nodeID), vBuffer, sendType);
			}
		}
		
		logger.debug("broadcastMessageFollower {} node.", followerNodeMap.size());
		return 0;
	}

	@Override
	public int broadcastMessageTempNode(int groupIdx, byte[] vBuffer, int sendType) {
		Map<Long, Long> tmpNodeMap = this.config.getTmpNodeMap();
		Iterator<Entry<Long, Long>> iter = tmpNodeMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<Long, Long> item = iter.next();
			long nodeID = item.getKey();
			if (nodeID != this.myNodeID) {
				send(groupIdx, nodeID, new NodeInfo(nodeID), vBuffer, sendType);
			}
		}
		
		logger.debug("broadcastMessageTempNode {} node.", tmpNodeMap.size());
		return 0;
	}
}
