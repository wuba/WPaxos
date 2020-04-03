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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.node.Node;

import java.util.Set;

/**
 * 网络通信接口
 */
public abstract class NetWork {
	private final static Logger logger = LogManager.getLogger(NetWork.class); 
	
	private Node node;
	
	// init and run network
	public abstract void runNetWork() throws Exception;

    // stop receive any message.
	public abstract void stopNetWork();

	// send message by tcp
	public abstract int sendMessageTCP(int groupIdx, String ip, int port, byte[] message);

	// send message by udp
	public abstract int sendMessageUDP(int groupIdx, String ip, int port, byte[] message);
	
	// check conn keepalive nodelist 
	public abstract void setCheckNode(int group, Set<NodeInfo> nodeInfo);

    //When receive a message, call this funtion.
    //This funtion is async, just enqueue an return.	
	public int onReceiveMessage(ReceiveMessage receiveMsg) {
		if (this.node != null) {
			this.node.onReceiveMessage(receiveMsg);
		} else {
			logger.info("receive msglen {}, node null.", receiveMsg.getReceiveLen());
		}
		
		return 0;
	}

	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}
}
