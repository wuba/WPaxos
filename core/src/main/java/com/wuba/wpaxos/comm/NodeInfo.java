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
package com.wuba.wpaxos.comm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.utils.ByteConverter;

import java.util.Objects;

/**
 * NodeInfo, paxos group member
 */
public class NodeInfo {
	private static final Logger logger = LogManager.getLogger(NodeInfo.class);
	private long nodeID = 0;
	private String ip = "";
	private int port = -1;
	
	public NodeInfo() {
	}
	
	public NodeInfo(long nodeID) {
		this.nodeID = nodeID;
		parseNodeID();
	}
	
	public NodeInfo(String ip, int port) throws Exception {
		this.ip = ip;
		this.port = port;
		makeNodeID();
	}
	
	public long getNodeID() {
		return nodeID;
	}
	
	public void setNodeID(long nodeID) {
		this.nodeID = nodeID;
	}
	
	public String getIp() {
		return ip;
	}
	
	public void setIp(String ip) {
		this.ip = ip;
		try {
			makeNodeID();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	public int getPort() {
		return port;
	}
	
	public void setPort(int port) {
		this.port = port;
		try {
			makeNodeID();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	public void setIpPort(String ip, int port) {
		this.ip = ip;
		this.port = port;
		try {
			makeNodeID();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	public String getIpPort() {
		String addr = this.ip + ":" + this.port;
		return addr;
	}
	
	private void makeNodeID() throws Exception {
		int ipInt = ByteConverter.ipToInt(ip);
		nodeID = ((long)ipInt) << 32 | port;	
	}
	
	private void parseNodeID() {
		port = (int) (nodeID & (0xffffffff));
		int ipInt = (int) (nodeID >> 32);
		ip = ByteConverter.getIpStr(ipInt);
	}
	
	public static long parseNodeID(String sip, int iport) throws Exception {
		int ipInt = ByteConverter.ipToInt(sip);
		long lnodeID = ((long)ipInt) << 32 | iport;
		return lnodeID;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		NodeInfo nodeInfo = (NodeInfo) o;
		return nodeID == nodeInfo.nodeID &&
				port == nodeInfo.port &&
				Objects.equals(ip, nodeInfo.ip);
	}

	@Override
	public int hashCode() {
		return Objects.hash(nodeID, ip, port);
	}

	@Override
	public String toString() {
		return "NodeInfo [nodeID=" + nodeID + ", ip=" + ip + ", port=" + port + "]";
	}
	
}
