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
package com.wuba.wpaxos.sample.election;

import com.wuba.wpaxos.comm.GroupSMInfo;
import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.node.Node;
import com.wuba.wpaxos.store.config.StoreConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ElectionServer {

	private NodeInfo myNode;
    private List<NodeInfo> nodeList;
    private String rootPath;
    private Node paxosNode;
    private int groupCount;

	public ElectionServer(NodeInfo myNode, List<NodeInfo> nodeList, int groupCount, String rootPath) {
		this.myNode = myNode;
		this.nodeList = nodeList;
		this.paxosNode = null;
		this.groupCount = groupCount;
		this.rootPath = rootPath;
	}
	
	public void runPaxos() throws Exception {
		Options options = new Options();
		String logStoragePath = this.makeLogStoragePath(this.rootPath);
		options.setLogStoragePath(logStoragePath);
		options.setGroupCount(groupCount);
		options.setMyNode(this.myNode);
		options.setNodeInfoList(this.nodeList);
		options.setUseMembership(true);
		options.setUseBatchPropose(false);
		options.setStoreConfig(new StoreConfig(rootPath, null));
		
		for(int gid = 0; gid < groupCount; gid++) {
			GroupSMInfo smInfo = new GroupSMInfo();
			smInfo.setUseMaster(true);
			smInfo.setGroupIdx(gid);
			options.getGroupSMInfoList().add(smInfo);
		}
		
		this.paxosNode = Node.runNode(options);
	}
	
	public void addMember(NodeInfo node) throws Exception {
		this.paxosNode.addMember(0, node);
	}
	
	public void deleteMember(NodeInfo node) throws Exception {
		this.paxosNode.removeMember(0, node);
	}
	
	public List<NodeInfo> getAllMembers() {
		List<NodeInfo> nodeInfoList = new ArrayList<NodeInfo>();
		this.paxosNode.showMembership(0, nodeInfoList);
		return nodeInfoList;
	}
	
	public NodeInfo getMasterNode(int groupIdx) {
		return this.paxosNode.getMaster(groupIdx);
	}
	
	public boolean isMaster(int groupIdx) {
		return this.paxosNode.isIMMaster(groupIdx);
	}

	public String makeLogStoragePath(String rootPath) {
		if (rootPath == null) {
			rootPath = System.getProperty("user.dir"); 
		}
		String logStoragePath = rootPath + File.separator + myNode.getNodeID() + File.separator + "db";
		File file = new File(logStoragePath);
		file.mkdirs();
		return logStoragePath;
	}
	
	public NodeInfo getMyNode() {
		return myNode;
	}

	public void setMyNode(NodeInfo myNode) {
		this.myNode = myNode;
	}

	public List<NodeInfo> getNodeList() {
		return nodeList;
	}

	public void setNodeList(List<NodeInfo> nodeList) {
		this.nodeList = nodeList;
	}

	public Node getPaxosNode() {
		return paxosNode;
	}

	public void setPaxosNode(Node paxosNode) {
		this.paxosNode = paxosNode;
	}
}
