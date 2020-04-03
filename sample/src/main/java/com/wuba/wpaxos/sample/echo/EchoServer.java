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
package com.wuba.wpaxos.sample.echo;

import com.wuba.wpaxos.ProposeResult;
import com.wuba.wpaxos.comm.GroupSMInfo;
import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.comm.enums.IndexType;
import com.wuba.wpaxos.config.PaxosTryCommitRet;
import com.wuba.wpaxos.node.Node;
import com.wuba.wpaxos.store.config.StoreConfig;
import com.wuba.wpaxos.storemachine.SMCtx;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class EchoServer {
	private NodeInfo myNode;
    private List<NodeInfo> nodeList;
    private String rootPath;
    private Node paxosNode;
    private IndexType indexType;
    private int groupCount;

	public EchoServer(NodeInfo myNode, List<NodeInfo> nodeList, int groupCount, String rootPath, int indexType) {
		this.myNode = myNode;
		this.nodeList = nodeList;
		this.paxosNode = null;
		this.groupCount = groupCount;
		this.rootPath = rootPath;
		if (indexType == IndexType.LEVEL_DB.getType()) {
			this.indexType = IndexType.LEVEL_DB;
		} else {
			this.indexType = IndexType.PHYSIC_FILE;
		}
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
		options.setIndexType(indexType);
		options.setStoreConfig(new StoreConfig(rootPath, null));
		
		for(int gid = 0; gid < groupCount; gid++) {
			GroupSMInfo smInfo = new GroupSMInfo();
			smInfo.setUseMaster(true);
			smInfo.setGroupIdx(gid);
			smInfo.getSmList().add(new EchoSM(gid));
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

	public String echo(String echoReqValue, int groupIdx) throws Exception {
		SMCtx ctx = new SMCtx();
		EchoSMCtx echoSMctx = new EchoSMCtx();
		ctx.setSmId(EchoSM.SMID);
		ctx.setpCtx(echoSMctx);

		this.paxosNode.setTimeoutMs(3000);
		ProposeResult proposeResult = null;
		proposeResult = this.paxosNode.propose(groupIdx, echoReqValue.getBytes(), ctx);

		if (PaxosTryCommitRet.PaxosTryCommitRet_OK.getRet() == proposeResult.getResult() && echoSMctx.getEchoRespValue() != null) {
			return new String(echoSMctx.getEchoRespValue());
		}
		
		return null;
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

	public IndexType getIndexType() {
		return indexType;
	}

	public void setIndexType(IndexType indexType) {
		this.indexType = indexType;
	}
}
