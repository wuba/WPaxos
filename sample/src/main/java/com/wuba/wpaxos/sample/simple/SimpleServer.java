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
package com.wuba.wpaxos.sample.simple;

import com.wuba.wpaxos.ProposeResult;
import com.wuba.wpaxos.comm.GroupSMInfo;
import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.comm.enums.IndexType;
import com.wuba.wpaxos.node.Node;
import com.wuba.wpaxos.sample.kv.rocksdb.RocksDBHolder;
import com.wuba.wpaxos.store.config.StoreConfig;
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

import java.io.File;
import java.util.List;

public class SimpleServer {
	private NodeInfo myNode;
	private List<NodeInfo> nodeList;
	private Node paxosNode;
	private int groupCount;
	private boolean useBatchPropose = true;
	private boolean useMaster = false;
	private int batchCount;
	private String rootPath;
	private IndexType indexType;

	public SimpleServer(NodeInfo myNode, List<NodeInfo> nodeList, int groupCount, boolean useBatch, int batchCount, String rootPath, int indexType) {
		this.myNode = myNode;
		this.nodeList = nodeList;
		this.groupCount = groupCount;
		this.useBatchPropose = useBatch;
		this.batchCount = batchCount;
		this.rootPath = rootPath + File.separator + "db" + File.separator + myNode.getNodeID();
		if (indexType == IndexType.LEVEL_DB.getType()) {
			this.indexType = IndexType.LEVEL_DB;
		} else {
			this.indexType = IndexType.PHYSIC_FILE;
		}
	}

	public SimpleServer(NodeInfo myNode, List<NodeInfo> nodeInfoList, int groupCount, String rootPath, int indexType) {
		this(myNode, nodeInfoList, groupCount, true, 20, rootPath, indexType);
	}

	public void runPaxos() throws Exception {
		Options options = new Options();
		String logStoragePath = this.makeLogStoragePath();
		options.setLogStoragePath(logStoragePath);
		options.setGroupCount(groupCount);
		options.setMyNode(this.myNode);
		options.setNodeInfoList(this.nodeList);
		options.setUseMembership(true);
		options.setUseBatchPropose(useBatchPropose);
		options.setIndexType(indexType);
		options.setStoreConfig(new StoreConfig(rootPath, null));
		for (int gid = 0; gid < groupCount; gid++) {
			GroupSMInfo smInfo = new GroupSMInfo();
			smInfo.setUseMaster(this.useMaster);
			smInfo.setGroupIdx(gid);
			SimpleSM fileSM = new SimpleSM(gid);
			smInfo.getSmList().add(fileSM);
			options.getGroupSMInfoList().add(smInfo);
		}
		this.paxosNode = Node.runNode(options);
		this.paxosNode.setHoldPaxosLogCount(500000);
		RocksDBHolder.init(groupCount, rootPath);
		for (int gid = 0; gid < this.groupCount; ++gid) {
			this.paxosNode.setBatchCount(gid, batchCount);
		}
	}

	public ProposeResult propose(byte[] writeReqValue, int groupIdx) {
		SMCtx ctx = new SMCtx();
		ctx.setSmId(3);
		JavaOriTypeWrapper<Integer> indexIdWrap = new JavaOriTypeWrapper<Integer>();
		indexIdWrap.setValue(0);
		this.paxosNode.setTimeoutMs(3000);
		ProposeResult proposeResult = null;
		if (useBatchPropose) {
			proposeResult = this.paxosNode.batchPropose(groupIdx, writeReqValue, indexIdWrap, ctx);
		} else {
			proposeResult = this.paxosNode.propose(groupIdx, writeReqValue, ctx);
		}
		return proposeResult;
	}

	private String makeLogStoragePath() {
		if (rootPath == null) {
			rootPath = System.getProperty("user.dir"); 
		}
		String logStoragePath = rootPath + File.separator + "db" + File.separator  + myNode.getNodeID() + File.separator + "db";
		File file = new File(logStoragePath);
		file.mkdirs();
		return logStoragePath;
	}
}
