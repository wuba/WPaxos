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
package com.wuba.wpaxos.node;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.CommitResult;
import com.wuba.wpaxos.ProposeResult;
import com.wuba.wpaxos.base.BaseMsg;
import com.wuba.wpaxos.comm.FollowerNodeInfo;
import com.wuba.wpaxos.comm.GroupSMInfo;
import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.communicate.DFNetWorker;
import com.wuba.wpaxos.communicate.NetWork;
import com.wuba.wpaxos.communicate.ReceiveMessage;
import com.wuba.wpaxos.config.Def;
import com.wuba.wpaxos.config.PaxosNodeFunctionRet;
import com.wuba.wpaxos.master.MasterInfo;
import com.wuba.wpaxos.master.MasterMgr;
import com.wuba.wpaxos.proto.AcceptorStateData;
import com.wuba.wpaxos.proto.BatchPaxosValue;
import com.wuba.wpaxos.proto.PaxosValue;
import com.wuba.wpaxos.store.DefaultLogStorage;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.storemachine.StateMachine;
import com.wuba.wpaxos.storemachine.SystemVSM;
import com.wuba.wpaxos.utils.ByteConverter;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import com.wuba.wpaxos.utils.NotifierPool;

/**
 * paxos node realize
 */
public class PNode extends Node {
	private final Logger logger = LogManager.getLogger(PNode.class);
	private List<Group> groupList = new ArrayList<Group>();
	private List<MasterMgr> masterList = new ArrayList<MasterMgr>();
	private List<ProposeBatch> proposeBatchList = new ArrayList<ProposeBatch>();
	private LogStorage logStorage = null;
	private DFNetWorker defaultNetWork = DFNetWorker.getInstance();
	private NetWork netWork = null;
	private NotifierPool notifierPool = new NotifierPool();
	private long myNodeID;

	public int init(Options options) throws Exception {
		int ret = checkOptions(options);
		if (ret != 0) {
			logger.error("checkOptions failed, ret {}.", ret);
			return ret;
		}
		myNodeID = options.getMyNode().getNodeID();

		// init logstorage
		logStorage = initLogStorage(options);

		// init network
		ret = initNetWork(options);
		if (ret != 0) {
			return ret;
		}

		// build masterlist
		for (int groupIdx = 0; groupIdx < options.getGroupCount(); groupIdx++) {
			MasterMgr master = new MasterMgr(this, groupIdx, logStorage, options.getMasterChangeCallback(), options);
			master.setName("MasterMgr-" + groupIdx);
			masterList.add(master);

			ret = master.init();
			if (ret != 0) {
				return ret;
			}
		}

		// build grouplist
		for (int groupIdx = 0; groupIdx < options.getGroupCount(); groupIdx++) {
			Group group = new Group(logStorage, netWork, masterList.get(groupIdx).getMasterSM(), groupIdx, options);
			groupList.add(group);
		}

		// build batchpropose
		if (options.isUseBatchPropose()) {
			for (int groupIdx = 0; groupIdx < options.getGroupCount(); groupIdx++) {
				ProposeBatch proposeBatch = new ProposeBatch(groupIdx, this, notifierPool);
				proposeBatchList.add(proposeBatch);
			}
		}

		// init statemachine
		initStateMachine(options);

		// parallel init group
		for (Group group : groupList) {
			group.startInit();
		}

		for (Group group : groupList) {
			int initret = -1;
			try {
				initret = group.getInitRet();
			} catch (Exception e) {
				logger.error("group getInitRet error", e);
			}
			if (initret != 0) {
				ret = initret;
			}
		}

		if (ret != 0) {
			return ret;
		}

		for (Group group : groupList) {
			group.start();
		}
		runMaster(options);
		runProposeBatch();

		logger.info("PNode init success.");
		return 0;
	}


	@Override
	public void resetPaxosNode(int groupIndex, ArrayList<NodeInfo> nodeList) {
		for (Group group : this.groupList) {
			if (group.getConfig().getMyGroupIdx() == groupIndex ) {
				logger.info("restart init system vsm");
				try {
					group.getConfig().flashNodeList(nodeList);
				} catch (Exception exception) {
					logger.info("group init error",exception);
				}
			}
		}
	}

	@Override
	public ProposeResult propose(int groupIdx, byte[] sValue, JavaOriTypeWrapper<Long> instanceIdWrap, SMCtx smCtx, int timeout) {
		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return new ProposeResult(PaxosNodeFunctionRet.Paxos_GroupIdxWrong.getRet(), instanceIdWrap.getValue());
		}

		CommitResult commitRet = groupList.get(groupIdx).getCommitter().newValueGetID(sValue, instanceIdWrap, smCtx, timeout);
		return new ProposeResult(commitRet.getCommitRet(), commitRet.getSuccInstanceID());
	}

	@Override
	public int setMasterElectionPriority(int groupIdx, int electionPriority) {
		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return PaxosNodeFunctionRet.Paxos_GroupIdxWrong.getRet();
		}

		masterList.get(groupIdx).setElectionPriority(electionPriority);
		return 0;
	}

	@Override
	public void stopPaxos() {
		for (int groupIdx = 0; groupIdx < groupList.size(); groupIdx++) {
			this.masterList.get(groupIdx).stopMaster();
		}

		this.netWork.stopNetWork();
		this.logStorage.shutdown();
		
		if (proposeBatchList != null && !proposeBatchList.isEmpty()) {
			for (ProposeBatch proposeBatch : proposeBatchList) {
				proposeBatch.shutdown();
			}
		}
	}

	@Override
	public ProposeResult propose(int groupIdx, byte[] sValue) {
		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return new ProposeResult(PaxosNodeFunctionRet.Paxos_GroupIdxWrong.getRet(), 0);
		}

		JavaOriTypeWrapper<Long> instanceIdWrap = new JavaOriTypeWrapper<Long>();
		CommitResult commitRet = groupList.get(groupIdx).getCommitter().newValueGetID(sValue, instanceIdWrap);
		return new ProposeResult(commitRet.getCommitRet(), commitRet.getSuccInstanceID());
	}

	@Override
	public ProposeResult propose(int groupIdx, byte[] sValue, SMCtx smCtx) {
		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return new ProposeResult(PaxosNodeFunctionRet.Paxos_GroupIdxWrong.getRet(), 0);
		}

		JavaOriTypeWrapper<Long> instanceIdWrap = new JavaOriTypeWrapper<Long>(0L);
		CommitResult commitRet = groupList.get(groupIdx).getCommitter().newValueGetID(sValue, instanceIdWrap, smCtx);
		return new ProposeResult(commitRet.getCommitRet(), commitRet.getSuccInstanceID());
	}

	@Override
	public long getNowInstanceID(int groupIdx) {
		if (!checkGroupID(groupIdx)) {
			return PaxosNodeFunctionRet.Paxos_SystemError.getRet();
		}

		return groupList.get(groupIdx).getInstance().getNowInstanceID();
	}

	@Override
	public long getMinChosenInstanceID(int groupIdx) {
		if (!checkGroupID(groupIdx)) {
			return PaxosNodeFunctionRet.Paxos_SystemError.getRet();
		}

		return groupList.get(groupIdx).getInstance().getMinChosenInstanceID();
	}

	@Override
	public ProposeResult batchPropose(int groupIdx, byte[] sValue, JavaOriTypeWrapper<Integer> indexIDWrap) {
		return batchPropose(groupIdx, sValue, indexIDWrap, null);
	}

	@Override
	public ProposeResult batchPropose(int groupIdx, byte[] sValue, JavaOriTypeWrapper<Integer> indexIDWrap, SMCtx smCtx) {
		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return new ProposeResult(PaxosNodeFunctionRet.Paxos_GroupIdxWrong.getRet());
		}

		if (proposeBatchList.size() == 0) {
			return new ProposeResult(PaxosNodeFunctionRet.Paxos_SystemError.getRet());
		}

		JavaOriTypeWrapper<Long> instanceIDWrap = new JavaOriTypeWrapper<Long>(0L);
		int ret = proposeBatchList.get(groupIdx).propose(sValue, instanceIDWrap, indexIDWrap, smCtx);
		return new ProposeResult(ret, instanceIDWrap.getValue());
	}

	@Override
	public void setBatchCount(int groupIdx, int batchCount) {
		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return;
		}

		if (proposeBatchList.size() == 0) {
			return;
		}

		proposeBatchList.get(groupIdx).setBatchCount(batchCount);
	}

	@Override
	public void setBatchSize(int groupIdx, int batchSize) {
		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return;
		}

		if (proposeBatchList.size() == 0) {
			return;
		}

		proposeBatchList.get(groupIdx).setBatchMaxSize(batchSize);
	}

	@Override
	public void addStateMachine(StateMachine sm) {
		for (Group group : groupList) {
			group.addStateMachine(sm);
		}
	}

	@Override
	public void addStateMachine(int groupIdx, StateMachine sm) {
		if (!checkGroupID(groupIdx)) {
			return;
		}

		groupList.get(groupIdx).addStateMachine(sm);
	}

	@Override
	public long getMasterVersion(int groupIdx) {
		if (!checkGroupID(groupIdx)) {
			return -1;
		}
		return masterList.get(groupIdx).getMasterSM().getMasterWithVersion().getMasterVersion();
	}

	@Override
	public int onReceiveMessage(ReceiveMessage receivemsg) {
		byte[] pcMessage = receivemsg.getReceiveBuf();
		int messageLen = receivemsg.getReceiveLen();
		if (pcMessage == null || messageLen <= 0) {
			logger.error("messge size {} to small, not valid.", messageLen);
			return PaxosNodeFunctionRet.Paxos_SystemError.getRet();
		}

		int groupIdx = -1;

		groupIdx = ByteConverter.bytesToIntLittleEndian(pcMessage, BaseMsg.GROUPIDX_OFFSET);

		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return PaxosNodeFunctionRet.Paxos_GroupIdxWrong.getRet();
		}

		return groupList.get(groupIdx).getInstance().onReceiveMessage(receivemsg);
	}

	@Override
	public long getMyNodeID() {
		return this.myNodeID;
	}

	@Override
	public void setTimeoutMs(int timeoutMs) {
		for (Group group : groupList) {
			group.getCommitter().setTimeoutMs(timeoutMs);
		}
	}

	@Override
	public void setHoldPaxosLogCount(long holdCount) {
		for (Group group : groupList) {
			group.getCheckpointCleaner().setHoldPaxosLogCount(holdCount);
		}
	}

	@Override
	public void pauseCheckpointReplayer() {
		for (Group group : groupList) {
			group.getCheckpointReplayer().pause();
		}
	}

	@Override
	public void continueCheckpointReplayer() {
		for (Group group : groupList) {
			group.getCheckpointReplayer().toContinue();
		}
	}

	@Override
	public void pausePaxosLogCleaner() {
		for (Group group : groupList) {
			group.getCheckpointCleaner().pause();
		}
	}

	@Override
	public void continuePaxosLogCleaner() {
		for (Group group : groupList) {
			group.getCheckpointCleaner().toContinue();
		}
	}

	@Override
	public int addMember(int groupIdx, NodeInfo node) {
		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return PaxosNodeFunctionRet.Paxos_GroupIdxWrong.getRet();
		}

		SystemVSM systemVSM = groupList.get(groupIdx).getConfig().getSystemVSM();
		if (systemVSM.getGid() == 0) {
			return PaxosNodeFunctionRet.Paxos_MembershipOp_NoGid.getRet();
		}

		long version = 0;
		List<NodeInfo> nodeInfoList = new LinkedList<NodeInfo>();
		version = systemVSM.getMembership(nodeInfoList);
		for (NodeInfo nodeInfo : nodeInfoList) {
			if (nodeInfo.getNodeID() == node.getNodeID()) {
				return PaxosNodeFunctionRet.Paxos_MembershipOp_Add_NodeExist.getRet();
			}
		}

		nodeInfoList.add(node);
		return proposalMembership(systemVSM, groupIdx, nodeInfoList, version).getResult();
	}

	@Override
	public int removeMember(int groupIdx, NodeInfo node) {
		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return PaxosNodeFunctionRet.Paxos_GroupIdxWrong.getRet();
		}

		SystemVSM systemVSM = groupList.get(groupIdx).getConfig().getSystemVSM();
		if (systemVSM.getGid() == 0) {
			return PaxosNodeFunctionRet.Paxos_MembershipOp_NoGid.getRet();
		}

		List<NodeInfo> nodeInfoList = new ArrayList<NodeInfo>();
		long version = 0;
		version = systemVSM.getMembership(nodeInfoList);

		boolean nodeExist = false;
		List<NodeInfo> afterNodeInfoList = new ArrayList<NodeInfo>();
		for (NodeInfo nodeInfo : nodeInfoList) {
			if (nodeInfo.getNodeID() == node.getNodeID()) {
				nodeExist = true;
			} else {
				afterNodeInfoList.add(nodeInfo);
			}
		}

		if (!nodeExist) {
			return PaxosNodeFunctionRet.Paxos_MembershipOp_Remove_NodeNotExist.getRet();
		}

		return proposalMembership(systemVSM, groupIdx, afterNodeInfoList, version).getResult();
	}

	@Override
	public int changeMember(int groupIdx, NodeInfo fromNode, NodeInfo toNode) {
		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return PaxosNodeFunctionRet.Paxos_GroupIdxWrong.getRet();
		}

		SystemVSM systemVSM = this.groupList.get(groupIdx).getConfig().getSystemVSM();
		if (systemVSM.getGid() == 0) {
			return PaxosNodeFunctionRet.Paxos_MembershipOp_NoGid.getRet();
		}

		List<NodeInfo> nodeInfoList = new ArrayList<NodeInfo>();
		long version = 0;
		version = systemVSM.getMembership(nodeInfoList);

		List<NodeInfo> afterNodeInfoList = new ArrayList<NodeInfo>();
		boolean fromNodeExist = false;
		boolean toNodeExist = false;

		for (NodeInfo nodeInfo : nodeInfoList) {
			if (nodeInfo.getNodeID() == fromNode.getNodeID()) {
				fromNodeExist = true;
				continue;
			} else if (nodeInfo.getNodeID() == toNode.getNodeID()) {
				toNodeExist = true;
				continue;
			}

			afterNodeInfoList.add(nodeInfo);
		}

		if ((!fromNodeExist) && toNodeExist) {
			return PaxosNodeFunctionRet.Paxos_MembershipOp_Change_NoChange.getRet();
		}

		afterNodeInfoList.add(toNode);

		return proposalMembership(systemVSM, groupIdx, afterNodeInfoList, version).getResult();
	}

	@Override
	public int showMembership(int groupIdx, List<NodeInfo> nodeInfoList) {
		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return PaxosNodeFunctionRet.Paxos_GroupIdxWrong.getRet();
		}

		SystemVSM systemVSM = groupList.get(groupIdx).getConfig().getSystemVSM();
		systemVSM.getMembership(nodeInfoList);

		return 0;
	}

	@Override
	public NodeInfo getMaster(int groupIdx) {
		if (!checkGroupID(groupIdx)) {
			return new NodeInfo(0);
		}

		return new NodeInfo(masterList.get(groupIdx).getMasterSM().getMaster());
	}

	@Override
	public NodeInfo getMasterWithVersion(int groupIdx, MasterInfo masterInfo) {
		if (!checkGroupID(groupIdx)) {
			return new NodeInfo(0);
		}

		MasterInfo mst = masterList.get(groupIdx).getMasterSM().getMasterWithVersion();
		masterInfo.setMasterNodeID(mst.getMasterNodeID());
		masterInfo.setMasterVersion(mst.getMasterVersion());
		return new NodeInfo(masterInfo.getMasterNodeID());
	}

	@Override
	public boolean isIMMaster(int groupIdx) {
		if (!checkGroupID(groupIdx)) {
			return false;
		}

		return masterList.get(groupIdx).getMasterSM().isIMMaster();
	}

	@Override
	public boolean isNoMaster(int groupIdx) {
		if (!checkGroupID(groupIdx)) {
			return false;
		}

		return masterList.get(groupIdx).getMasterSM().noMaster();
	}

	@Override
	public int setMasterLease(int groupIdx, int leaseTimeMs) {
		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return PaxosNodeFunctionRet.Paxos_GroupIdxWrong.getRet();
		}

		masterList.get(groupIdx).setLeaseTime(leaseTimeMs);
		return 0;
	}

	@Override
	public int dropMaster(int groupIdx) {
		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return PaxosNodeFunctionRet.Paxos_GroupIdxWrong.getRet();
		}

		masterList.get(groupIdx).dropMaster();
		return 0;
	}

	@Override
	public int toBeMaster(int groupIdx, int leaseTime) {
		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return PaxosNodeFunctionRet.Paxos_GroupIdxWrong.getRet();
		}

		masterList.get(groupIdx).toBeMaster(leaseTime);
		return 0;
	}

	@Override
	public int toBeMaster(int groupIdx) {
		if (!checkGroupID(groupIdx)) {
			logger.error("message groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return PaxosNodeFunctionRet.Paxos_GroupIdxWrong.getRet();
		}

		masterList.get(groupIdx).toBeMaster();
		return 0;
	}

	@Override
	public void setMaxHoldThreads(int groupIdx, int maxHoldThreads) {
		if (!checkGroupID(groupIdx)) {
			return;
		}

		groupList.get(groupIdx).getCommitter().setMaxHoldThreads(maxHoldThreads);
	}

	@Override
	public void setProposeWaitTimeThresholdMS(int groupIdx, int waitTimeThresholdMS) {
		if (!checkGroupID(groupIdx)) {
			return;
		}

		groupList.get(groupIdx).getCommitter().setProposeWaitTimeThresholdMS(waitTimeThresholdMS);
	}

	@Override
	public void setLogSync(int groupIdx, boolean bLogSync) {
		if (!checkGroupID(groupIdx)) {
			return;
		}

		groupList.get(groupIdx).getConfig().setLogSync(bLogSync);
	}

	@Override
	public int getInstanceValue(int groupIdx, long instanceID, List<PaxosValue> valueList) {
		if (!checkGroupID(groupIdx)) {
			return PaxosNodeFunctionRet.Paxos_GroupIdxWrong.getRet();
		}

		PaxosValue paxosValue = new PaxosValue();
		int ret = groupList.get(groupIdx).getInstance().getInstanceValue(instanceID, paxosValue);
		if (ret != 0) {
			return ret;
		}

		if (paxosValue.getSmID() == Def.BATCH_PROPOSE_SMID) {
			BatchPaxosValue batchValues = new BatchPaxosValue();
			byte[] data = paxosValue.getValue();
			try {
				batchValues.parseFromBytes(data, data.length);
			} catch (Exception e) {
				logger.error("BatchPaxosValue parse from bytes failed.", e);
				return PaxosNodeFunctionRet.Paxos_SystemError.getRet();
			}

			valueList.addAll(batchValues.getBatchList());
		} else {
			valueList.add(paxosValue);
		}

		return 0;
	}

	public int checkOptions(Options options) {
		if (options.getLogStorage() == null && options.getLogStoragePath().length() == 0) {
			logger.error("no logpath and logstorage is null");
			return -2;
		}

		if (options.getUDPMaxSize() > 64 * 1024) {
			logger.error("udp max size {} is too large", options.getUDPMaxSize());
			return -2;
		}

		if (options.getGroupCount() > 200) {
			logger.error("group count {} is too large", options.getGroupCount());
			return -2;
		}

		if (options.getGroupCount() <= 0) {
			logger.error("group count {} is small than zero or equal to zero", options.getGroupCount());
			return -2;
		}

		for (FollowerNodeInfo followerNodeInfo : options.getFollowerNodeInfoList()) {
			if (followerNodeInfo.getMyNode().getNodeID() == followerNodeInfo.getFollowNode().getNodeID()) {
				logger.error("self node ip {} port {} equal to follow node", followerNodeInfo.getFollowNode().getIp(), followerNodeInfo.getMyNode().getPort());
				return -2;
			}
		}

		for (GroupSMInfo groupSMInfo : options.getGroupSMInfoList()) {
			if (groupSMInfo.getGroupIdx() >= options.getGroupCount()) {
				logger.error("SM GroupIdx {} large than groupcount {}", groupSMInfo.getGroupIdx(), options.getGroupCount());
				return -2;
			}
		}

		return 0;
	}

	private LogStorage initLogStorage(Options options) throws Exception {
		if (options.getLogStorage() != null) {
			logger.info("OK, use user logstorage.");
			logStorage = options.getLogStorage();
		} else {
			logStorage = DefaultLogStorage.getInstance();
		}
		String logStoragePath = options.getLogStoragePath();
		if (logStoragePath == null || logStoragePath.length() == 0) {
			throw new Exception("LogStorage path is null.");
		}
		logStorage.init(options);
		logStorage.start();
		options.setLogStorage(logStorage);
		return logStorage;
	}

	public int initNetWork(Options options) {
		if (options.getNetWork() != null) {
			this.netWork = options.getNetWork();
			logger.info("OK, use user network.");
			return 0;
		}

		int ret = defaultNetWork.init(options, options.getIoThreadCount());
		if (ret != 0) {
			logger.error("init default network failed, listenip {} listenport {} ret {}.", options.getMyNode().getIp(), options.getMyNode().getPort(), ret);
			return ret;
		}

		this.netWork = defaultNetWork;
		options.setNetWork(netWork);
		logger.info("OK, use default network.");

		return 0;
	}

	public void initStateMachine(Options options) {
		for (GroupSMInfo groupSMInfo : options.getGroupSMInfoList()) {
			for (StateMachine sm : groupSMInfo.getSmList()) {
				addStateMachine(groupSMInfo.getGroupIdx(), sm);
			}
		}
	}

	public ProposeResult proposalMembership(SystemVSM systemVSM, int groupIdx, List<NodeInfo> nodeInfoList, long version) {
		byte[] pValues = systemVSM.memberShipOpValue(nodeInfoList, version);
		if (pValues == null || pValues.length == 0) {
			return new ProposeResult(PaxosNodeFunctionRet.Paxos_SystemError.getRet());
		}

		SMCtx ctx = new SMCtx();
		JavaOriTypeWrapper<Integer> smret = new JavaOriTypeWrapper<>(-1);
		ctx.setSmId(Def.SYSTEM_V_SMID);
		ctx.setpCtx(smret);

		ProposeResult pResult = propose(groupIdx, pValues, ctx);
		if (pResult.getResult() != 0) {
			return pResult;
		}

		pResult.setResult(((JavaOriTypeWrapper<Integer>) ctx.getpCtx()).getValue());
		return pResult;
	}

	public void runMaster(Options options) {
		for (GroupSMInfo groupSMInfo : options.getGroupSMInfoList()) {
			if (groupSMInfo.isUseMaster()) {
				if (!groupList.get(groupSMInfo.getGroupIdx()).getConfig().isIMFollower()) {
					masterList.get(groupSMInfo.getGroupIdx()).runMaster();
				} else {
					logger.info("I'm follower, not run master damon.");
				}
			}
		}
	}

	public void runProposeBatch() {
		for (ProposeBatch proposeBatch : proposeBatchList) {
			proposeBatch.start();
		}
	}

	public boolean checkGroupID(int groupIdx) {
		if (groupIdx < 0 || groupIdx >= groupList.size()) {
			return false;
		}

		return true;
	}

	@Override
	public LogStorage getLogStorage() {
		return logStorage;
	}

	public void setLogStorage(LogStorage logStorage) {
		this.logStorage = logStorage;
	}

	public static int getPayLoadStartoffset() {
		return AcceptorStateData.HeaderLen + 4; //SMID
	}

	@Override
	public MasterMgr getMasterMgr(int groupIdx) {
		return masterList.get(groupIdx);
	}

	@Override
	public boolean isLearning(int groupIdx) {
		if (!checkGroupID(groupIdx)) {
			logger.error("groupid {} wrong, groupsize {}.", groupIdx, groupList.size());
			return false;
		}
		return groupList.get(groupIdx).getInstance().isLearning();
	}

	@Override
	public long getNowInstanceId(int groupIdx) {
		return groupList.get(groupIdx).getInstance().getNowInstanceID();
	}
}

