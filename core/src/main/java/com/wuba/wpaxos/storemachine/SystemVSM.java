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
package com.wuba.wpaxos.storemachine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.comm.MembershipChangeCallback;
import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.communicate.NetWork;
import com.wuba.wpaxos.config.Def;
import com.wuba.wpaxos.config.PaxosNodeFunctionRet;
import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.exception.SerializeException;
import com.wuba.wpaxos.proto.PaxosNodeInfo;
import com.wuba.wpaxos.proto.SystemVariables;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.store.SystemVariablesStore;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * group membership wrapper
 */
public class SystemVSM implements InsideSM {
	private final Logger logger = LogManager.getLogger(SystemVSM.class);
	public int smID;
	public Object smCtx;
	private int myGroupIdx;
	private SystemVariables systemVariables = new SystemVariables();
	private SystemVariablesStore systemVStore;
	private Set<Long> nodeIDSet;
	private long myNodeID;
	private MembershipChangeCallback memberShipChangeCallback;
	private NetWork netWork;
	
	public SystemVSM(int groupIdx, long myNodeID, LogStorage logStorage, NetWork netWork, MembershipChangeCallback membershipChangeCallback) {
		this.smID = Def.SYSTEM_V_SMID;
		this.myGroupIdx = groupIdx;
		this.systemVStore = new SystemVariablesStore(logStorage);
		this.myNodeID = myNodeID;
		this.memberShipChangeCallback = membershipChangeCallback;
		this.nodeIDSet = new HashSet<Long>();
		this.netWork = netWork;
	}

	public void init() {
		this.systemVariables = this.systemVStore.read(myGroupIdx);

		if (this.systemVariables == null) {
			this.systemVariables = new SystemVariables();
			this.systemVariables.setGid(0);
			this.systemVariables.setVersion(-1);
			logger.info("variables not exist.");
		} else {
			this.releshNodeID();
			logger.info("OK, groupidx {} gid {} version {}.", this.myGroupIdx, this.systemVariables.getGid(), this.systemVariables.getVersion());
		}
	}

	public int updateSystemVariables(SystemVariables variables) {
		WriteOptions writeOptions = new WriteOptions(true);

		int ret = this.systemVStore.write(writeOptions, this.myGroupIdx, variables);
		if (ret != 0) {
			logger.error("SystemStore::write fail, ret {}.", ret);
			return -1;
		}

		this.systemVariables = variables;
		releshNodeID();

		return 0;
	}

	@Override
	public int getSMID() {
		return this.smID;
	}

	public long getGid () {
		return systemVariables.getGid();
	}

	@Override
	public boolean execute(int groupIdx, long instanceID, byte[] paxosValue, SMCtx smCtx) {
		SystemVariables variables = new SystemVariables();
		try {
			variables.parseFromBytes(paxosValue, paxosValue.length);
		} catch(Exception e) {
			logger.error("SystemVariables parseFromBytes failed, bufferlen {}.", paxosValue.length, e);
			return false;
		}

		if (this.systemVariables.getGid() != 0 && variables.getGid() != this.systemVariables.getGid()) {
			logger.error("modify.gid {} not equal to now.gid {}.", variables.getGid(), this.systemVariables.getGid());
			if (smCtx != null && smCtx.getpCtx() != null) {
				smCtx.setpCtx(PaxosNodeFunctionRet.Paxos_MembershipOp_GidNotSame.getRet());
			}
			return true;
		}

		if (variables.getVersion() != this.systemVariables.getVersion()) {
			logger.error("modify.version {} not equal to now.version {}.", variables.getVersion(), this.systemVariables.getVersion());
			if (smCtx != null && smCtx.getpCtx() != null) {
				smCtx.setpCtx(PaxosNodeFunctionRet.Paxos_MembershipOp_VersionConflit.getRet());
			}
			return true;
		}

		variables.setVersion(instanceID);
		int ret = updateSystemVariables(variables);
		if (ret != 0) {
			return false;
		}

		logger.error("OK, new version {} gid {}.", this.systemVariables.getVersion(), this.systemVariables.getGid());

		if (smCtx != null && smCtx.getpCtx() != null) {
			smCtx.setpCtx(0);
		}

		return true;
	}

	@Override
	public long getCheckpointInstanceID(int groupIdx) {
		return this.systemVariables.getVersion();
	}

	@Override
	public int lockCheckpointState() {
		return 0;
	}

	@Override
	public int getCheckpointState(int groupIdx, JavaOriTypeWrapper<String> dirPath, List<String> fileList) {
		return 0;
	}

	@Override
	public void unLockCheckpointState() {
	}

	@Override
	public int loadCheckpointState(int groupIdx, String checkpointTmpFileDirPath, List<String> fileList,
	                               long checkpointInstanceID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte[] beforePropose(int groupIdx, byte[] sValue) {
		// TODO Auto-generated method stub
		return sValue;
	}

	@Override
	public boolean needCallBeforePropose() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean executeForCheckpoint(int groupIdx, long instanceID, byte[] paxosValue) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public byte[] getCheckpointBuffer() {
		if (this.systemVariables.getVersion() == -1 || this.systemVariables.getGid() == 0) {
			return null;
		}

		byte[] cpBuf = null;
		try {
			cpBuf = this.systemVariables.serializeToBytes();
		} catch(Exception e) {
			logger.error("", e);
		}

		return cpBuf;
	}

	@Override
	public UpdateCpRet updateByCheckpoint(byte[] cpBuffer) {
		UpdateCpRet updateCpRet = new UpdateCpRet(false, 0);
		if (cpBuffer == null || cpBuffer.length == 0) {
			return updateCpRet;
		}

		SystemVariables variables = new SystemVariables();
		try {
			variables.parseFromBytes(cpBuffer, cpBuffer.length);
		} catch(Exception e) {
			e.printStackTrace();
			logger.error("Variables.parseFromArray failed, bufferLen {}.", cpBuffer.length);
			updateCpRet.setRet(-1);
			return updateCpRet;
		}

		if (variables.getVersion() == -1) {
			logger.error("variables.version not init, this is not checkpoint.");
			updateCpRet.setRet(-2);
			return updateCpRet;
		}

		if (this.systemVariables.getGid() != 0 && variables.getGid() != this.systemVariables.getGid()) {
			logger.error("gid not same, cp.gid {} now.gid {}.", variables.getGid(), this.systemVariables.getGid());
			updateCpRet.setRet(-2);
			return updateCpRet;
		}

		if (this.systemVariables.getVersion() != -1 && variables.getVersion() <= this.systemVariables.getVersion()) {
			logger.debug("lag checkpoint, no need update, cp.version {} now.version {}.", variables.getVersion(), this.systemVariables.getVersion());
			updateCpRet.setRet(0);
			return updateCpRet;
		}

		updateCpRet.setChange(true);
		SystemVariables oldVariables = this.systemVariables;

		int ret = updateSystemVariables(variables);
		if (ret != 0) {
			updateCpRet.setRet(-1);
			return updateCpRet;
		}

		logger.info("ok, cp.version {} cp.membercount {} old.version {} old.memebercount {}.",
				variables.getVersion(), variables.getMembershipSize(), oldVariables.getVersion(), oldVariables.getMembershipSize());

		return updateCpRet;
	}

	public void releshNodeID() {
		this.nodeIDSet.clear();

		List<NodeInfo> nodeInfoList = new LinkedList<NodeInfo>();
		for (int i = 0; i < this.systemVariables.getMembershipSize(); i++) {
			PaxosNodeInfo nodeInfo = this.systemVariables.getMemberShips().get(i);
			NodeInfo tmpNode = new NodeInfo(nodeInfo.getNodeID());

			logger.info("ip {} port {} nodeid {},", tmpNode.getIp(), tmpNode.getPort(), tmpNode.getNodeID());
			this.nodeIDSet.add(tmpNode.getNodeID());
			nodeInfoList.add(tmpNode);
		}
		Set<NodeInfo> members = new HashSet<>();
		for (NodeInfo nodeInfo : nodeInfoList) {
			if (nodeInfo.getNodeID() != myNodeID) {
				members.add(nodeInfo);
			}
		}
		if (netWork != null) {
			logger.info("tcp handler set check node members {}",members.size());
			netWork.setCheckNode(myGroupIdx, members);
		}
		if (this.memberShipChangeCallback != null) {
			this.memberShipChangeCallback.callback(this.myGroupIdx, nodeInfoList);
		}
	}

	public long getMembership(List<NodeInfo> nodeInfoList) {
		long version = systemVariables.getVersion();

		for (int i = 0; i < systemVariables.getMembershipSize(); i++) {
			PaxosNodeInfo pnodeInfo = systemVariables.getMemberShips().get(i);

			NodeInfo nodeInfo = new NodeInfo(pnodeInfo.getNodeID());
			nodeInfoList.add(nodeInfo);
		}

		return version;
	}

	public byte[] memberShipOpValue(List<NodeInfo> nodeInfoList, long version) {
		SystemVariables variables = new SystemVariables();
		variables.setVersion(version);
		variables.setGid(systemVariables.getGid());

		for (NodeInfo nodeInfo : nodeInfoList) {
			PaxosNodeInfo pNodeInfo = new PaxosNodeInfo();
			pNodeInfo.setRid(0);
			pNodeInfo.setNodeID(nodeInfo.getNodeID());
			variables.addMemberShip(pNodeInfo);
		}

		try {
			return variables.serializeToBytes();
		} catch (SerializeException e) {
			logger.error("", e);
		}

		return null;
	}

	public byte[] createGidReTurnOpValue(long gid) {
		SystemVariables variables = this.systemVariables.clone();
		variables.setGid(gid);

		byte[] opValue = null;
		try {
			opValue = variables.serializeToBytes();
		} catch(Exception e) {
			logger.error("", e);
		}

		return opValue;
	}

	public boolean isIMInMembership() {
	    return this.nodeIDSet.contains(this.myNodeID);
	}

	public int getNodeCount() {
	    return this.nodeIDSet.size();
	}

	public int getMajorityCount() {
	    return (int)(Math.floor(((double)getNodeCount() / 2)) + 1);
	}

	public boolean isValidNodeID(Long nodeID) {
	    if (this.systemVariables.getGid() == 0) {
	        return true;
	    }

	    return this.nodeIDSet.contains(nodeID);
	}

	public Set<Long> getMembershipMap() {
	    return this.nodeIDSet;
	}

	public SystemVariables getSystemVariables() {
		return this.systemVariables;
	}

	public void addNodeIDList(List<NodeInfo> nodeInfoList) {
		if (this.systemVariables.getGid() != 0) {
			logger.debug("no need to add, i already have memebership info.");
			return;
		}

		this.nodeIDSet.clear();
		this.systemVariables.clearMembership();

		for (NodeInfo nodeInfo : nodeInfoList) {
			PaxosNodeInfo paxosNodeInfo = new PaxosNodeInfo();
			paxosNodeInfo.setRid(0);
			paxosNodeInfo.setNodeID(nodeInfo.getNodeID());
			this.systemVariables.addMemberShip(paxosNodeInfo);
		}

		releshNodeID();
	}

	@Override
	public void fixCheckpointByMinChosenInstanceId(long minChosenInstanceID) {
		// TODO Auto-generated method stub
	}
}
