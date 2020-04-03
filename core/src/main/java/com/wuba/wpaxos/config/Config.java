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
package com.wuba.wpaxos.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.comm.FollowerNodeInfo;
import com.wuba.wpaxos.comm.InsideOptions;
import com.wuba.wpaxos.comm.MembershipChangeCallback;
import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.comm.enums.PaxosLogCleanType;
import com.wuba.wpaxos.communicate.NetWork;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.storemachine.InsideSM;
import com.wuba.wpaxos.storemachine.SystemVSM;
import com.wuba.wpaxos.utils.Time;

import java.util.*;
import java.util.Map.Entry;

/**
 * 每个group运行相关配置
 */
public class Config {
	private static final Logger logger = LogManager.getLogger(Config.class);
	private static final int tmpNodeTimeout = 60000;
	private boolean logSync;
	private int syncInterval;
	private boolean useMemberShip;
	private long myNodeID;
	private int myGroupIdx;
	private int groupCount;
	private List<NodeInfo> nodeInfoList = new ArrayList<NodeInfo>();
	private boolean isIMFollower;
	private long followNodeID;
	private SystemVSM systemVSM;
	private InsideSM masterSM;
	private Map<Long, Long> tmpNodeOnlyForLearnMap = new HashMap<Long, Long>();
	private Map<Long, Long> myFollowerMap = new HashMap<Long, Long>();
	private int prepareTimeout = 1000;
	private int acceptTimeout = 1000;
	private int askForLearnTimeout = 3000;
	private int learnerSendSpeed;
	private PaxosLogCleanType paxosLogCleanType;

	public Config(LogStorage logStorage, boolean logSync,
				  int syncInterval,
				  boolean useMembership,
				  NodeInfo myNode,
				  List<NodeInfo> nodeInfoList,
				  List<FollowerNodeInfo> followerNodeInfoList,
				  int myGroupIdx,
				  int groupCount,
				  MembershipChangeCallback membershipChangeCallback,
				  PaxosLogCleanType paxosLogCleanType,
				  int learnerSendSpeed, NetWork network) {
		this.logSync = logSync;
		this.syncInterval = syncInterval;
		this.useMemberShip = useMembership;
		this.myNodeID = myNode.getNodeID();
		this.myGroupIdx = myGroupIdx;
		this.groupCount = groupCount;
		this.systemVSM = new SystemVSM(myGroupIdx, myNode.getNodeID(), logStorage, network,membershipChangeCallback);
		this.masterSM = null;

		this.nodeInfoList = nodeInfoList;
		this.isIMFollower = false;
		this.followNodeID = 0;
		this.learnerSendSpeed = learnerSendSpeed;
		this.paxosLogCleanType = paxosLogCleanType;

		for(FollowerNodeInfo fni :followerNodeInfoList) {
			if(fni.getMyNode().getNodeID() == myNode.getNodeID()) {
				logger.warn("I'm follower, ip={}, port={}, nodeid={}.", myNode.getIp(), myNode.getPort(), myNode.getNodeID());
				this.isIMFollower = true;
				this.followNodeID = fni.getFollowNode().getNodeID();
				InsideOptions.getInstance().setAsFollower();
			}
		}
	}

	public int getLearnerSendSpeed() {
		return learnerSendSpeed;
	}

	public void init() throws Exception {
		this.systemVSM.init();
		this.systemVSM.addNodeIDList(this.nodeInfoList);
		logger.info("config init success");
	}

	public boolean checkConfig() {
		if(!this.systemVSM.isIMInMembership()) {
			logger.error("my node={} is not in membership", this.myNodeID);
			return false;
		}

		return true;
	}

	public SystemVSM getSystemVSM() {
		return this.systemVSM;
	}

	public long getGid() {
		return this.systemVSM.getGid();
	}

	public long getMyNodeID() {
		return this.myNodeID;
	}

	public int getNodeCount() {
		return this.systemVSM.getNodeCount();
	}

	public int getMyGroupIdx() {
		return this.myGroupIdx;
	}

	public int getGroupCount() {
		return this.groupCount;
	}

	public int getMajorityCount() {
		return this.systemVSM.getMajorityCount();
	}

	public boolean getIsUseMembership() {
		return this.useMemberShip;
	}

	public int getPrepareTimeoutMs() {
		return prepareTimeout;
	}

	public int getAcceptTimeoutMs() {
		return acceptTimeout;
	}

	public long getAskforLearnTimeoutMs() {
		return askForLearnTimeout;
	}

	public boolean isValidNodeID(long nodeID) {
		return this.systemVSM.isValidNodeID(nodeID);
	}

	public boolean isIMFollower() {
		return this.isIMFollower;
	}

	public long getFollowToNodeID() {
		return this.followNodeID;
	}

	public boolean logSync() {
		return this.logSync;
	}

	public int syncInterval() {
		return this.syncInterval;
	}

	public void setLogSync(boolean logSync) {
		this.logSync = logSync;
	}

	public void setMasterSM(InsideSM masterSM) {
		this.masterSM = masterSM;
	}

	public InsideSM getMasterSM() {
		return this.masterSM;
	}

	public void addTmpNodeOnlyForLearn(Long tmpNodeID) {
		Set<Long> nodeIdSet = this.systemVSM.getMembershipMap();
		if(nodeIdSet.contains(tmpNodeID)) {
			return ;
		}

		this.tmpNodeOnlyForLearnMap.put(tmpNodeID, Time.getSteadyClockMS() + tmpNodeTimeout);
	}

	public Map<Long, Long> getTmpNodeMap() {
		long now = Time.getSteadyClockMS();

		Iterator<Entry<Long, Long>> iter = this.tmpNodeOnlyForLearnMap.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Long, Long> entry = iter.next();
			if(entry.getValue() < now) {
				iter.remove();
			}
		}

		return this.tmpNodeOnlyForLearnMap;
	}

	public void addFollowerNode(Long myFollowerNodeID) {
		int followerTimeout = InsideOptions.getInstance().getAskforLearnInterval() * 3;
		this.myFollowerMap.put(myFollowerNodeID, Time.getSteadyClockMS() + followerTimeout);
	}

	public Map<Long, Long> getMyFollowerMap() {
		long now = Time.getSteadyClockMS();

		Iterator<Entry<Long, Long>> iter = this.myFollowerMap.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Long, Long> entry = iter.next();
			if(entry.getValue() < now) {
				iter.remove();
			}
		}

		return this.myFollowerMap;
	}

	public int getMyFollowerCount() {
		return this.myFollowerMap.size();
	}

	public int getPrepareTimeout() {
		return prepareTimeout;
	}

	public void setPrepareTimeout(int prepareTimeout) {
		this.prepareTimeout = prepareTimeout;
	}

	public int getAcceptTimeout() {
		return acceptTimeout;
	}

	public void setAcceptTimeout(int acceptTimeout) {
		this.acceptTimeout = acceptTimeout;
	}

	public int getAskForLearnTimeout() {
		return askForLearnTimeout;
	}

	public void setAskForLearnTimeout(int askForLearnTimeout) {
		this.askForLearnTimeout = askForLearnTimeout;
	}

	public PaxosLogCleanType getPaxosLogCleanType() {
		return paxosLogCleanType;
	}

	public void setPaxosLogCleanType(PaxosLogCleanType paxosLogCleanType) {
		this.paxosLogCleanType = paxosLogCleanType;
	}

	public boolean isUseCheckpointCleaner() {
		return this.getPaxosLogCleanType().getType() == PaxosLogCleanType.cleanByHoldCount.getType();
	}
}















