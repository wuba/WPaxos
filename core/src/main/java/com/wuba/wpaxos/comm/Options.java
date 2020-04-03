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

import java.util.ArrayList;
import java.util.List;

import com.wuba.wpaxos.comm.breakpoint.Breakpoint;
import com.wuba.wpaxos.comm.enums.IndexType;
import com.wuba.wpaxos.comm.enums.PaxosLogCleanType;
import com.wuba.wpaxos.communicate.NetWork;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.store.config.StoreConfig;

public class Options {
	private LogStorage logStorage;

	//optional
	//If poLogStorage == nullptr, sLogStoragePath is required.
	private String logStoragePath;

	//optional
	private String logStorageConfPath;

	// defaultStoreConfig
	private StoreConfig storeConfig;

	//optional
	//If true, the write will be flushed from the operating system
	//buffer cache before the write is considered complete.
	//If this flag is true, writes will be slower.
	//
	//If this flag is false, and the machine crashes, some recent
	//writes may be lost. Note that if it is just the process that
	//crashes (i.e., the machine does not reboot), no writes will be
	//lost even if sync==false. Because of the data lost, we not guarantee consistence.
	//
	//Default is true.
	private boolean writeSync = true;

	//optional
	//Default is 0.
	//This means the write will skip flush at most iSyncInterval times.
	//That also means you will lost at most iSyncInterval count's paxos log.
	private int syncInterval = 0;

	//optional
	//User-specified network.
	private NetWork netWork;

	//optional
	//Our default network use udp and tcp combination, a message we use udp or tcp to send decide by a threshold.
	//Message size under iUDPMaxSize we use udp to send.
	//Default is 4096.
	private int UDPMaxSize = 4096;

	//optional
	//Our default network io thread count.
	//Default is 1.
	private int ioThreadCount = 1;

	//optional
	//We support to run multi phxpaxos on one process.
	//One paxos group here means one independent phxpaxos. Any two phxpaxos(paxos group) only share network, no other.
	//There is no communication between any two paxos group.
	//Default is 1.
	private int groupCount;

	//required
	//Self node's ip/port.
	private NodeInfo myNode;

	//required
	//All nodes's ip/port with a paxos set(usually three or five nodes).
	private List<NodeInfo> nodeInfoList = new ArrayList<NodeInfo>();

	//optional
	//Only bUseMembership == true, we use option's nodeinfolist to init paxos membership,
	//after that, paxos will remember all nodeinfos, so second time you can run paxos without vecNodeList,
	//and you can only change membership by use function in node.h.
	//
	//Default is false.
	//if bUseMembership == false, that means every time you run paxos will use vecNodeList to build a new membership.
	//when you change membership by a new vecNodeList, we don't guarantee consistence.
	//
	//For test, you can set false.
	//But when you use it to real services, remember to set true.
	private boolean useMembership;

	//While membership change, phxpaxos will call this function.
	//Default is nullptr.
	private MembershipChangeCallback membershipChangeCallback;

	//While master change, phxpaxos will call this function.
	//Default is nullptr.
	private MasterChangeCallback masterChangeCallback;

	//optional
	//One phxpaxos can mounting multi state machines.
	//This vector include different phxpaxos's state machines list.
	private List<GroupSMInfo> groupSMInfoList = new ArrayList<GroupSMInfo>();

	//optional
	private Breakpoint breakpoint;

	//optional
	//If use this mode, that means you propose large value(maybe large than 5M means large) much more.
	//Large value means long latency, long timeout, this mode will fit it.
	//Default is false
	private boolean isLargeValueMode;

	//optional
	//All followers's ip/port, and follow to node's ip/port.
	//Follower only learn but not participation paxos algorithmic process.
	//Default is empty.
	private List<FollowerNodeInfo> followerNodeInfoList = new ArrayList<FollowerNodeInfo>();

	//optional
	//Notice, this function must be thread safe!
	//if pLogFunc == nullptr, we will print log to standard ouput.
	//LogFunc pLogFunc;

	//optional
	//If you use your own log function, then you control loglevel yourself, ignore this.
	//Check log.h to find 5 level.
	//Default is LogLevel::LogLevel_None, that means print no log.
	//LogLevel eLogLevel;

	//optional
	//If you use checkpoint replayer feature, set as true.
	//Default is false;
	private boolean useCheckpointReplayer = false;

	//optional
	//Only bUseBatchPropose is true can use API BatchPropose in node.h
	//Default is false;
	private boolean useBatchPropose;

	//optional
	//Only bOpenChangeValueBeforePropose is true, that will callback sm's function(BeforePropose).
	//Default is false;
	private boolean openChangeValueBeforePropose;

	//optional
	//prepareTimeout
	//Default is 1000
	private int prepareTimeout = 1000;

	//optional
	//acceptTimeout
	//Default is 1000
	private int acceptTimeout = 1000;

	//optional
	//askForLearnTimeout
	//Default is 1000
	private int askForLearnTimeout = 3000;
	
	//optional
	//commitTimeout
	//Default is 2000
	private int commitTimeout = 2000;

	//learner sender发送者限速，默认10M
	private int learnerSendSpeed = 100 * 1024 * 1024;

	//optional
	//debugLogEnabled
	//Default is false
	private boolean debugLogEnabled = false;

	//optional
	//batchCount
	//Default is 20
	private int batchCount;

	//optional
	//batchSize
	//Default is 512 * 10
	private int batchSize;

	//optional
	//batchMills
	//Default is 5
	private int batchMills;

	//optional
	//indexType
	//Default is LEVEL_DB
	//IndexType.LEVEL_DB is appropriate for a large number of groups.
	//IndexType.PHYSIC_FILE is appropriate for groups less than 9, and it has high performance when storeconfig's transientStorePoolEnable set true.
	private IndexType indexType = IndexType.LEVEL_DB;
	
	//optional
	//paxosLogCleanType
	//Default is cleanByHoldCount
	private PaxosLogCleanType paxosLogCleanType = PaxosLogCleanType.cleanByHoldCount;

	public IndexType getIndexType() {
		return indexType;
	}

	public void setIndexType(IndexType indexType) {
		this.indexType = indexType;
	}

	public int getLearnerSendSpeed() {
		return learnerSendSpeed;
	}

	public void setLearnerSendSpeed(int learnerSendSpeed) {
		this.learnerSendSpeed = learnerSendSpeed;
	}

	public LogStorage getLogStorage() {
		return logStorage;
	}

	public void setLogStorage(LogStorage logStorage) {
		this.logStorage = logStorage;
	}

	public String getLogStoragePath() {
		return logStoragePath;
	}

	public void setLogStoragePath(String logStoragePath) {
		this.logStoragePath = logStoragePath;
	}

	public String getLogStorageConfPath() {
		return logStorageConfPath;
	}

	public void setLogStorageConfPath(String logStorageConfPath) {
		this.logStorageConfPath = logStorageConfPath;
	}

	public StoreConfig getStoreConfig() {
		return storeConfig;
	}

	public void setStoreConfig(StoreConfig storeConfig) {
		this.storeConfig = storeConfig;
	}

	public boolean isWriteSync() {
		return writeSync;
	}

	public void setWriteSync(boolean writeSync) {
		this.writeSync = writeSync;
	}

	public int getSyncInterval() {
		return syncInterval;
	}

	public void setSyncInterval(int syncInterval) {
		this.syncInterval = syncInterval;
	}

	public NetWork getNetWork() {
		return netWork;
	}

	public void setNetWork(NetWork netWork) {
		this.netWork = netWork;
	}

	public int getUDPMaxSize() {
		return UDPMaxSize;
	}

	public void setUDPMaxSize(int uDPMaxSize) {
		UDPMaxSize = uDPMaxSize;
	}

	public int getIoThreadCount() {
		return ioThreadCount;
	}

	public void setIoThreadCount(int ioThreadCount) {
		this.ioThreadCount = ioThreadCount;
	}

	public int getGroupCount() {
		return groupCount;
	}

	public void setGroupCount(int groupCount) {
		this.groupCount = groupCount;
	}

	public NodeInfo getMyNode() {
		return myNode;
	}

	public void setMyNode(NodeInfo myNode) {
		this.myNode = myNode;
	}

	public List<NodeInfo> getNodeInfoList() {
		return nodeInfoList;
	}

	public void setNodeInfoList(List<NodeInfo> nodeInfoList) {
		this.nodeInfoList = nodeInfoList;
	}

	public boolean isUseMembership() {
		return useMembership;
	}

	public void setUseMembership(boolean useMembership) {
		this.useMembership = useMembership;
	}

	public MembershipChangeCallback getMembershipChangeCallback() {
		return membershipChangeCallback;
	}

	public void setMembershipChangeCallback(MembershipChangeCallback membershipChangeCallback) {
		this.membershipChangeCallback = membershipChangeCallback;
	}

	public MasterChangeCallback getMasterChangeCallback() {
		return masterChangeCallback;
	}

	public void setMasterChangeCallback(MasterChangeCallback masterChangeCallback) {
		this.masterChangeCallback = masterChangeCallback;
	}

	public List<GroupSMInfo> getGroupSMInfoList() {
		return groupSMInfoList;
	}

	public void setGroupSMInfoList(List<GroupSMInfo> groupSMInfoList) {
		this.groupSMInfoList = groupSMInfoList;
	}

	public boolean isLargeValueMode() {
		return isLargeValueMode;
	}

	public void setLargeValueMode(boolean isLargeValueMode) {
		this.isLargeValueMode = isLargeValueMode;
	}

	public List<FollowerNodeInfo> getFollowerNodeInfoList() {
		return followerNodeInfoList;
	}

	public void setFollowerNodeInfoList(List<FollowerNodeInfo> followerNodeInfoList) {
		this.followerNodeInfoList = followerNodeInfoList;
	}

	public boolean isUseCheckpointReplayer() {
		return useCheckpointReplayer;
	}

	public void setUseCheckpointReplayer(boolean useCheckpointReplayer) {
		this.useCheckpointReplayer = useCheckpointReplayer;
	}

	public boolean isUseBatchPropose() {
		return useBatchPropose;
	}

	public void setUseBatchPropose(boolean useBatchPropose) {
		this.useBatchPropose = useBatchPropose;
	}

	public boolean isOpenChangeValueBeforePropose() {
		return openChangeValueBeforePropose;
	}

	public void setOpenChangeValueBeforePropose(boolean openChangeValueBeforePropose) {
		this.openChangeValueBeforePropose = openChangeValueBeforePropose;
	}

	public Breakpoint getBreakpoint() {
		return breakpoint;
	}

	public void setBreakpoint(Breakpoint breakpoint) {
		this.breakpoint = breakpoint;
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

	public int getCommitTimeout() {
		return commitTimeout;
	}

	public void setCommitTimeout(int commitTimeout) {
		this.commitTimeout = commitTimeout;
	}

	public boolean isDebugLogEnabled() {
		return debugLogEnabled;
	}

	public void setDebugLogEnabled(boolean debugLogEnabled) {
		this.debugLogEnabled = debugLogEnabled;
	}

	public int getBatchCount() {
		return batchCount;
	}

	public void setBatchCount(int batchCount) {
		this.batchCount = batchCount;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public int getBatchMills() {
		return batchMills;
	}

	public void setBatchMills(int batchMills) {
		this.batchMills = batchMills;
	}

	public PaxosLogCleanType getPaxosLogCleanType() {
		return paxosLogCleanType;
	}

	public void setPaxosLogCleanType(PaxosLogCleanType paxosLogCleanType) {
		this.paxosLogCleanType = paxosLogCleanType;
	}
}
