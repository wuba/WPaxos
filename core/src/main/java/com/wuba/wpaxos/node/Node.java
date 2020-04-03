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

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.ProposeResult;
import com.wuba.wpaxos.comm.InsideOptions;
import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.communicate.ReceiveMessage;
import com.wuba.wpaxos.master.MasterInfo;
import com.wuba.wpaxos.master.MasterMgr;
import com.wuba.wpaxos.proto.PaxosValue;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.storemachine.StateMachine;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/**
 * paxos node
 */
public abstract class Node {
	private static final Logger logger = LogManager.getLogger(Node.class);
	
	/**
	 * init and start all paxos instances
	 * @param options
	 * @return
	 * @throws Exception
	 */
	public static Node runNode(Options options) throws Exception {
		if (options.isLargeValueMode()) {
			InsideOptions.getInstance().setAsLargeBufferMode();
		}
		
		InsideOptions.getInstance().setGroupCount(options.getGroupCount());
		
		PNode pRealNode = new PNode();
		int ret = pRealNode.init(options);
		if (ret != 0) {
			logger.error("node init error, ret=" + ret);
			return null;
		}
		
		options.getNetWork().setNode(pRealNode);
		options.getNetWork().runNetWork();
		
		return pRealNode;
		
	}

	/**
	 * paxos propose
	 * @param groupIdx
	 * @param sValue : propose value
	 * @return
	 */
    public abstract ProposeResult propose(int groupIdx, byte[] sValue);

    /**
     * paxos propose
     * @param groupIdx
     * @param sValue
     * @param poSMCtx : statemachine context
     * @return
     */
    public abstract ProposeResult propose(int groupIdx, byte[] sValue, SMCtx poSMCtx);

    /**
     * get one group now max instanceID
     * @param groupIdx
     * @return
     */
    public abstract long getNowInstanceID(int groupIdx);

    /**
     * get one group minchosen instanceID
     * @param groupIdx
     * @return
     */
    public abstract long getMinChosenInstanceID(int groupIdx);

    /**
     * get my nodeID
     * @return
     */
    public abstract long getMyNodeID();

    /**
     * Batch propose.
     * Only set options::bUserBatchPropose as true can use this batch API.
     * Warning: BatchProposal will have same llInstanceID returned but different iBatchIndex.
     * Batch values's execute order in StateMachine is certain, the return value iBatchIndex
     * means the execute order index, start from 0.
     * @param groupIdx
     * @param sValue
     * @param indexIDWrap
     * @return
     */
    public abstract ProposeResult batchPropose(int groupIdx, byte[] sValue, JavaOriTypeWrapper<Integer> indexIDWrap);

    /**
     * Batch propose.
     * Only set options::bUserBatchPropose as true can use this batch API.
     * Warning: BatchProposal will have same llInstanceID returned but different iBatchIndex.
     * Batch values's execute order in StateMachine is certain, the return value iBatchIndex
     * means the execute order index, start from 0.
     * @param groupIdx
     * @param sValue
     * @param indexIDWrap
     * @param poSMCtx : state machine context
     * @return
     */
    public abstract ProposeResult batchPropose(int groupIdx, byte[] sValue, JavaOriTypeWrapper<Integer> indexIDWrap, SMCtx poSMCtx);

    /**
     * WPaxos will batch proposal while waiting proposals count reach to BatchCount, or wait time reach to BatchDelayTimeMs.
     * @param groupIdx
     * @param batchCount
     */
    public abstract void setBatchCount(int groupIdx, int batchCount);

	/**
	 * WPaxos will batch proposal while waiting proposals size reach to batchSize, or wait time reach to BatchDelayTimeMs.
	 * @param groupIdx
	 * @param batchSize
	 */
	public abstract void setBatchSize(int groupIdx, int batchSize);
    /**
     * add state machine to all group
     * @param poSM
     */
    public abstract void addStateMachine(StateMachine poSM);
    
    /**
     * add state machine to one group.
     * @param groupIdx
     * @param poSM
     */
    public abstract void addStateMachine(int groupIdx, StateMachine poSM);
    
    /**
     * get one group master version.
     * @param groupIdx
     * @return
     */
    public abstract long getMasterVersion(int groupIdx);
    
    /**
     * set commit timeout mills.
     * @param timeoutMs
     */
    public abstract void setTimeoutMs(int timeoutMs);
    
    /**
     * Set the number you want to keep paxoslog's count.
     * We will only delete paxoslog before checkpoint instanceid.
     * If llHoldCount < 300, we will set it to 300. Not suggest too small holdcount.
     * @param holdCount
     */
    public abstract void setHoldPaxosLogCount(long holdCount);
    
    /**
     * Pause checkpoint replayer.
     * Replayer is to help sm make checkpoint.
     * Checkpoint replayer default is paused, if you not use this, ignord this function.
     * If sm use ExecuteForCheckpoint to make checkpoint, you need to run replayer(you can run in any time).
     */
    public abstract void pauseCheckpointReplayer();

    /**
     * Continue to run replayer
     */
    public abstract void continueCheckpointReplayer();
    
    /**
     * pause paxos log cleaner.
     * Paxos log cleaner working for deleting paxoslog before checkpoint instanceid.
     * Paxos log cleaner default is pausing.
     */
    public abstract void pausePaxosLogCleaner();
    
    /**
     * Continue to run paxos log cleaner.
     */
    public abstract void continuePaxosLogCleaner();
    
    /**
     * Show now membership.
     * @param groupIdx
     * @param nodeInfoList : nodeinfoList to return
     * @return 0 if success 
     */
    public abstract int showMembership(int groupIdx, List<NodeInfo> nodeInfoList);
    
    /**
     * Add a paxos node to membership.
     * @param groupIdx
     * @param node
     * @return 0 if success 
     */
    public abstract int addMember(int groupIdx, NodeInfo node);
    
    /**
     * Remove a paxos node from membership.
     * @param groupIdx
     * @param node
     * @return 0 if success 
     */
    public abstract int removeMember(int groupIdx, NodeInfo node);
    
    /**
     * Change membership by one node to another node.
     * @param groupIdx
     * @param fromNode
     * @param oToNode
     * @return 0 if success
     */
    public abstract int changeMember(int groupIdx, NodeInfo fromNode, NodeInfo oToNode);
    
    /**
     * get who is master.
     * @param groupIdx
     * @return master node
     */
    public abstract NodeInfo getMaster(int groupIdx);
    
    /**
     * Check who is master and get version.
     * @param groupIdx
     * @param masterInfo
     * @return master node info
     */
    public abstract NodeInfo getMasterWithVersion(int groupIdx, MasterInfo masterInfo);
    
    /**
     * Check is i'm master.
     * @param groupIdx
     * @return
     */
    public abstract boolean isIMMaster(int groupIdx);
    
    /**
     * Check whether has master.
     * @param groupIdx
     * @return
     */
    public abstract boolean isNoMaster(int groupIdx);

    /**
     * set master lease time
     * @param groupIdx
     * @param leaseTimeMs
     * @return 0 if success
     */
    public abstract int setMasterLease(int groupIdx, int leaseTimeMs);

    /**
     * drop master
     * @param groupIdx
     * @return 0 if success
     */
    public abstract int dropMaster(int groupIdx);
    
    /**
     * try to be master.
     * @param groupIdx
     * @param leaseTime, master lease time
     * @return 0 if success
     */
    public abstract int toBeMaster(int groupIdx, int leaseTime);
    
    /**
     * try to be master, default leaseTime.
     * @param groupIdx
     * @return
     */
    public abstract int toBeMaster(int groupIdx);
    
    /**
     * Qos
     * If many threads propose same group, that some threads will be on waiting status.
     * Set max hold threads, and we will reject some propose request to avoid to many threads be holded.
     * Reject propose request will get retcode(PaxosTryCommitRet_TooManyThreadWaiting_Reject), check on def.h.
     * @param groupIdx
     * @param maxHoldThreads
     */
    public abstract void setMaxHoldThreads(int groupIdx, int maxHoldThreads);
    
    /**
     * set propose waittime thresholdms
     * To avoid threads be holded too long time, we use this threshold to reject some propose to control thread's wait time.
     * @param groupIdx
     * @param waitTimeThresholdMS
     */
    public abstract void setProposeWaitTimeThresholdMS(int groupIdx, int waitTimeThresholdMS);
    
    /**
     * write disk
     * @param groupIdx
     * @param bLogSync
     */
    public abstract void setLogSync(int groupIdx, boolean bLogSync);
    
    /**
     * Not suggest to use this function
     * pair: value,smid.
     * Because of BatchPropose, a InstanceID maybe include multi-value.
     * @param groupIdx
     * @param instanceID
     * @param valueList
     * @return 0 if success
     */
    public abstract int getInstanceValue(int groupIdx, long instanceID, List<PaxosValue> valueList);

    /**
     * when receive message callback
     * @param receivemsg
     * @return
     */
    public abstract int onReceiveMessage(ReceiveMessage receivemsg);
    
    /**
     * stop all paxos instance
     */
    public abstract void stopPaxos();

    /**
     * get master manager
     * @param groupIdx
     * @return
     */
	public abstract MasterMgr getMasterMgr(int groupIdx);

	/**
	 * get log storage
	 * @return
	 */
	public abstract LogStorage getLogStorage();

	/**
	 * whether this group is learning
	 * @param groupIdx
	 * @return
	 */
	public abstract boolean isLearning(int groupIdx);
}
