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
package com.wuba.wpaxos.master;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.comm.MasterChangeCallback;
import com.wuba.wpaxos.comm.breakpoint.Breakpoint;
import com.wuba.wpaxos.config.Def;
import com.wuba.wpaxos.node.Node;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.utils.MasterGroupStatPrinter;
import com.wuba.wpaxos.utils.OtherUtils;

/**
 * master manager
 */
public class MasterMgr extends Thread {
	private final Logger logger = LogManager.getLogger(MasterMgr.class); 
	private int leaseTime;
    private boolean isEnd;
    private boolean isStarted;
    private int myGroupIdx;
    private boolean needDropMaster;
    private Node paxosNode;
    private MasterStateMachine defaultMasterSM;
    
    public MasterMgr(Node paxosNode, int groupIdx, LogStorage logStorage, MasterChangeCallback masterChangeCallback) {
		defaultMasterSM = new MasterStateMachine(logStorage, paxosNode.getMyNodeID(), groupIdx, masterChangeCallback);
		leaseTime = 10000;
		this.paxosNode = paxosNode;
		this.myGroupIdx = groupIdx;
		this.isEnd = false;
		this.isStarted = false;
		this.needDropMaster = false;
	}
    
    public void runMaster() {
    	start();
    }
    
    public void stopMaster() {
    	if (isStarted) {
    		isEnd = true;
    		try {
				this.join();
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
    	}
    }

    public int init() {
    	return defaultMasterSM.init();
    }

    @Override
    public void run() {
    	this.isStarted = true;
    	
    	while(true) {
    		if (isEnd) {
    			logger.info("MasterMgr stop..., groupid : {}.", this.myGroupIdx);
    			return;
    		}
    		
    		int nLeaseTime = this.leaseTime;
    		long beginTime = System.currentTimeMillis();
    		
    		tryBeMaster(nLeaseTime);
    		int continueLeaseTimeout = (nLeaseTime - 100) / 4;
    		continueLeaseTimeout = continueLeaseTimeout / 2 + OtherUtils.fastRand() % continueLeaseTimeout;
    		
    		if (this.needDropMaster) {
    			Breakpoint.getInstance().getMasterBP().dropMaster(this.myGroupIdx);
    			this.needDropMaster = false;
    			continueLeaseTimeout = nLeaseTime * 2;
    			logger.debug("Need drop master, this round wait time {}.", continueLeaseTimeout);
    		}

    		long endTime = System.currentTimeMillis();
    		int runTime = (int) (endTime > beginTime ? (endTime - beginTime) : 0);
    		int needSleepTime = continueLeaseTimeout > runTime ? (continueLeaseTimeout - runTime) : 0;

    		logger.debug("TryBeMaster, sleep time {}.", needSleepTime);
    		try {
				Thread.sleep(needSleepTime);
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
    	}
    }

    public void setLeaseTime(int leaseTimeMs) {
    	if (leaseTimeMs < 1000) {
    		return;
    	}
    	
    	this.leaseTime = leaseTimeMs;
    }

    public void tryBeMaster(int leaseTime) {
    	MasterInfo masterInfo = new MasterInfo();
    	// TODO
    	logger.debug("tryBeMaster......");
    	defaultMasterSM.safeGetMaster(masterInfo);
    	logger.debug("safeGetMaster......");
    	
    	if ((masterInfo.getMasterNodeID() != 0) && (masterInfo.getMasterNodeID() != paxosNode.getMyNodeID())) {
    		logger.debug("other is master, can't try be master, masterid {} myid {}.", masterInfo.getMasterNodeID(), paxosNode.getMyNodeID());
    		return;
    	} 
    	
    	if (masterInfo.getMasterNodeID() == paxosNode.getMyNodeID()) {
    		logger.debug("I'm master, group id {}.", this.myGroupIdx);
    		MasterGroupStatPrinter.put(this.myGroupIdx);
    	}
    	
    	Breakpoint.getInstance().getMasterBP().tryBeMaster(this.myGroupIdx);
    	
    	byte[] pValue = MasterStateMachine.makeOpValue(paxosNode.getMyNodeID(), masterInfo.getMasterVersion(), leaseTime, MasterOperatorType.MasterOperatorType_Complete);
    	if (null == pValue || pValue.length == 0) {
    		logger.error("Make paxos value failed.");
    		return;
    	}
    	
    	int masterLeaseTimeout = leaseTime - 100;
    	long absMasterTimeout = System.currentTimeMillis() + masterLeaseTimeout;
    	SMCtx ctx = new SMCtx();
    	ctx.setSmId(Def.MASTER_V_SMID);
    	ctx.setpCtx(absMasterTimeout);
    	int ret = paxosNode.propose(myGroupIdx, pValue, ctx).getResult();
    	if (ret != 0) {
    		Breakpoint.getInstance().getMasterBP().tryBeMasterProposeFail(this.myGroupIdx);
    	}
    }

    public int toBeMaster() {
    	return toBeMaster(this.leaseTime);
    }
    
    public int toBeMaster(int leaseTime) {
    	MasterInfo masterInfo = new MasterInfo();
    	defaultMasterSM.safeGetMaster(masterInfo);
    	logger.debug("safeGetMaster......");
    	
    	Breakpoint.getInstance().getMasterBP().toBeMaster(this.myGroupIdx);
    	
    	byte[] pValue = MasterStateMachine.makeOpValue(paxosNode.getMyNodeID(), masterInfo.getMasterVersion(), leaseTime, MasterOperatorType.MasterOperatorType_Complete);
    	
    	if (null == pValue || pValue.length == 0) {
    		logger.error("Make paxos value failed.");
    		return -1;
    	}
    	
    	int masterLeaseTimeout = leaseTime - 100;
    	long absMasterTimeout = System.currentTimeMillis() + masterLeaseTimeout;  	
    	SMCtx ctx = new SMCtx();
    	ctx.setSmId(Def.MASTER_V_SMID);
    	ctx.setpCtx(absMasterTimeout);
    	int ret = paxosNode.propose(myGroupIdx, pValue, ctx).getResult();
    	if (ret != 0) {
    		Breakpoint.getInstance().getMasterBP().tryBeMasterProposeFail(this.myGroupIdx);
    	}
    	
    	return ret;
    }
    
    public void dropMaster() {
    	this.needDropMaster = true;
    }

    public MasterStateMachine getMasterSM() {
    	return this.defaultMasterSM;
    }
    
}
