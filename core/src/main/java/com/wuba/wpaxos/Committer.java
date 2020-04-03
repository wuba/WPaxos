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
package com.wuba.wpaxos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.comm.breakpoint.Breakpoint;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.config.Def;
import com.wuba.wpaxos.config.PaxosTryCommitRet;
import com.wuba.wpaxos.helper.WaitLock;
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.storemachine.SMFac;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import com.wuba.wpaxos.utils.OtherUtils;
import com.wuba.wpaxos.utils.TimeStat;

/**
 * Committer
 */
public class Committer {
	private final Logger logger = LogManager.getLogger(Committer.class); 
	private Config config;
	private CommitCtx commitCtx;
	private IoLoop ioLoop;
	private SMFac smFac;
	private WaitLock waitLock = new WaitLock();
	private int timeoutMs;
	private long lastLogTime;
	
	public Committer(Config config, CommitCtx commitCtx, IoLoop ioLoop, SMFac smFac,int commitTimeout) {
		super();
		this.config = config;
		this.commitCtx = commitCtx;
		this.ioLoop = ioLoop;
		this.smFac = smFac;
		this.timeoutMs = commitTimeout;
		this.lastLogTime = OtherUtils.getSystemMS();
	}
	
	public CommitResult newValueGetID(byte[] sValue, JavaOriTypeWrapper<Long> instanceIdWrap) {
		return newValueGetID(sValue, instanceIdWrap, null);
	}
	
	public CommitResult newValueGetID(byte[] sValue, JavaOriTypeWrapper<Long> instanceIdWrap, SMCtx smCtx) {
		Breakpoint.getInstance().getCommiterBP().newValue(this.config.getMyGroupIdx());
		CommitResult commitRet = new CommitResult(PaxosTryCommitRet.PaxosTryCommitRet_OK.getRet(), instanceIdWrap.getValue());
		int retryCount = 3;
		while (retryCount > 0) {
			TimeStat timestat = new TimeStat();
			timestat.point();
			
			commitRet = newValueGetIDNoRetry(sValue, instanceIdWrap, smCtx);
			if (commitRet.getCommitRet() != PaxosTryCommitRet.PaxosTryCommitRet_Conflict.getRet()) {
				if (commitRet.getCommitRet() == 0) {
					Breakpoint.getInstance().getCommiterBP().newValueCommitOK(timestat.point(), this.config.getMyGroupIdx());
				} else {
					Breakpoint.getInstance().getCommiterBP().newValueCommitFail(this.config.getMyGroupIdx(), instanceIdWrap.getValue());
				}
				break;
			}
			
			Breakpoint.getInstance().getCommiterBP().newValueConflict(this.config.getMyGroupIdx(), instanceIdWrap.getValue());
			
			if (smCtx != null && smCtx.getSmId() == Def.MASTER_V_SMID) {
				break;
			}
			retryCount--;
		}

		return commitRet;
	}
	
	public CommitResult newValueGetIDNoRetry(byte[] sValue, JavaOriTypeWrapper<Long> instanceIdWrap, SMCtx smCtx) {
		logStatus();
		CommitResult commitRet = new CommitResult(-1, instanceIdWrap.getValue());
		int lockUseTimeMS = 0;
		long beginLock = OtherUtils.getSystemMS();
		boolean hasLock = this.waitLock.lock(this.timeoutMs);
		long endLock = OtherUtils.getSystemMS();
		lockUseTimeMS = (int) (hasLock && (endLock > beginLock) ? (endLock - beginLock) : 0);
		
		if (!hasLock) {
			if (lockUseTimeMS > 0) {
				Breakpoint.getInstance().getCommiterBP().newValueGetLockTimeout(this.config.getMyGroupIdx());
				logger.error("try get lock, but timeout, lockusetime {}, groupId {}.", lockUseTimeMS, this.config.getMyGroupIdx());
				commitRet.setCommitRet(PaxosTryCommitRet.PaxosTryCommitRet_Timeout.getRet());
				logger.info("wait threads {} avg thread wait ms {} reject rate {}, groupIdx {} wait threads set {}.", this.waitLock.getNowHoldThreadCount(), this.waitLock.getNowAvgThreadWaitTime(),
						this.waitLock.getNowRejectRate(), this.config.getMyGroupIdx(), this.waitLock.getWaitThdSet());
				return commitRet;
			} else {
				Breakpoint.getInstance().getCommiterBP().newValueGetLockReject(this.config.getMyGroupIdx());
				logger.error("try get lock, but too many thread waiting, reject, groupId {}.", this.config.getMyGroupIdx());
				commitRet.setCommitRet(PaxosTryCommitRet.PaxosTryCommitRet_TooManyThreadWaiting_Reject.getRet());
				logger.info("wait threads {} avg thread wait ms {} reject rate {}, groupIdx {} wait threads set {}.", this.waitLock.getNowHoldThreadCount(), this.waitLock.getNowAvgThreadWaitTime(),
						this.waitLock.getNowRejectRate(), this.config.getMyGroupIdx(), this.waitLock.getWaitThdSet());
				return commitRet;
			}
		}
		
		int leftTimeoutMS = -1;
		if (this.timeoutMs > 0) {
			leftTimeoutMS = this.timeoutMs > lockUseTimeMS ? (this.timeoutMs - lockUseTimeMS) : 0;
			if (leftTimeoutMS < 200) {
				logger.error("get lock ok, but lockusetime {} too long, lefttimeout {}.", lockUseTimeMS, leftTimeoutMS);
				
				Breakpoint.getInstance().getCommiterBP().newValueGetLockTimeout(this.config.getMyGroupIdx());
				this.waitLock.unLock();
				commitRet.setCommitRet(PaxosTryCommitRet.PaxosTryCommitRet_Timeout.getRet());
				return commitRet;
			}
		}
		
		logger.debug("getlock ok, use time {}.", lockUseTimeMS);
		
		Breakpoint.getInstance().getCommiterBP().newValueCommitOK(lockUseTimeMS, this.config.getMyGroupIdx());
		
		int smID = smCtx != null ? smCtx.getSmId() : 0;
		
		byte[] packSMIDValue = this.smFac.packPaxosValue(sValue, sValue.length, smID);
		
		this.commitCtx.newCommit(packSMIDValue, smCtx, leftTimeoutMS);
		this.ioLoop.addNotify();
		
		commitRet = this.commitCtx.getResult();
		instanceIdWrap.setValue(commitRet.getSuccInstanceID());
		
		this.waitLock.unLock();	
		return commitRet;	
	}
	
	public int newValue(byte[] sValue) {
		JavaOriTypeWrapper<Long> instanceIdWrap = new JavaOriTypeWrapper<Long>();
		instanceIdWrap.setValue(0L);
		CommitResult commitRet = newValueGetID(sValue, instanceIdWrap, null);
		return commitRet.getCommitRet();
	}
	
	public void setTimeoutMs(int timeoutMs) {
		this.timeoutMs = timeoutMs;
	}
	
	public void setMaxHoldThreads(int maxHoldThreads) {
		this.waitLock.setMaxWaitLogCount(maxHoldThreads);
	}
	
	public void setProposeWaitTimeThresholdMS(int waitTimeThresholdMS) {
		this.waitLock.setLockWaitTimeThreshold(waitTimeThresholdMS);
	}
	
	public void logStatus() {
		long nowTime = OtherUtils.getSystemMS();
		if (nowTime > this.lastLogTime && nowTime - this.lastLogTime > 30000) {
			this.lastLogTime = nowTime;
			logger.info("wait threads {} avg thread wait ms {} reject rate {}, groupIdx {} wait threads set {}.", this.waitLock.getNowHoldThreadCount(), this.waitLock.getNowAvgThreadWaitTime(),
	                this.waitLock.getNowRejectRate(), this.config.getMyGroupIdx(), this.waitLock.getWaitThdSet());
		}
	}
}
