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

import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.config.PaxosTryCommitRet;
import com.wuba.wpaxos.helper.SerialLock;
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.utils.Crc32;
import com.wuba.wpaxos.utils.OtherUtils;

/**
 * commiter当前最新状态
 */
public class CommitCtx {
	private final Logger logger = LogManager.getLogger(CommitCtx.class); 
	private Config config;
	private volatile long instanceID;
	private int commitRet;
	private boolean isCommitEnd;
	private int timeoutMs;
	private byte[] psValue;
	private SMCtx smCtx;
	private SerialLock serialLock = new SerialLock();
	
	public CommitCtx(Config config) {
		this.config = config;
		this.newCommit(null, null, 0);
	}
	
	public void newCommit(byte[] psValue, SMCtx pSMCtx, int timeoutMs) {
		serialLock.lock();
		this.instanceID = -1;
		this.commitRet = -1;
		this.isCommitEnd = false;
		this.timeoutMs = timeoutMs;
		
		this.psValue = psValue;
		this.smCtx = pSMCtx;
		
		if (psValue != null) {
			logger.debug("OK, valuesize {}.", psValue.length);
		}
		this.serialLock.unLock();
	}
	
	public boolean isNewCommit() {
		return this.instanceID == (long)-1 && this.psValue != null;
	}
	
	public byte[] getCommitValue() {
		return this.psValue;
	}
	
	public void startCommit(long instanceID) {
		serialLock.lock();
		this.instanceID = instanceID;
		serialLock.unLock();
	}
	
	public boolean isMycommit(long instanceID, byte[] sLearnValue, SMCtx smCtx) {
		serialLock.lock();
		boolean isMyCommit = false;
		
		try {
			if ((!isCommitEnd) && (this.instanceID == instanceID)) {
				if (psValue.length == sLearnValue.length) {
					int crc1 = Crc32.crc32(psValue);
					int crc2 = Crc32.crc32(sLearnValue);
					if (crc1 == crc2) {
						isMyCommit = true;
					}
				}
			}
			
			if (isMyCommit) {
				smCtx.setpCtx(this.smCtx.getpCtx());
				smCtx.setSmId(this.smCtx.getSmId());
			}
		} catch(Exception e) {
			logger.error("", e);
		} finally {
			this.serialLock.unLock();
		}
		
		return isMyCommit;
	}
	
	public void setResult(int commitRet, long instanceID, byte[] sLearnValue) {
		
		this.serialLock.lock();
		try {
			if (isCommitEnd || (this.instanceID != instanceID)) {
				return;
			}
			
			this.commitRet = commitRet;
			if (this.commitRet == 0) {
				if (psValue.length == sLearnValue.length) {
					int crc1 = Crc32.crc32(psValue);
					int crc2 = Crc32.crc32(sLearnValue);
					if (crc1 != crc2) {
						this.commitRet = PaxosTryCommitRet.PaxosTryCommitRet_Conflict.getRet();
						logger.warn("crc check failed.smid {}", smCtx.getSmId());
					}
				} else {
					this.commitRet = PaxosTryCommitRet.PaxosTryCommitRet_Conflict.getRet();
				}
			}
			
			this.isCommitEnd = true;
			this.psValue = null;
		} catch(Exception e) {
			logger.error(e.getMessage(),e);
		} finally {
			this.serialLock.interupt();
			this.serialLock.unLock();
		}
	}
	
	public void setResultOnlyRet(int commitRet) {
		setResult(commitRet, -1, null);
	}
	
	public CommitResult getResult() {
		long succInstanceID = -1;
		this.serialLock.lock();
		try {
			long start = OtherUtils.getSystemMS();
			while (!isCommitEnd) {
				this.serialLock.waitTime(1000);
			}
			
			if (this.commitRet == 0) {
				logger.debug("commit success, instanceid {}", this.instanceID);
				succInstanceID = this.instanceID;
			} else {
				logger.error("commit failed, ret {}.smid {}", this.commitRet,smCtx.getSmId());
			}
		} catch(Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			this.serialLock.unLock();
		}
		
		CommitResult commitResult = new CommitResult(this.commitRet, succInstanceID);
		return commitResult;
	}
	
	public int getTimeoutMs() {
		return this.timeoutMs;
	}

	public Config getConfig() {
		return config;
	}

	public void setConfig(Config config) {
		this.config = config;
	}

	public void setCommitValue(byte[] psValue) {
		this.psValue = psValue;
	}
}
