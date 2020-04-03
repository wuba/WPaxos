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
package com.wuba.wpaxos.helper;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.utils.OtherUtils;

/**
 * 在serialLock基础上，封装了锁等待队列状态
 */
public class WaitLock {
	private static final Logger logger = LogManager.getLogger(WaitLock.class);
	private SerialLock serialLock = new SerialLock();
	private volatile boolean isLockUsing;
	private AtomicInteger waitLockCount = new AtomicInteger();
	private AtomicInteger maxWaitLockCount = new AtomicInteger();
	private AtomicInteger lockUseTimeSum = new AtomicInteger();
	private AtomicInteger avgLockUseTime = new AtomicInteger();
	private AtomicInteger lockUseTimeCount = new AtomicInteger();
	private volatile int rejectRate;
	private int lockWaitTimeThresholdMS;
	public static final int WAIT_LOCK_USERTIME_AVG_INTERVAL = 250;
	private Set<Long> waitThdSet = new HashSet<Long>();

	public WaitLock() {
		super();
		this.isLockUsing = false;
		this.waitLockCount = new AtomicInteger();
		this.maxWaitLockCount = new AtomicInteger(-1);
		this.lockUseTimeSum = new AtomicInteger();
		this.avgLockUseTime = new AtomicInteger();
		this.lockUseTimeCount = new AtomicInteger();
		this.rejectRate = 0;
		this.lockWaitTimeThresholdMS = -1;
	}

	public boolean canLock() {
		if (this.maxWaitLockCount.get() != -1 && this.waitLockCount.get() >= this.maxWaitLockCount.get()) {
			return false;
		}
		
		if (this.lockWaitTimeThresholdMS == -1) {
			return true;
		}
		
		return (OtherUtils.fastRand() % 100) >= this.rejectRate;
	}
	
	public boolean lock(int timeoutMS) {
		long beginTime = OtherUtils.getSystemMS();
		int useTimeMS = 0;
		
		this.serialLock.lock();
		if (!canLock()) {
			this.serialLock.unLock();
			return false;
		}
		
		waitThdSet.add(Thread.currentThread().getId());
		this.waitLockCount.incrementAndGet();
		boolean bGetLock = true;
		
		while(isLockUsing) {
			try {
				if (timeoutMS == -1) {
					this.serialLock.waitTime(1000);
					continue;
				} else {
					if (!this.serialLock.waitTime(timeoutMS)) {
						bGetLock = false;
						break;
					}
				}
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		}
		
		this.waitLockCount.decrementAndGet();
		
		long endTime = OtherUtils.getSystemMS();
		useTimeMS = (int) (endTime > beginTime ? (endTime - beginTime) : 0);
		
		refleshRejectRate(useTimeMS);
		
		if (bGetLock) {
			this.isLockUsing = true;
		}
		this.serialLock.unLock();
		
		return bGetLock;
	}
	
	public void unLock() {
		this.serialLock.lock();
		this.isLockUsing = false;
		this.serialLock.interupt();
		this.serialLock.unLock();
		this.waitThdSet.remove(Thread.currentThread().getId());
	}
	
	public void setMaxWaitLogCount(int maxWaitLockCount) {
		this.maxWaitLockCount = new AtomicInteger(maxWaitLockCount);
	}
	
	public void setLockWaitTimeThreshold(int lockWaitTimeThresholdMS) {
		this.lockWaitTimeThresholdMS = lockWaitTimeThresholdMS;
	}
	
    public int getNowHoldThreadCount() {
    	return this.waitLockCount.get();
    }

    public int getNowAvgThreadWaitTime() {
    	return this.avgLockUseTime.get();
    }

    public int getNowRejectRate() {
    	return this.rejectRate;
    }

    public Set<Long> getWaitThdSet() {
		return waitThdSet;
	}

	public void setWaitThdSet(Set<Long> waitThdSet) {
		this.waitThdSet = waitThdSet;
	}

	public void refleshRejectRate(int useTimeMs) {
    	if (lockWaitTimeThresholdMS == -1) {
    		return;
    	}
    	
    	this.lockUseTimeSum.addAndGet(useTimeMs);
    	this.lockUseTimeCount.incrementAndGet();
    	
    	if (this.lockUseTimeCount.get() >= WAIT_LOCK_USERTIME_AVG_INTERVAL) {
    		this.avgLockUseTime.set(this.lockUseTimeSum.get() / this.lockUseTimeCount.get());
    		this.lockUseTimeSum = new AtomicInteger();
    		this.lockUseTimeCount = new AtomicInteger();
    		
    		if (this.avgLockUseTime.get() > this.lockWaitTimeThresholdMS) {
    			if (this.rejectRate != 98) {
    				this.rejectRate = this.rejectRate + 3 > 98 ? 98 : this.rejectRate + 3;
    			}
    		} else {
    			if (this.rejectRate != 0) {
    				this.rejectRate = this.rejectRate -3 < 0 ? 0 : this.rejectRate -3;
    			}
    		}
    	}
    }
}
