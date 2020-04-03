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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.comm.InsideOptions;
import com.wuba.wpaxos.comm.breakpoint.Breakpoint;
import com.wuba.wpaxos.communicate.ReceiveMessage;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.config.Def;
import com.wuba.wpaxos.proto.PaxosMsg;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import com.wuba.wpaxos.utils.Time;
import com.wuba.wpaxos.utils.Timer;
import com.wuba.wpaxos.utils.Timer.TimerObj;

/**
 * Io 任务分发控制
 */
public class IoLoop extends Thread {
	private final Logger logger = LogManager.getLogger(IoLoop.class); 
	private boolean isEnd;
	private Timer timer;
	private Map<Long, Boolean> timerIDExist = new HashMap<Long, Boolean>();
	private LinkedBlockingQueue<ReceiveMessage> messageQueue = new LinkedBlockingQueue<ReceiveMessage>();
	private LinkedBlockingQueue<PaxosMsg> retryQueue = new LinkedBlockingQueue<PaxosMsg>();
	private AtomicInteger queueMemSize = new AtomicInteger();
	private Config poConfig;
	private Instance poInstance;
	public static final int RETRY_QUEUE_MAX_LEN = 300;

	public IoLoop(Config poConfig, Instance poInstance) {
		super();
		this.poConfig = poConfig;
		this.poInstance = poInstance;
		this.isEnd = false;
		this.timer = new Timer();
	}

	@Override
	public void run() {
		this.isEnd = false;
		while(true) {
			try {
				int nextTimeout = 5000;
				nextTimeout = dealwithTimeout(nextTimeout);
				
				oneLoop(nextTimeout);
				
				if (this.isEnd) {
					logger.info("IOLoop [End]");
					break;
				}
			} catch (Throwable th) {
				logger.error("io loop error", th);
			}
		}
	}
	
	public void shutdown() {
		if (!this.isEnd) {
			this.isEnd = true;
			try {
				this.join();
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	public void oneLoop(int timeoutMs) {
		ReceiveMessage receiveMsg = null;
		
		try {
			receiveMsg = this.messageQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
			if (receiveMsg != null && receiveMsg.getReceiveLen() > 0 && !receiveMsg.isNotifyMsg()) {
				this.queueMemSize.addAndGet(-receiveMsg.getReceiveLen());
				this.poInstance.onReceive(receiveMsg);
				Breakpoint.getInstance().getIOLoopBP().outQueueMsg(this.poConfig.getMyGroupIdx());
			}
			
			dealWithRetry();
			
			this.poInstance.checkNewValue();
		} catch(Exception e) {
			logger.error("oneLoop error" , e);
		}
	}

	public void dealWithRetry() {
		if (this.retryQueue.isEmpty()) {
			return;
		}
		
		boolean haveRetryOne = false;
		while(!this.retryQueue.isEmpty()) {
			try {
				PaxosMsg paxosMsg = this.retryQueue.peek();
				if (paxosMsg.getInstanceID() > this.poInstance.getNowInstanceID() + 1) {
					break;
				} else if (paxosMsg.getInstanceID() == this.poInstance.getNowInstanceID() + 1) {
					if (haveRetryOne) {
						Breakpoint.getInstance().getIOLoopBP().dealWithRetryMsg(this.poConfig.getMyGroupIdx());
						logger.debug("retry msg (i+1). instanceid {}.", paxosMsg.getInstanceID());
						this.poInstance.onReceivePaxosMsg(paxosMsg, true);
					} else {
						break;
					}
				} else if (paxosMsg.getInstanceID() == this.poInstance.getNowInstanceID()) {
					Breakpoint.getInstance().getIOLoopBP().dealWithRetryMsg(this.poConfig.getMyGroupIdx());
					logger.debug("retry msg. instanceid {}.", paxosMsg.getInstanceID());
					this.poInstance.onReceivePaxosMsg(paxosMsg, false);
					haveRetryOne = true;
				}
				
				this.retryQueue.poll();
			} catch(Exception e) {
				logger.error("ioloop dealWithRetry error", e);
			}
		}
	}

	public void clearRetryQueue() {
		while (!this.retryQueue.isEmpty()) {
			this.retryQueue.poll();
		}
	}

	public int addMessage(ReceiveMessage receiveMsg) {
		// every group send message with same channel, needn't lock;
		try {
			Breakpoint.getInstance().getIOLoopBP().enqueueMsg(this.poConfig.getMyGroupIdx());
			
			if (this.messageQueue.size() > InsideOptions.getInstance().getMaxIOLoopQueueLen()) {
				Breakpoint.getInstance().getIOLoopBP().enqueueMsgRejectByFullQueue(this.poConfig.getMyGroupIdx());
				logger.error("Queue full, skip msg.");
				return -2;
			}
			
			if (this.queueMemSize.get() > Def.MAX_QUEUE_MEM) {
				logger.error("Queue memsize {} too large, can't enqueue", this.queueMemSize);
				return -2;
			}
			
			this.messageQueue.offer(receiveMsg);
			this.queueMemSize.addAndGet(receiveMsg.getReceiveLen());
			return 0;
		} catch(Exception e) {
			logger.error("ioloop addMessage error",e);
		}
		return -2;
	}

	public int addRetryPaxosMsg(PaxosMsg paxosMsg) {
		Breakpoint.getInstance().getIOLoopBP().enqueueRetryMsg(this.poConfig.getMyGroupIdx());
		
		if (this.retryQueue.size() > RETRY_QUEUE_MAX_LEN) {
			Breakpoint.getInstance().getIOLoopBP().enqueueMsgRejectByFullQueue(this.poConfig.getMyGroupIdx());
			this.retryQueue.poll();
		}
		
		this.retryQueue.offer(paxosMsg);
		return 0;
	}

	public void addNotify() {
		this.messageQueue.offer(ReceiveMessage.getNotifyNullMsg());
	}

	public boolean addTimer(int timeout, int type, JavaOriTypeWrapper<Long> timerID) {
		if (timeout == -1) {
			return true;
		}
		
		long absTime = Time.getSteadyClockMS() + timeout;
		this.timer.addTimerWithType(absTime, type, timerID);
		this.timerIDExist.put(timerID.getValue(), true);

		this.addNotify();
		
		return true;
	}

	public void removeTimer(JavaOriTypeWrapper<Long> timerID) {
		if (this.timerIDExist.containsKey(timerID.getValue())) {
			this.timerIDExist.remove(timerID.getValue());
		}
		
		timerID.setValue(0L);
	}

	public int dealwithTimeout(int nextTimeout) {
		boolean hasTimeout = true;
		
		while (hasTimeout) {
			
			TimerObj timerObj = new TimerObj();
			hasTimeout = this.timer.popTimeout(timerObj);
			
			if (hasTimeout) {
				dealwithTimeoutOne(timerObj.getTimerID(), timerObj.getType());
				nextTimeout = this.timer.getNextTimeout(nextTimeout);
				if (nextTimeout != 0) {
					break;
				}
			}else {
				nextTimeout = this.timer.getNextTimeout(nextTimeout);
			}
		}
		
		return nextTimeout;
	}

	public void dealwithTimeoutOne(long timerID, int type) {
		if (!this.timerIDExist.containsKey(timerID)) {
			return;
		}
		
		this.timerIDExist.remove(timerID);
		this.poInstance.onTimeout(timerID, type);
	}
}
