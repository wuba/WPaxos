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

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.RateLimiter;
import com.wuba.wpaxos.base.BallotNumber;
import com.wuba.wpaxos.comm.InsideOptions;
import com.wuba.wpaxos.comm.breakpoint.Breakpoint;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.helper.SerialLock;
import com.wuba.wpaxos.proto.AcceptorStateData;
import com.wuba.wpaxos.store.PaxosLog;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import com.wuba.wpaxos.utils.Time;

/**
 * LearnerSender
 */
public class LearnerSender implements Runnable {
	private static final Logger logger = LogManager.getLogger(LearnerSender.class);
	private Config config;
	private Learner learner;
	private PaxosLog paxosLog;
	private SerialLock lock;
	private volatile boolean isIMSending;
	private volatile long absLastSendTime;
	private volatile long beginInstanceID;
	private volatile long sendToNodeID;
	private volatile boolean isComfirmed;
	private volatile long ackInstanceID;
	private volatile long absLastAckTime;
	private volatile int ackLead;
	private volatile boolean isEnd;
	private volatile boolean isStart;
	private static volatile RateLimiter limiter = null;
	private static volatile int LEARNER_SENDER_SPEED = 100 * 1024 * 1024;
	
	public LearnerSender(Config config, Learner learner, PaxosLog paxosLog) {
		this.config = config;
		if(limiter == null) {
			synchronized (LearnerSender.class) {
				if(limiter == null) {
					limiter = RateLimiter.create(this.config.getLearnerSendSpeed(), 100, TimeUnit.MILLISECONDS);
				}
			}
		}
		
		this.learner = learner;
		this.paxosLog = paxosLog;
		this.ackLead = InsideOptions.getInstance().getLearnerSenderAckLead();
		this.isEnd = false;
		this.isStart = false;
		this.lock = new SerialLock();
		sendDone();
	}
	
	public static void setLearnerSenderSpeed(int speed) {
		if(speed == LEARNER_SENDER_SPEED || speed == 0) return;
		LEARNER_SENDER_SPEED = speed;
		limiter = RateLimiter.create(speed, 100, TimeUnit.MILLISECONDS);
		logger.info("learner sender set new speed {} success", speed);
	}

	@Override
	public void run() {
		this.isStart = true;
		while(true) {
			try {
				waitToSend();
				
				if(this.isEnd) {
					logger.warn("Learner.Sender [END]");
					return ;
				}
				
				sendLearnedValue(beginInstanceID, sendToNodeID);
				sendDone();
			} catch (Exception e) {
				logger.error("run throws exception", e);
			}
		}
	}

	public void stop() throws Exception {
		if(this.isStart) {
			this.isEnd = true;
			Thread.currentThread().join();
		}
	}
	
	public boolean prepare(long beginInstanceID, long sendToNodeID) {
		this.lock.lock();
		
		boolean prepareRet = false;
		if(!isIMSending() && !this.isComfirmed) {
			prepareRet = true;
			this.isIMSending = true;
			this.absLastSendTime = this.absLastAckTime = Time.getSteadyClockMS();
			this.beginInstanceID = this.ackInstanceID = beginInstanceID;
			this.sendToNodeID = sendToNodeID;
		}
		
		this.lock.unLock();
		return prepareRet;
	}

	public boolean comfirm(long beginInstanceID, long sendToNodeID) {
		this.lock.lock();
		boolean confirmRet = false;
		
		if(isIMSending() && !this.isComfirmed) {
			if(this.beginInstanceID == beginInstanceID && this.sendToNodeID == sendToNodeID) {
				confirmRet = true;
				this.isComfirmed = true;
				this.lock.interupt();
			}
		}
		
		this.lock.unLock();
		return confirmRet;
	}

	public void ack(long ackInstanceID, long fromNodeID) {
		this.lock.lock();
		
		if(isIMSending() && this.isComfirmed) {
			if(this.sendToNodeID == fromNodeID) {
				if(ackInstanceID > this.ackInstanceID) {
					this.ackInstanceID = ackInstanceID;
					this.absLastAckTime = Time.getSteadyClockMS();
					this.lock.interupt();
				}
			}
		}
		this.lock.unLock();
	}

	public void waitToSend() {
		this.lock.lock();
		while(!this.isComfirmed) {
			try {
				this.lock.waitTime(1000);
			} catch (Exception e) {
				logger.error("waitToSend error" , e);
			}
			if(this.isEnd) break;
		}
		this.lock.unLock();
	}

	public void sendLearnedValue(long beginInstanceID, long sendToNodeID) {
		logger.debug("BeginInstanceID {} SendToNodeID {}.", beginInstanceID, sendToNodeID);
		
		long sendInstanceId = beginInstanceID;
		int ret = 0;
		
		//control send speed to avoid affecting the network too much.
		int sendQps = InsideOptions.getInstance().getLearnerSenderSendQps();
		int sleepMs = sendQps > 1000 ? 1 : 1000 / sendQps;
		
		int sendInterval = sendQps > 1000 ? sendQps / 1000 + 1 : 1;
		
		int sendCount = 0;
		JavaOriTypeWrapper<Integer> lastCheckSumWrap = new JavaOriTypeWrapper<Integer>(0);
		while(sendInstanceId < this.learner.getInstanceID()) {
			ret = sendOne(sendInstanceId, sendToNodeID, lastCheckSumWrap);
			if(ret != 0) {
				logger.error("SendOne fail, SendInstanceID {} SendToNodeID {} ret {}.", sendInstanceId, sendToNodeID, ret);
				return ;
			}
			
			if(!checkAck(sendInstanceId)) return ;
			
			sendCount ++;
			sendInstanceId ++;
			releshSending();
			
			if(sendCount >= sendInterval) {
				sendCount = 0;
				Time.sleep(sleepMs);
			}
		}
		
		//succ send, reset ack lead.
		this.ackLead = InsideOptions.getInstance().getLearnerSenderAckLead();
		logger.debug("SendDone, SendEndInstanceID {}.", sendInstanceId);
	}
	
	public int sendOne(long sendInstanceID, long sendToNodeID, JavaOriTypeWrapper<Integer> lastChecksum) {
		Breakpoint.getInstance().getLearnerBP().senderSendOnePaxosLog(this.config.getMyGroupIdx());
		
		AcceptorStateData state = new AcceptorStateData();
		int ret = this.paxosLog.readState(this.config.getMyGroupIdx(), sendInstanceID, state);
		if(ret != 0) {
			return ret;
		}
		
		//限速
		byte[] sendValue = state.getAcceptedValue();
		if(sendValue != null && sendValue.length > 0) {
			limiter.acquire(sendValue.length);
		}
		
		BallotNumber ballot = new BallotNumber(state.getAcceptedID(), state.getAcceptedNodeID());
		ret = this.learner.sendLearnValue(sendToNodeID, sendInstanceID, ballot, state.getAcceptedValue(), lastChecksum.getValue(), true);		
		lastChecksum.setValue(state.getCheckSum());
		return ret;
	}

	public void sendDone() {
		this.lock.lock();
		this.isIMSending = false;
		this.isComfirmed = false;
		this.beginInstanceID = (long) - 1;
		this.sendToNodeID = 0;
		this.absLastSendTime = 0;
		this.ackInstanceID = 0;
		this.absLastAckTime = 0;
		this.lock.unLock();
	}

	/**
	 * 判断是否在发送中，没有发送或者已经超时均算为未发送状态。
	 * @return
	 */
	public boolean isIMSending() {
		if(!this.isIMSending) {
			return false;
		}
		
		long nowTime = Time.getSteadyClockMS();
		long passTime = nowTime > this.absLastSendTime ? nowTime - this.absLastSendTime : 0;
		if(passTime >= InsideOptions.getInstance().getLearnerSenderPrepareTimeoutMs()) {
			return false;
		}
		
		return true;
	}

	public void releshSending() {
		this.absLastSendTime = Time.getSteadyClockMS();
	}

	public boolean checkAck(long sendInstanceID) {
		this.lock.lock();
		
		if(sendInstanceID < this.ackInstanceID) {
			this.ackLead = InsideOptions.getInstance().getLearnerSenderAckLead();
			logger.info("Already catch up, ack instanceid {} now send instanceid {}.", this.ackInstanceID, sendInstanceID);
			this.lock.unLock();
			return false;
		}
		
		while(sendInstanceID > (this.ackInstanceID + this.ackLead)) {
			long nowTime = Time.getSteadyClockMS();
			long passTime = nowTime > this.absLastAckTime ? nowTime - this.absLastAckTime : 0;
			
			if(passTime > InsideOptions.getInstance().getLearnerSenderAckTimeoutMs()) {
				Breakpoint.getInstance().getLearnerBP().senderAckTimeout(this.config.getMyGroupIdx());
				logger.error("Ack timeout, last acktime {} now send instanceid {}.", this.absLastAckTime, sendInstanceID);
				
				cutAckLead();
				this.lock.unLock();
				return false;
			}
			
			Breakpoint.getInstance().getLearnerBP().senderAckDelay(this.config.getMyGroupIdx());
			
			try {
				this.lock.waitTime(20);
			} catch (Exception e) {
				logger.error("checkAck lock wait error", e);
			}
		}
		
		this.lock.unLock();
		return true;
	}

	public void cutAckLead() {
		int receiveAckLead = InsideOptions.getInstance().getLearnerReceiverAckLead();
		if(this.ackLead - receiveAckLead > receiveAckLead) {
			this.ackLead = this.ackLead - receiveAckLead;
		}
	}

}
















