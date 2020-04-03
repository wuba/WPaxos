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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.ProposeResult;
import com.wuba.wpaxos.comm.breakpoint.Breakpoint;
import com.wuba.wpaxos.config.Def;
import com.wuba.wpaxos.config.PaxosNodeFunctionRet;
import com.wuba.wpaxos.config.PaxosTryCommitRet;
import com.wuba.wpaxos.helper.SerialLock;
import com.wuba.wpaxos.proto.BatchPaxosValue;
import com.wuba.wpaxos.proto.PaxosValue;
import com.wuba.wpaxos.storemachine.BatchSMCtx;
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import com.wuba.wpaxos.utils.Notifier;
import com.wuba.wpaxos.utils.NotifierPool;
import com.wuba.wpaxos.utils.OtherUtils;
import com.wuba.wpaxos.utils.TimeStat;

/**
 * batch propose
 */
public class ProposeBatch extends Thread {
	private final Logger logger = LogManager.getLogger(ProposeBatch.class);
	private int myGroupIdx;
	private Node paxosNode;
	private NotifierPool notifierPool;
	private SerialLock serialLock = new SerialLock();
	private volatile boolean isEnd = false;
	private volatile boolean isStarted = false;
	private int batchCount = 20;
	private int batchMaxSize = 512 * 10;
	private int pCount = 0;
	private LinkedBlockingQueue<PendingProposal> pendingQueue = new LinkedBlockingQueue<ProposeBatch.PendingProposal>();

	public ProposeBatch(int groupIdx, Node paxosNode, NotifierPool notifierPool) {
		this.myGroupIdx = groupIdx;
		this.paxosNode = paxosNode;
		this.notifierPool = notifierPool;
	}

	public void shutdown() {
		if (this.isStarted) {
			this.serialLock.lock();
			this.isEnd = true;
			try {
				this.serialLock.interupt();
				this.serialLock.unLock();
				this.join();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			} finally {
				if (this.serialLock.isLocked()) {
					this.serialLock.unLock();
				}
			}
		}
	}

	@Override
	public void run() {
		isStarted = true;
		TimeStat timeStat = new TimeStat();
		while (true) {
			if (this.isEnd) {
				break;
			}

			List<PendingProposal> vecRequest = null;
			serialLock.lock();
			try {
				timeStat.point();

				vecRequest = new LinkedList<PendingProposal>();
				pluckProposal(vecRequest);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			} finally {
				serialLock.unLock();
			}

			if (vecRequest != null && !vecRequest.isEmpty()) {
				doPropose(vecRequest);
			}

		}
		
		try {
			Thread.sleep(5);
		} catch (InterruptedException e1) {
			logger.error("", e1);
		}

		cleanPendingQueue();

		logger.info("ProposeBatch ended.");
	}
	
	public void cleanPendingQueue() {
		serialLock.lock();
		try {
			while (!this.pendingQueue.isEmpty()) {
				PendingProposal pendingProposal = this.pendingQueue.peek();
				try {
					pendingProposal.getNotifier().sendNotify(PaxosNodeFunctionRet.Paxos_SystemError.getRet());
				} catch (IOException e) {
					logger.error("pendingProposal send notify error.", e);
				}
				this.pendingQueue.poll();
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (serialLock.isLocked()) {
				serialLock.unLock();
			}
		}
	}

	public int propose(byte[] sValue, JavaOriTypeWrapper<Long> instanceIDWrap, JavaOriTypeWrapper<Integer> indexIDWrap, SMCtx smCtx) {
		if (this.isEnd) {
			return PaxosNodeFunctionRet.Paxos_SystemError.getRet();
		}

		Breakpoint.getInstance().getCommiterBP().batchPropose(this.myGroupIdx);

		long threadID = Thread.currentThread().getId();
		Notifier notifier = this.notifierPool.getNotifier(threadID);
		if (notifier == null) {
			logger.error("getNotifier failed.");
			Breakpoint.getInstance().getCommiterBP().batchProposeFail(this.myGroupIdx);
			return PaxosNodeFunctionRet.Paxos_SystemError.getRet();
		}
		notifier.init();

		int ret = addProposal(sValue, instanceIDWrap, indexIDWrap, smCtx, notifier);
		if (ret < 0) {
			return PaxosNodeFunctionRet.Paxos_SystemError.getRet();
		}

		ret = notifier.waitNotify();
		if (ret == PaxosTryCommitRet.PaxosTryCommitRet_OK.getRet()) {
			Breakpoint.getInstance().getCommiterBP().batchProposeOK(this.myGroupIdx);
		} else {
			Breakpoint.getInstance().getCommiterBP().batchProposeFail(this.myGroupIdx);
		}

		return ret;
	}

	public void setBatchMaxSize(int batchMaxSize) {
		this.batchMaxSize = batchMaxSize;
	}

	public void setBatchCount(int iBatchCount) {
		this.batchCount = iBatchCount;
	}

	public void doPropose(List<PendingProposal> reqList) {
		if (reqList.size() == 0) {
			return;
		}

		Breakpoint.getInstance().getCommiterBP().batchProposeDoPropose(reqList.size(), this.myGroupIdx);

		if (reqList.size() == 1) {
			logger.debug("paxosNode batch propose groupIdx {}, batch vecReqSize {} ", myGroupIdx, 1);
			onlyOnePropose(reqList.get(0));
			return;
		}

		BatchPaxosValue batchValues = new BatchPaxosValue();
		BatchSMCtx batchSMCtx = new BatchSMCtx();
		for (PendingProposal pendingProposal : reqList) {
			PaxosValue pValue = new PaxosValue();
			pValue.setSmID(pendingProposal.smCtx != null ? pendingProposal.smCtx.getSmId() : 0);
			pValue.setValue(pendingProposal.psValue);
			batchValues.addPaxosValue(pValue);
			batchSMCtx.getSmCtxList().add(pendingProposal.getSmCtx());
		}

		SMCtx smCtx = new SMCtx();
		smCtx.setSmId(Def.BATCH_PROPOSE_SMID);
		smCtx.setpCtx(batchSMCtx);

		byte[] buf;
		ProposeResult pResult = new ProposeResult(0);
		try {
			buf = batchValues.serializeToBytes();

			pResult = this.paxosNode.propose(myGroupIdx, buf, smCtx);
			if (pResult.getResult() != 0) {
				logger.error("real propose failed, ret {}.", pResult.getResult());
			}
			logger.debug("paxosNode batch propose groupIdx {}, batch vecReqSize {} ", myGroupIdx, reqList.size());
		} catch (Exception e) {
			logger.error("BatchValues serialiseToBytes failed.", e);
			pResult.setResult(PaxosNodeFunctionRet.Paxos_SystemError.getRet());
		}

		for (int i = 0; i < reqList.size(); i++) {
			PendingProposal pendingProposal = reqList.get(i);
			pendingProposal.setBatchIndex(i);
			pendingProposal.setInstanceID(pResult.getInstanceID());
			try {
				pendingProposal.getNotifier().sendNotify(pResult.getResult());
			} catch (IOException e) {
				logger.error("pendingProposal sendNotify error.", e);
			}
		}
	}

	public int addProposal(byte[] sValue, JavaOriTypeWrapper<Long> instanceIDWrap, JavaOriTypeWrapper<Integer> indexIDWrap,
	                        SMCtx smCtx, Notifier poNotifier) {
		try {
			PendingProposal pendingProposal = new PendingProposal();
			pendingProposal.notifier = poNotifier;
			pendingProposal.psValue = sValue;
			pendingProposal.smCtx = smCtx;
			pendingProposal.instanceIDWrap = instanceIDWrap;
			pendingProposal.batchIndexWrap = indexIDWrap;
			pendingProposal.absEnqueueTime = OtherUtils.getSystemMS();

			if (!this.isEnd) {
				this.pendingQueue.put(pendingProposal);
			}
			
			if (this.isEnd) {
				return -1;
			} else {
				return 0;
			}
		} catch (Exception e) {
			logger.error("addProposal request error.", e);
			if (serialLock.isLocked()) {
				serialLock.unLock();
			}
		}
		
		return -1;
	}

	public void pluckProposal(List<PendingProposal> vecRequest) {
		int pluckCount = 0;
		int pluckSize = 0;

		long nowTime = OtherUtils.getSystemMS();

		long sleepInterval = 1000 * 1000;
		while (true) {
			PendingProposal pendingProposal = null;
			try {
				pendingProposal = this.pendingQueue.poll(sleepInterval, TimeUnit.MICROSECONDS);
				if (pendingProposal != null) {
					sleepInterval = 10;
					vecRequest.add(pendingProposal);

					pluckCount++;
					pluckSize += pendingProposal.psValue.length;

					int proposalWaitTime = (int) (nowTime > pendingProposal.absEnqueueTime ? (nowTime - pendingProposal.absEnqueueTime) : 0);
					Breakpoint.getInstance().getCommiterBP().batchProposeWaitTimeMs(proposalWaitTime, this.myGroupIdx);

					if (pluckCount >= this.batchCount || pluckSize >= this.batchMaxSize) {
						logger.debug("pluckCount {} batchCount {}, pluckSize {}, batchMaxSize {}, proposalWaitTime {}."
								, pluckCount, batchCount, pluckSize, batchMaxSize, proposalWaitTime);
						break;
					}
				} else {
					if (pluckCount > 0) {
						logger.debug("pluck {} request.", pluckCount);
						break;
					}
				}
			} catch (InterruptedException e) {
				logger.error("pluckProposal error.", e);
			}
		}

		pCount++;
		if (pCount % 10000 == 0) {
			try {
				Thread.sleep(2);
			} catch (InterruptedException e) {
				logger.error("", e);
			}
		}
	}

	public void onlyOnePropose(PendingProposal pendingProposal) {
		ProposeResult pResult = this.paxosNode.propose(myGroupIdx, pendingProposal.psValue, pendingProposal.smCtx);
		try {
			pendingProposal.notifier.sendNotify(pResult.getResult());
		} catch (IOException e) {
			logger.error("pendingProposal sendNotify error.", e);
		}
	}

	static class PendingProposal {
		private byte[] psValue;
		private SMCtx smCtx;
		private JavaOriTypeWrapper<Long> instanceIDWrap;
		private JavaOriTypeWrapper<Integer> batchIndexWrap;
		private Notifier notifier;
		private long absEnqueueTime;

		public PendingProposal() {
			super();
			this.psValue = null;
			this.smCtx = null;
			this.instanceIDWrap = null;
			this.batchIndexWrap = null;
			this.notifier = null;
			this.absEnqueueTime = 0;
		}

		public byte[] getPsValue() {
			return psValue;
		}

		public void setPsValue(byte[] psValue) {
			this.psValue = psValue;
		}

		public SMCtx getSmCtx() {
			return smCtx;
		}

		public void setSmCtx(SMCtx smCtx) {
			this.smCtx = smCtx;
		}

		public JavaOriTypeWrapper<Long> getInstanceIDWrap() {
			return instanceIDWrap;
		}

		public void setInstanceIDWrap(JavaOriTypeWrapper<Long> instanceIDWrap) {
			this.instanceIDWrap = instanceIDWrap;
		}

		public void setInstanceID(long instanceID) {
			this.instanceIDWrap.setValue(instanceID);
		}

		public Integer getBatchIndex() {
			return batchIndexWrap.getValue();
		}

		public void setBatchIndex(Integer batchIndex) {
			this.batchIndexWrap.setValue(batchIndex);
		}

		public Notifier getNotifier() {
			return notifier;
		}

		public void setNotifier(Notifier notifier) {
			this.notifier = notifier;
		}

		public long getAbsEnqueueTime() {
			return absEnqueueTime;
		}

		public void setAbsEnqueueTime(long absEnqueueTime) {
			this.absEnqueueTime = absEnqueueTime;
		}
	};
}
