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

import com.wuba.wpaxos.base.BallotNumber;
import com.wuba.wpaxos.base.Base;
import com.wuba.wpaxos.base.BroadcastMessageType;
import com.wuba.wpaxos.comm.InsideOptions;
import com.wuba.wpaxos.comm.MsgTransport;
import com.wuba.wpaxos.comm.breakpoint.Breakpoint;
import com.wuba.wpaxos.comm.enums.MessageSendType;
import com.wuba.wpaxos.comm.enums.PaxosMsgType;
import com.wuba.wpaxos.comm.enums.TimerType;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.helper.MsgCounter;
import com.wuba.wpaxos.proto.PaxosMsg;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import com.wuba.wpaxos.utils.OtherUtils;
import com.wuba.wpaxos.utils.TimeStat;

/**
 * proposer
 */
public class Proposer extends Base {
	private final static Logger logger = LogManager.getLogger(Proposer.class); 
	private ProposerState proposerState;
	private MsgCounter msgCounter;
	private Learner learner;
	private boolean isPreparing;
	private boolean isAccepting;
	private IoLoop ioLoop;
	private long prepareTimerID;
	private int lastPrepareTimeoutMs;
	private long acceptTimerID;
	private int lastAcceptTimeoutMs;
	private long timeoutInstanceID;
	private boolean canSkipPrepare;
	private boolean wasRejectBySomeone;
	private TimeStat timeStat = new TimeStat();

	public Proposer(Config config, MsgTransport msgTransport, Instance instance, Learner learner, IoLoop ioLoop) {
		super(config, msgTransport, instance);
		this.proposerState = new ProposerState(config);
		this.msgCounter = new MsgCounter(config);
		this.learner = learner;
		this.ioLoop = ioLoop;
		this.isPreparing = false;
		this.isAccepting = false;
		this.canSkipPrepare = false;
		initForNewPaxosInstance();
		this.prepareTimerID = 0;
		this.acceptTimerID = 0;
		this.timeoutInstanceID = 0;
		this.lastPrepareTimeoutMs = this.pConfig.getPrepareTimeoutMs();
		this.lastAcceptTimeoutMs = this.pConfig.getAcceptTimeoutMs();
		this.wasRejectBySomeone = false;
	}

	public void setStartProposalID(long proposalID) {
		this.proposerState.setStartProposalID(proposalID);
	}

	@Override
	public void initForNewPaxosInstance() {
		this.msgCounter.startNewRound();
		this.proposerState.init();
	}

	public int newValue(byte[] value) {
		Breakpoint.getInstance().getProposerBP().newProposal(new String(value), this.pConfig.getMyGroupIdx(), this.instanceID);
		
		if(this.proposerState.getValue().length == 0) {
			this.proposerState.setValue(value);
		}
		
		InsideOptions io = InsideOptions.getInstance();
		this.lastPrepareTimeoutMs = io.getStartPrepareTimeoutMs();
		this.lastAcceptTimeoutMs = io.getStartAcceptTimeoutMs();
		
		if(this.canSkipPrepare && !this.wasRejectBySomeone) {
			Breakpoint.getInstance().getProposerBP().newProposalSkipPrepare(this.pConfig.getMyGroupIdx(), this.instanceID);
			accept();
		} else {
			//if not reject by someone, no need to increase ballot
			prepare(this.wasRejectBySomeone);
		}
		return 0;
	}

	public boolean isWorking() {
		return this.isPreparing || this.isAccepting;
	}

	public void prepare(boolean needNewBallot) {
		Breakpoint.getInstance().getProposerBP().prepare(this.pConfig.getMyGroupIdx(), this.instanceID);
		this.timeStat.point();
		
		exitAccept();
		this.isPreparing = true;
		this.canSkipPrepare = false;
		this.wasRejectBySomeone = false;
		
		this.proposerState.resetHighestOtherPreAcceptBallot();
		if(needNewBallot) {
			this.proposerState.newPrepare();
		}
		
		PaxosMsg paxosMsg = new PaxosMsg();
		paxosMsg.setMsgType(PaxosMsgType.paxosPrepare.getValue());
		paxosMsg.setInstanceID(getInstanceID());
		paxosMsg.setNodeID(this.pConfig.getMyNodeID());
		paxosMsg.setProposalID(this.proposerState.getProposalID());
		
		this.msgCounter.startNewRound();
		
		addPrepareTimer(0);
		
		int runSelfFirst = BroadcastMessageType.BroadcastMessage_Type_RunSelf_First.getType();
		int sendType = MessageSendType.UDP.getValue();
		
		paxosMsg.setTimestamp(System.currentTimeMillis());
		broadcastMessage(paxosMsg, runSelfFirst, sendType);
	}

	public void onPrepareReply(PaxosMsg paxosMsg) {
		Breakpoint.getInstance().getProposerBP().onPrepareReply(this.pConfig.getMyGroupIdx(), this.instanceID);
		
		if(!this.isPreparing) {
			Breakpoint.getInstance().getProposerBP().onPrepareReplyButNotPreparing(this.pConfig.getMyGroupIdx(), this.instanceID);
			return ;
		}
		
		if(paxosMsg.getProposalID() != this.proposerState.getProposalID()) {
			Breakpoint.getInstance().getProposerBP().onPrepareReplyNotSameProposalIDMsg(this.pConfig.getMyGroupIdx(), this.instanceID);
			return ;
		}
		
		this.msgCounter.addReceive(paxosMsg.getNodeID());
		
		if(paxosMsg.getRejectByPromiseID() == 0) {
			BallotNumber ballot = new BallotNumber(paxosMsg.getPreAcceptID(), paxosMsg.getPreAcceptNodeID());
			this.msgCounter.addPromiseOrAccept(paxosMsg.getNodeID());
			this.proposerState.addPreAcceptValue(ballot, paxosMsg.getValue());
			
		} else {
			this.msgCounter.addReject(paxosMsg.getNodeID());
			this.wasRejectBySomeone = true;
			this.proposerState.setOtherProposalID(paxosMsg.getRejectByPromiseID());
		}
		
		if(this.msgCounter.isPassedOnThisRound()) {
			int useTimeMs = this.timeStat.point();
			Breakpoint.getInstance().getProposerBP().preparePass(useTimeMs, this.pConfig.getMyGroupIdx(), this.getInstanceID());
			
			this.canSkipPrepare = true;
			accept();
		} else if(this.msgCounter.isRejectedOnThisRound() 
				|| this.msgCounter.isAllReceiveOnThisRound()) {
			Breakpoint.getInstance().getProposerBP().prepareNotPass(this.pConfig.getMyGroupIdx(), this.getInstanceID());
			addPrepareTimer(OtherUtils.fastRand() % 30 + 10);
		}
	}

	public void onExpiredPrepareReply(PaxosMsg paxosMsg) {
		if(paxosMsg.getRejectByPromiseID() != 0) {
			this.wasRejectBySomeone = true;
			this.proposerState.setOtherProposalID(paxosMsg.getRejectByPromiseID());
		}
	}

	public void accept() {
		Breakpoint.getInstance().getProposerBP().accept(this.pConfig.getMyGroupIdx(), this.getInstanceID());
		this.timeStat.point();
		
		exitPrepare();
		this.isAccepting = true;
		
		PaxosMsg paxosMsg = new PaxosMsg();
		paxosMsg.setMsgType(PaxosMsgType.paxosAccept.getValue());
		paxosMsg.setInstanceID(getInstanceID());
		paxosMsg.setNodeID(this.pConfig.getMyNodeID());
		paxosMsg.setProposalID(this.proposerState.getProposalID());
		paxosMsg.setValue(this.proposerState.getValue());
		paxosMsg.setLastChecksum(getLastChecksum());
		
		this.msgCounter.startNewRound();
		addAcceptTimer(0);
		
		int runSelfFirst = BroadcastMessageType.BroadcastMessage_Type_RunSelf_Final.getType();
		int sendType = MessageSendType.UDP.getValue();
		
		paxosMsg.setTimestamp(System.currentTimeMillis());
		broadcastMessage(paxosMsg, runSelfFirst, sendType);
	}

	public void onAcceptReply(PaxosMsg paxosMsg) {
		Breakpoint.getInstance().getProposerBP().onAcceptReply(this.pConfig.getMyGroupIdx(), this.getInstanceID());
		
		if(!this.isAccepting) {
			Breakpoint.getInstance().getProposerBP().onAcceptReplyButNotAccepting(this.pConfig.getMyGroupIdx(), this.getInstanceID());
			return ;
		}
		
		if(paxosMsg.getProposalID() != this.proposerState.getProposalID()) {
			Breakpoint.getInstance().getProposerBP().onAcceptReplyNotSameProposalIDMsg(this.pConfig.getMyGroupIdx(), this.getInstanceID());
			return ;
		}
		
		this.msgCounter.addReceive(paxosMsg.getNodeID());
		if(paxosMsg.getRejectByPromiseID() == 0) {
			this.msgCounter.addPromiseOrAccept(paxosMsg.getNodeID());
		} else {
			this.msgCounter.addReject(paxosMsg.getNodeID());
			this.wasRejectBySomeone = true;
			this.proposerState.setOtherProposalID(paxosMsg.getRejectByPromiseID());
		}
		
		if(this.msgCounter.isPassedOnThisRound()) {
			int useTimeMs = this.timeStat.point();
			Breakpoint.getInstance().getProposerBP().acceptPass(useTimeMs, this.pConfig.getMyGroupIdx(), this.getInstanceID());
			exitAccept();
			this.learner.proposerSendSuccess(getInstanceID(), this.proposerState.getProposalID());
		} else if(this.msgCounter.isRejectedOnThisRound()
				|| this.msgCounter.isAllReceiveOnThisRound()) {
			Breakpoint.getInstance().getProposerBP().acceptNotPass(this.pConfig.getMyGroupIdx(), this.getInstanceID());
			
			addAcceptTimer(OtherUtils.fastRand() % 30 +10);
		}
	}

	public void onExpiredAcceptReply(PaxosMsg paxosMsg) {
		if(paxosMsg.getRejectByPromiseID() != 0) {
			this.wasRejectBySomeone = true;
			this.proposerState.setOtherProposalID(paxosMsg.getRejectByPromiseID());
		}
	}

	public void onPrepareTimeout() {
		if(getInstanceID() != this.timeoutInstanceID) {
			return ;
		}
		
		Breakpoint.getInstance().getProposerBP().prepareTimeout(this.pConfig.getMyGroupIdx(), this.instanceID);
		
		prepare(this.wasRejectBySomeone);
	}

	public void onAcceptTimeout() {
		if(getInstanceID() != this.timeoutInstanceID) {
			return ;
		}
		
		Breakpoint.getInstance().getProposerBP().acceptTimeout(this.pConfig.getMyGroupIdx(), this.instanceID);
		prepare(this.wasRejectBySomeone);
	}

	public void exitPrepare() {
		if(this.isPreparing) {
			this.isPreparing = false;
			JavaOriTypeWrapper<Long> prepareTimerIDWrap = new JavaOriTypeWrapper<Long>();
			prepareTimerIDWrap.setValue(this.prepareTimerID);
			this.ioLoop.removeTimer(prepareTimerIDWrap);
			this.prepareTimerID = prepareTimerIDWrap.getValue();
		}
	}

	public void exitAccept() {
		if(this.isAccepting) {
			this.isAccepting = false;
			JavaOriTypeWrapper<Long> acceptTimerIDWrap = new JavaOriTypeWrapper<Long>();
			acceptTimerIDWrap.setValue(this.acceptTimerID);
			this.ioLoop.removeTimer(acceptTimerIDWrap);
			this.acceptTimerID = acceptTimerIDWrap.getValue();
		}
	}

	public void cancelSkipPrepare() {
		this.canSkipPrepare = false;
	}

	public void addPrepareTimer(int timeoutMs) {
		JavaOriTypeWrapper<Long> prepareTimerIDWrap = new JavaOriTypeWrapper<Long>();
		if(this.prepareTimerID > 0) {
			
			prepareTimerIDWrap.setValue(this.prepareTimerID);
			this.ioLoop.removeTimer(prepareTimerIDWrap);
			this.prepareTimerID = prepareTimerIDWrap.getValue();
		}
		
		if(timeoutMs > 0) {
			this.ioLoop.addTimer(timeoutMs, TimerType.proposerPrepareTimeout.getValue(), prepareTimerIDWrap);
			this.prepareTimerID = prepareTimerIDWrap.getValue();
			return ;
		}
		
		this.ioLoop.addTimer(this.lastPrepareTimeoutMs, TimerType.proposerPrepareTimeout.getValue(), prepareTimerIDWrap);
		this.prepareTimerID = prepareTimerIDWrap.getValue();
		this.timeoutInstanceID = getInstanceID();
		
		this.lastPrepareTimeoutMs *= 2;
		int maxPrepareTimeoutMs = InsideOptions.getInstance().getMaxPrepareTimeoutMs();
		if(this.lastPrepareTimeoutMs > maxPrepareTimeoutMs) {
			this.lastPrepareTimeoutMs = maxPrepareTimeoutMs;
		}
	}

	public void addAcceptTimer(int timeoutMs) {
		JavaOriTypeWrapper<Long> acceptTimerIDWrap = new JavaOriTypeWrapper<Long>();
		if(this.acceptTimerID > 0) {
			acceptTimerIDWrap.setValue(this.acceptTimerID);
			this.ioLoop.removeTimer(acceptTimerIDWrap);
			this.acceptTimerID = acceptTimerIDWrap.getValue();
		}
		
		if(timeoutMs > 0) {
			this.ioLoop.addTimer(timeoutMs, TimerType.proposerAcceptTimeout.getValue(), acceptTimerIDWrap);
			this.acceptTimerID = acceptTimerIDWrap.getValue();
			return ;
		}
		
		this.ioLoop.addTimer(this.lastAcceptTimeoutMs, TimerType.proposerAcceptTimeout.getValue(), acceptTimerIDWrap);
		this.acceptTimerID = acceptTimerIDWrap.getValue();
		this.timeoutInstanceID = getInstanceID();
		
		logger.debug("accept timeout mills {}.", this.lastAcceptTimeoutMs);
		this.lastAcceptTimeoutMs *= 2;
		int maxAcceptTimeoutMs = InsideOptions.getInstance().getMaxAcceptTimeoutMs();
		if(this.lastAcceptTimeoutMs > maxAcceptTimeoutMs) {
			this.lastAcceptTimeoutMs = maxAcceptTimeoutMs;
		}
	}
}












