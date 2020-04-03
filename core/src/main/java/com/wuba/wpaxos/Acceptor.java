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
import com.wuba.wpaxos.comm.MsgTransport;
import com.wuba.wpaxos.comm.breakpoint.Breakpoint;
import com.wuba.wpaxos.comm.enums.PaxosMsgType;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.proto.PaxosMsg;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/**
 * acceptor
 */
public class Acceptor extends Base {
	private static final Logger logger = LogManager.getLogger(Acceptor.class); 
	
	private AcceptorState acceptorState;

	public Acceptor(Config config, MsgTransport msgTransport, Instance instance, LogStorage logStorage) {
		super(config, msgTransport, instance);
		this.acceptorState = new AcceptorState(config, logStorage);
	}

	@Override
	public void initForNewPaxosInstance() {
		this.acceptorState.init();
	}
	
	public int init() throws Exception {
		JavaOriTypeWrapper<Long> instanceIdWrap = new JavaOriTypeWrapper<Long>();
		int ret = this.acceptorState.load(instanceIdWrap);
		long instanceId = instanceIdWrap.getValue();
		if(ret != 0) {
			logger.error("Load State fail, ret = {}.", ret);
			return ret;
		}
		
		if(instanceId == 0) {
			logger.info("Empty database");
		}
		
		setInstanceID(instanceId);
		return 0;
	}
	
	public int onPrepare(PaxosMsg paxosMsg) {
		Breakpoint.getInstance().getAcceptorBP().onPrepare(this.pConfig.getMyGroupIdx(), getInstanceID());
		PaxosMsg replyPaxosMsg = new PaxosMsg();
		replyPaxosMsg.setInstanceID(getInstanceID());
		replyPaxosMsg.setNodeID(this.pConfig.getMyNodeID());
		replyPaxosMsg.setProposalID(paxosMsg.getProposalID());
		replyPaxosMsg.setMsgType(PaxosMsgType.paxosPrepareReply.getValue());
		
		BallotNumber ballot = new BallotNumber(paxosMsg.getProposalID(), paxosMsg.getNodeID());
		BallotNumber pbn = this.acceptorState.getPromiseBallot();
		if(ballot.ge(pbn)) {
			int ret = updateAcceptorState4Prepare(replyPaxosMsg, ballot);
			if(ret != 0) return ret;
		} else {
			Breakpoint.getInstance().getAcceptorBP().onPrepareReject(this.pConfig.getMyGroupIdx(), getInstanceID());
			replyPaxosMsg.setRejectByPromiseID(this.acceptorState.getPromiseBallot().getProposalID());
		}
		
		long replyNodeId = paxosMsg.getNodeID();
		sendMessage(replyNodeId, replyPaxosMsg);
		return 0;
	}

	private int updateAcceptorState4Prepare(PaxosMsg replyPaxosMsg, BallotNumber ballot) {
		replyPaxosMsg.setPreAcceptID(this.acceptorState.getAcceptedBallot().getProposalID());
		replyPaxosMsg.setPreAcceptNodeID(this.acceptorState.getAcceptedBallot().getNodeId());
		
		if(this.acceptorState.getAcceptedBallot().getProposalID() > 0) {
			replyPaxosMsg.setValue(this.acceptorState.getAcceptedValue());
		}
		
		this.acceptorState.setPromiseBallot(ballot);
		
		int ret = this.acceptorState.persist(getInstanceID(), getLastChecksum());
		if(ret != 0) {
			Breakpoint.getInstance().getAcceptorBP().onPreparePersistFail(this.pConfig.getMyGroupIdx(), getInstanceID());
			return -1;
		}
		
		Breakpoint.getInstance().getAcceptorBP().onPreparePass(this.pConfig.getMyGroupIdx(), getInstanceID());
		return 0;
	}
	
	public void onAccept(PaxosMsg paxosMsg) {
		Breakpoint.getInstance().getAcceptorBP().onAccept(this.pConfig.getMyGroupIdx(), getInstanceID());
		PaxosMsg replyPaxosMsg = new PaxosMsg();
		replyPaxosMsg.setInstanceID(getInstanceID());
		replyPaxosMsg.setNodeID(this.pConfig.getMyNodeID());
		replyPaxosMsg.setProposalID(paxosMsg.getProposalID());
		replyPaxosMsg.setMsgType(PaxosMsgType.paxosAcceptReply.getValue());
		
		BallotNumber ballot = new BallotNumber(paxosMsg.getProposalID(), paxosMsg.getNodeID());
		BallotNumber promiseBallot = this.acceptorState.getPromiseBallot();
		if(ballot.ge(promiseBallot)) {
			this.acceptorState.setPromiseBallot(ballot);
			BallotNumber acceptedBallot = new BallotNumber(ballot.getProposalID(), ballot.getNodeId());
			this.acceptorState.setAcceptedBallot(acceptedBallot);
			this.acceptorState.setAcceptedValue(paxosMsg.getValue());
			updateAcceptorState4Accept(replyPaxosMsg);
		} else {
			Breakpoint.getInstance().getAcceptorBP().onAcceptReject(this.pConfig.getMyGroupIdx(), getInstanceID());
			replyPaxosMsg.setRejectByPromiseID(this.acceptorState.getPromiseBallot().getProposalID());
		}
		
		long replyNodeId = paxosMsg.getNodeID();
		sendMessage(replyNodeId, replyPaxosMsg);
	}
	
	private void updateAcceptorState4Accept(PaxosMsg replyPaxosMsg) {

		int ret = this.acceptorState.persist(getInstanceID(), getLastChecksum());
		if(ret != 0) {
			Breakpoint.getInstance().getAcceptorBP().onAcceptPersistFail(this.pConfig.getMyGroupIdx(), getInstanceID());
			return ;
		}
		Breakpoint.getInstance().getAcceptorBP().onAcceptPass(this.pConfig.getMyGroupIdx(), getInstanceID());
	}

	public AcceptorState getAcceptorState() {
		return acceptorState;
	}

	public void setAcceptorState(AcceptorState acceptorState) {
		this.acceptorState = acceptorState;
	}
}















