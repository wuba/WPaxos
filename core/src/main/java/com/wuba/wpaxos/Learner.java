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

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.base.BallotNumber;
import com.wuba.wpaxos.base.Base;
import com.wuba.wpaxos.base.BroadcastMessageType;
import com.wuba.wpaxos.checkpoint.CheckpointMgr;
import com.wuba.wpaxos.comm.InsideOptions;
import com.wuba.wpaxos.comm.MsgTransport;
import com.wuba.wpaxos.comm.breakpoint.Breakpoint;
import com.wuba.wpaxos.comm.enums.CheckpointMsgType;
import com.wuba.wpaxos.comm.enums.CheckpointSendFileAckFlag;
import com.wuba.wpaxos.comm.enums.CheckpointSendFileFlag;
import com.wuba.wpaxos.comm.enums.MessageSendType;
import com.wuba.wpaxos.comm.enums.PaxosMsgFlagType;
import com.wuba.wpaxos.comm.enums.PaxosMsgType;
import com.wuba.wpaxos.comm.enums.TimerType;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.config.Def;
import com.wuba.wpaxos.helper.LearnerState;
import com.wuba.wpaxos.proto.AcceptorStateData;
import com.wuba.wpaxos.proto.CheckpointMsg;
import com.wuba.wpaxos.proto.PaxosMsg;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.store.PaxosLog;
import com.wuba.wpaxos.storemachine.SMFac;
import com.wuba.wpaxos.storemachine.StateMachine;
import com.wuba.wpaxos.storemachine.InsideSM.UpdateCpRet;
import com.wuba.wpaxos.utils.FileUtils;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/**
 * Learner
 */
public class Learner extends Base {
	private static final Logger logger = LogManager.getLogger(Learner.class); 
	private LearnerState learnerState;
	private Acceptor acceptor;
	private PaxosLog paxosLog;
	private long askForLearnNoopTimerID;
	private IoLoop ioLoop;
	private long highestSeenInstanceID;
	private long highestSeenInstanceIDFromNodeID;
	private boolean isIMLearning;
	private LearnerSender learnerSender;
	private long lastAckInstanceID;
	private CheckpointMgr checkpointMgr;
	private SMFac smFac;
	private CheckpointSender checkpointSender;
	private CheckpointReceiver checkpointReceiver;

	public Learner(Config config, MsgTransport msgTransport, Instance instance, Acceptor acceptor,
			LogStorage logStorage, IoLoop ioLoop, CheckpointMgr checkpointMgr, SMFac smFac) {
		super(config, msgTransport, instance);
		this.learnerState = new LearnerState(config, logStorage);
		this.paxosLog = new PaxosLog(logStorage);
		this.learnerSender = new LearnerSender(config, this, this.paxosLog);
		this.checkpointReceiver = new CheckpointReceiver(config, logStorage);
		this.acceptor = acceptor;
		initForNewPaxosInstance();
		this.askForLearnNoopTimerID = 0;
		this.ioLoop = ioLoop;
		this.checkpointMgr = checkpointMgr;
		this.smFac = smFac;
		this.checkpointSender = null;
		this.highestSeenInstanceID = 0;
		this.highestSeenInstanceIDFromNodeID = 0;
		this.isIMLearning = false;
		this.lastAckInstanceID = 0;
	}

	public void startLearnerSender() {
		Thread t = new Thread(this.learnerSender);
		t.setName("Learner");
		t.start();
	}

	@Override
	public void initForNewPaxosInstance() {
		this.learnerState.init();
	}

	public boolean isLearned() {
		return this.learnerState.getIsLearned();
	}

	public byte[] getLearnValue() {
		return this.learnerState.getLearnValue();
	}

	public int getNewChecksum() {
		return this.learnerState.getNewChecksum();
	}

	public void stop() throws Exception {
		this.learnerSender.stop();
		if(this.checkpointSender != null) {
			this.checkpointSender.stop();
			this.checkpointSender = null;
		}
	}
	
	public void join() throws Exception {
    	Thread.currentThread().join();
    }

	// prepare learn
	public void askforLearn() {
		Breakpoint.getInstance().getLearnerBP().askforLearn(this.pConfig.getMyGroupIdx());
		
		PaxosMsg paxosMsg = new PaxosMsg();
		paxosMsg.setInstanceID(getInstanceID());
		paxosMsg.setNodeID(this.pConfig.getMyNodeID());
		paxosMsg.setMsgType(PaxosMsgType.paxosLearnerAskforLearn.getValue());
		
		if(this.pConfig.isIMFollower()) {
			//this is not proposal nodeid, just use this val to bring follow to nodeid info.
			paxosMsg.setProposalNodeID(this.pConfig.getFollowToNodeID());
		}
		
		logger.debug("END InstanceID {} MyNodeID {}.", paxosMsg.getInstanceID(), paxosMsg.getNodeID());
		broadcastMessage(paxosMsg, BroadcastMessageType.BroadcastMessage_Type_RunSelf_None.getType(), MessageSendType.TCP.getValue());
		broadcastMessageToTempNode(paxosMsg, MessageSendType.UDP.getValue());
	}

	public void onAskforLearn(PaxosMsg paxosMsg) {
		Breakpoint.getInstance().getLearnerBP().onAskforLearn(this.pConfig.getMyGroupIdx());
		
		logger.debug("START Msg.InstanceID {} Now.InstanceID {} Msg.from_nodeid {} MinChosenInstanceID {}.",
				paxosMsg.getInstanceID(), getInstanceID(), paxosMsg.getNodeID(), this.checkpointMgr.getMinChosenInstanceID());
		setSeenInstanceID(paxosMsg.getInstanceID(), paxosMsg.getNodeID());
		
		if(paxosMsg.getProposalNodeID() == this.pConfig.getMyNodeID()) {
			//Found a node follow me.
			logger.info("Found a node {} follow me.", paxosMsg.getNodeID());
			this.pConfig.addFollowerNode(paxosMsg.getNodeID());
		}
		
		if(paxosMsg.getInstanceID() >= getInstanceID()) return;
		
		if(paxosMsg.getInstanceID() >= this.checkpointMgr.getMinChosenInstanceID()) {
			if(!this.learnerSender.prepare(paxosMsg.getInstanceID(), paxosMsg.getNodeID())) {
				Breakpoint.getInstance().getLearnerBP().onAskforLearnGetLockFail(this.pConfig.getMyGroupIdx());
				
				logger.debug("LearnerSender working for others.");
				
				if(paxosMsg.getInstanceID() == (getInstanceID() - 1)) {
					logger.debug("InstanceID only difference one, just send this value to other.");
					//send one value
					AcceptorStateData state = new AcceptorStateData();
					int ret = this.paxosLog.readState(this.pConfig.getMyGroupIdx(), paxosMsg.getInstanceID(), state);
					if(ret == 0) {
						BallotNumber ballot = new BallotNumber(state.getAcceptedID(), state.getAcceptedNodeID());
						sendLearnValue(paxosMsg.getNodeID(), paxosMsg.getInstanceID(), ballot, state.getAcceptedValue(), 0, false);
					}
				}
				
				return;
			}
		}
		sendNowInstanceID(paxosMsg.getInstanceID(), paxosMsg.getNodeID());
	}

	public void sendNowInstanceID(long instanceID, long sendNodeId) {
		Breakpoint.getInstance().getLearnerBP().sendNowInstanceID(this.pConfig.getMyGroupIdx());
		
		PaxosMsg msg = new PaxosMsg();
		msg.setInstanceID(instanceID);
		msg.setNodeID(this.pConfig.getMyNodeID());
		msg.setMsgType(PaxosMsgType.paxosLearnerSendNowInstanceID.getValue());
		msg.setNowInstanceID(getInstanceID());
		msg.setMinchosenInstanceID(this.checkpointMgr.getMinChosenInstanceID());
		
		if(getInstanceID() - instanceID > 50) {
			//instanceid too close not need to send vsm/master checkpoint.
			byte[] systemVariablesCPBuffer = this.pConfig.getSystemVSM().getCheckpointBuffer();
			msg.setSystemVariables(systemVariablesCPBuffer);
			
			if(this.pConfig.getMasterSM() != null) {
				byte[] masterVariableCPBuffer = this.pConfig.getMasterSM().getCheckpointBuffer();
				msg.setMasterVariables(masterVariableCPBuffer);
			}
		}
		sendMessage(sendNodeId, msg);
	}

	public void onSendNowInstanceID(PaxosMsg paxosMsg) {
		Breakpoint.getInstance().getLearnerBP().onSendNowInstanceID(this.pConfig.getMyGroupIdx());
		
		logger.debug("START Msg.InstanceID {} Now.InstanceID {} Msg.from_nodeid {} Msg.MaxInstanceID {} systemvariables_size {} "
				+ "mastervariables_size {} paxosMsg.MinChosenInstanceID {}.",paxosMsg.getInstanceID(), getInstanceID(), paxosMsg.getNodeID(),
				paxosMsg.getNowInstanceID(), paxosMsg.getSystemVariables().length,paxosMsg.getMasterVariables().length, paxosMsg.getMinChosenInstanceID());
		
		long receiveNowInstanceId = paxosMsg.getNowInstanceID();
		long currInstanceId = this.getInstanceID();
		if(receiveNowInstanceId - currInstanceId >= 5) {
			logger.warn("ask for learn receive instance id : {}, current instance id : {}, diff : {}, groupId : {}.",
							receiveNowInstanceId, currInstanceId, (receiveNowInstanceId - currInstanceId), pConfig.getMyGroupIdx());
		}
		
		setSeenInstanceID(receiveNowInstanceId, paxosMsg.getNodeID());
		UpdateCpRet updateCpRet = this.pConfig.getSystemVSM().updateByCheckpoint(paxosMsg.getSystemVariables());
		int ret = updateCpRet.getRet();
		boolean isChanged = updateCpRet.isChange();
		if(ret == 0 && isChanged) {
			logger.warn("SystemVariables changed!, all thing need to reflesh, so skip this msg");
			return ;
		}
		
		if(this.pConfig.getMasterSM() != null) {
			UpdateCpRet masterUpdateCpRet = this.pConfig.getMasterSM().updateByCheckpoint(paxosMsg.getMasterVariables());
			ret = masterUpdateCpRet.getRet();
			boolean isMasterChanged = masterUpdateCpRet.isChange();
			if(ret == 0 && isMasterChanged) {
				logger.warn("MasterVariables changed!");
			}
		}
		
		if(paxosMsg.getInstanceID() != getInstanceID()) {
			logger.debug("Lag msg, skip");
			return ;
		}
		
		if(paxosMsg.getNowInstanceID() <= getInstanceID()) {
			logger.debug("Lag msg, skip");
			return ;
		}
		
		if(paxosMsg.getMinChosenInstanceID() > getInstanceID()) {
			Breakpoint.getInstance().getCheckpointBP().needAskforCheckpoint();
			
			logger.warn("my instanceid {} small than other's minchoseninstanceid {}, other nodeid {}.",
						getInstanceID(), paxosMsg.getMinChosenInstanceID(), paxosMsg.getNodeID());
			askforCheckpoint(paxosMsg.getNodeID());
		} else if(!this.isIMLearning) {
			comfirmAskForLearn(paxosMsg.getNodeID());
		}
		
	}

	public void askforCheckpoint(long sendNodeID) {
		int ret = this.checkpointMgr.prepareAskForCheckpoint(sendNodeID);
		if(ret != 0) return;
		
		PaxosMsg msg = new PaxosMsg();
		msg.setInstanceID(getInstanceID());
		msg.setNodeID(this.pConfig.getMyNodeID());
		msg.setMsgType(PaxosMsgType.paxosLearnerAskforCheckpoint.getValue());
		
		logger.debug("END InstanceID {} MyNodeID {}.", getInstanceID(), msg.getNodeID());
		sendMessage(sendNodeID, msg);
	}

	public void onAskforCheckpoint(PaxosMsg paxosMsg) {
		CheckpointSender cps = getNewCheckpointSender(paxosMsg.getNodeID());
		if(cps != null) {
			Thread t = new Thread(cps);
			t.setName("CheckpointSender-" + paxosMsg.getNodeID());
			t.start();
			logger.info("new checkpoint sender started, send to nodeid {}.", paxosMsg.getNodeID());
		} else {
			logger.error("Checkpoint Sender is running");
		}
	}

	// comfirm learn
	public void comfirmAskForLearn(long sendNodeID) {
		Breakpoint.getInstance().getLearnerBP().comfirmAskForLearn(this.pConfig.getMyGroupIdx());
		
		PaxosMsg msg = new PaxosMsg();
		msg.setInstanceID(getInstanceID());
		msg.setNodeID(this.pConfig.getMyNodeID());
		msg.setMsgType(PaxosMsgType.paxosLearnerComfirmAskforLearn.getValue());
		
		logger.debug("END InstanceID {} MyNodeID {}.", getInstanceID(), msg.getNodeID());
		sendMessage(sendNodeID, msg);
		this.isIMLearning = true;
	}

	public void onComfirmAskForLearn(PaxosMsg paxosMsg) {
		Breakpoint.getInstance().getLearnerBP().onComfirmAskForLearn(this.pConfig.getMyGroupIdx());
		logger.debug("START Msg.InstanceID {} Msg.from_nodeid {}.", paxosMsg.getInstanceID(), paxosMsg.getNodeID());
		
		if(!this.learnerSender.comfirm(paxosMsg.getInstanceID(), paxosMsg.getNodeID())) {
			Breakpoint.getInstance().getLearnerBP().onComfirmAskForLearnGetLockFail(this.pConfig.getMyGroupIdx());
			logger.error("LearnerSender comfirm fail, maybe is lag msg");
			return ;
		}
		
		logger.debug("OK, success comfirm");
	}

	public int sendLearnValue(long sendNodeID, long learnInstanceID, 
			BallotNumber learnedBallot, byte[] learnedValue, int checksum, boolean needAck) {
		Breakpoint.getInstance().getLearnerBP().sendLearnValue(this.pConfig.getMyGroupIdx());
		
		PaxosMsg msg = new PaxosMsg();
		msg.setMsgType(PaxosMsgType.paxosLearnerSendLearnValue.getValue());
		msg.setInstanceID(learnInstanceID);
		msg.setNodeID(this.pConfig.getMyNodeID());
		msg.setProposalNodeID(learnedBallot.getNodeId());
		msg.setProposalID(learnedBallot.getProposalID());
		msg.setValue(learnedValue);
		msg.setLastChecksum(checksum);
		
		if(needAck) {
			msg.setFlag(PaxosMsgFlagType.sendLearnValueNeedAck.getValue());
		}
		
		return sendMessage(sendNodeID, msg, MessageSendType.TCP.getValue());
	}

	public void onSendLearnValue(PaxosMsg paxosMsg) {
		Breakpoint.getInstance().getLearnerBP().onSendLearnValue(this.pConfig.getMyGroupIdx());
		
		logger.debug("START Msg.InstanceID {} Now.InstanceID {} Msg.ballot_proposalid {} Msg.ballot_nodeid {} Msg.ValueSize {}.",
					paxosMsg.getInstanceID(), paxosMsg.getProposalID(), getInstanceID(), paxosMsg.getNodeID(), paxosMsg.getValue().length);
		
		if(paxosMsg.getInstanceID() > getInstanceID()) {
			logger.warn("[Latest Msg] i can't learn, paxosmsg instanceID {}, myInstanceID {}.", paxosMsg.getInstanceID(), getInstanceID());
			return ;
		}
		
		if(paxosMsg.getInstanceID() < getInstanceID()) {
			logger.debug("[Lag Msg] no need to learn");
		} else {
			//learn value
			BallotNumber ballot = new BallotNumber(paxosMsg.getProposalID(), paxosMsg.getProposalNodeID());
			int ret = this.learnerState.learnValue(paxosMsg.getInstanceID(), ballot, paxosMsg.getValue(), getLastChecksum());
			if(ret != 0) {
				logger.error("LearnState.LearnValue fail, ret {}.", ret);
				return ;
			}
			
			logger.debug("END LearnValue OK, proposalid {} proposalid_nodeid {} valueLen {}.", paxosMsg.getProposalID(), paxosMsg.getNodeID(), paxosMsg.getValue().length);
		}
		
		if(paxosMsg.getFlag() == PaxosMsgFlagType.sendLearnValueNeedAck.getValue()) {
			//every time' when receive valid need ack learn value, reset noop timeout.
			resetAskforLearnNoop(InsideOptions.getInstance().getAskforLearnInterval());
			sendLearnValueAck(paxosMsg.getNodeID());
		}
	}

	public void sendLearnValueAck(long sendNodeID) {
		logger.debug("START LastAck.Instanceid {} Now.Instanceid {}.", this.lastAckInstanceID, getInstanceID());
		
		if(getInstanceID() < this.lastAckInstanceID + InsideOptions.getInstance().getLearnerReceiverAckLead()) {
			logger.debug("No need to ack");
			return ;
		}
		
		Breakpoint.getInstance().getLearnerBP().sendLearnValueAck(this.pConfig.getMyGroupIdx());
		this.lastAckInstanceID = getInstanceID();
		
		PaxosMsg msg = new PaxosMsg();
		msg.setInstanceID(getInstanceID());
		msg.setMsgType(PaxosMsgType.paxosLearnerSendLearnValueAck.getValue());
		msg.setNodeID(this.pConfig.getMyNodeID());
		
		sendMessage(sendNodeID, msg);
	}

	public void onSendLearnValueAck(PaxosMsg paxosMsg) {
		Breakpoint.getInstance().getLearnerBP().onSendLearnValueAck(this.pConfig.getMyGroupIdx());
		
		logger.debug("Msg.Ack.Instanceid {} Msg.from_nodeid {}.", paxosMsg.getInstanceID(), paxosMsg.getNodeID());
		this.learnerSender.ack(paxosMsg.getInstanceID(), paxosMsg.getNodeID());
	}

	public void proposerSendSuccess(long learnInstanceID, long proposalID) {
		Breakpoint.getInstance().getLearnerBP().proposerSendSuccess(this.pConfig.getMyGroupIdx());
		
		PaxosMsg msg = new PaxosMsg();
		msg.setMsgType(PaxosMsgType.paxosLearnerProposerSendSuccess.getValue());
		msg.setInstanceID(learnInstanceID);
		msg.setNodeID(this.pConfig.getMyNodeID());
		msg.setProposalID(proposalID);
		msg.setLastChecksum(getLastChecksum());
		
		broadcastMessage(msg, BroadcastMessageType.BroadcastMessage_Type_RunSelf_First.getType(), MessageSendType.UDP.getValue());
	}

	public void onProposerSendSuccess(PaxosMsg paxosMsg) {
		Breakpoint.getInstance().getLearnerBP().onProposerSendSuccess(this.pConfig.getMyGroupIdx());
		
		logger.debug("START Msg.InstanceID {} Now.InstanceID {} Msg.ProposalID {} State.AcceptedID {} State.AcceptedNodeID {}, Msg.from_nodeid {}", 
				paxosMsg.getInstanceID(), getInstanceID(), paxosMsg.getProposalID(), this.acceptor.getAcceptorState().getAcceptedBallot().getProposalID(), this.acceptor.getAcceptorState().getAcceptedBallot().getNodeId(), paxosMsg.getNodeID());
		
		if(paxosMsg.getInstanceID() != getInstanceID()) {
			//Instance id not same, that means not in the same instance, ignord.
			logger.debug("InstanceID not same, skip msg, paxosMsg instanceID {}, now instanceID {}.", paxosMsg.getInstanceID(), getInstanceID());
			return ;
		}
		
		if(this.acceptor.getAcceptorState().getAcceptedBallot().getProposalID() == 0) {
			//Not accept any yet.
			Breakpoint.getInstance().getLearnerBP().onProposerSendSuccessNotAcceptYet(this.pConfig.getMyGroupIdx());
			logger.debug("I haven't accpeted any proposal");
			return ;
		}
		
		BallotNumber ballot = new BallotNumber(paxosMsg.getProposalID(), paxosMsg.getNodeID());
		BallotNumber thisBallot = this.acceptor.getAcceptorState().getAcceptedBallot();
		if(thisBallot.getProposalID() != ballot.getProposalID() 
				|| thisBallot.getNodeId() != ballot.getNodeId()) {
			//Proposalid not same, this accept value maybe not chosen value.
			logger.warn("ProposalBallot not same to AcceptedBallot");
			Breakpoint.getInstance().getLearnerBP().onProposerSendSuccessBallotNotSame(this.pConfig.getMyGroupIdx());
			return ;
		}
			
		//learn value.
		this.learnerState.learnValueWithoutWrite(paxosMsg.getInstanceID(), 
				this.acceptor.getAcceptorState().getAcceptedValue(), this.acceptor.getAcceptorState().getCheckSum());
		Breakpoint.getInstance().getLearnerBP().onProposerSendSuccessSuccessLearn(this.pConfig.getMyGroupIdx());
		
		logger.debug("END Learn value OK, value {}, groupId : {}.", this.acceptor.getAcceptorState().getAcceptedValue().length, this.pConfig.getMyGroupIdx());
		
		transmitToFollower();
	}

	public void transmitToFollower() {
		if(this.pConfig.getMyFollowerCount() == 0) return ;
		
		PaxosMsg msg = new PaxosMsg();
		msg.setMsgType(PaxosMsgType.paxosLearnerSendLearnValue.getValue());
		msg.setInstanceID(getInstanceID());
		msg.setNodeID(this.pConfig.getMyNodeID());
		msg.setProposalNodeID(this.acceptor.getAcceptorState().getAcceptedBallot().getNodeId());
		msg.setProposalID(this.acceptor.getAcceptorState().getAcceptedBallot().getProposalID());
		msg.setValue(this.acceptor.getAcceptorState().getAcceptedValue());
		msg.setLastChecksum(getLastChecksum());
		
		broadcastMessageToFollower(msg, MessageSendType.TCP.getValue());
	}

	// learn noop
	public void askforLearnNoop(boolean isStart) {
		resetAskforLearnNoop(InsideOptions.getInstance().getAskforLearnInterval());
		this.isIMLearning = false;
		this.checkpointMgr.exitCheckpointMode();
		
		askforLearn();
		
		if(isStart) {
			askforLearn();
		}
	}

	public void resetAskforLearnNoop(int timeout) {
		JavaOriTypeWrapper<Long> askForLearnNoopTimerIDWrap = new JavaOriTypeWrapper<Long>();
		if(this.askForLearnNoopTimerID > 0) {
			askForLearnNoopTimerIDWrap.setValue(this.askForLearnNoopTimerID);
			this.ioLoop.removeTimer(askForLearnNoopTimerIDWrap);
			this.askForLearnNoopTimerID = askForLearnNoopTimerIDWrap.getValue();
		}
		
		this.ioLoop.addTimer(timeout, TimerType.learnerAskforlearnNoop.getValue(), askForLearnNoopTimerIDWrap);
		this.askForLearnNoopTimerID = askForLearnNoopTimerIDWrap.getValue();
	}

	// checkpoint logic
	public int sendCheckpointBegin(long sendNodeID, long uuid, long sequence, long checkpointInstanceID) {
		CheckpointMsg msg = new CheckpointMsg();
		msg.setMsgType(CheckpointMsgType.sendFile.getValue());
		msg.setNodeID(this.pConfig.getMyNodeID());
		msg.setFlag(CheckpointSendFileFlag.BEGIN.getValue());
		msg.setUuid(uuid);
		msg.setSequenceID(sequence);
		msg.setCheckpointInstanceID(checkpointInstanceID);
		
		return sendMessage(sendNodeID, msg, MessageSendType.TCP.getValue());
	}

	public int sendCheckpoint(long sendNodeID, long uuid, long sequence, long checkpointInstanceID,
			int checksum, String filePath, int smid, long offset, byte[] buffer) {
		CheckpointMsg msg = new CheckpointMsg();
		msg.setMsgType(CheckpointMsgType.sendFile.getValue());
		msg.setNodeID(this.pConfig.getMyNodeID());
		msg.setFlag(CheckpointSendFileFlag.ING.getValue());
		msg.setUuid(uuid);
		msg.setSequenceID(sequence);
		msg.setCheckpointInstanceID(checkpointInstanceID);
		msg.setChecksum(checksum);
		msg.setFilePath(filePath);
		msg.setSmID(smid);
		msg.setOffset(offset);
		msg.setBuffer(buffer);
		
		return sendMessage(sendNodeID, msg, MessageSendType.TCP.getValue());
	}

	public int sendCheckpointEnd(long sendNodeID, long uuid, long sequence, long checkpointInstanceID) {
		CheckpointMsg msg = new CheckpointMsg();
		msg.setMsgType(CheckpointMsgType.sendFile.getValue());
		msg.setNodeID(this.pConfig.getMyNodeID());
		msg.setFlag(CheckpointSendFileFlag.END.getValue());
		msg.setUuid(uuid);
		msg.setSequenceID(sequence);
		msg.setCheckpointInstanceID(checkpointInstanceID);
		
		return sendMessage(sendNodeID, msg, MessageSendType.TCP.getValue());
	}

	public void onSendCheckpoint(CheckpointMsg checkpointMsg) {
		int ret = 0;
		
		if(checkpointMsg.getFlag() == CheckpointSendFileFlag.BEGIN.getValue()) {
			ret = onSendCheckpointBegin(checkpointMsg);
		} else if(checkpointMsg.getFlag() == CheckpointSendFileFlag.ING.getValue()) {
			ret = onSendCheckpointIng(checkpointMsg);
		} else if(checkpointMsg.getFlag() == CheckpointSendFileFlag.END.getValue()) {
			ret = onSendCheckpointEnd(checkpointMsg);
		}
		
		if(ret != 0) {
			logger.error("[FAIL] reset checkpoint receiver and reset askforlearn");
			this.checkpointReceiver.reset();
			resetAskforLearnNoop(5000);
			sendCheckpointAck(checkpointMsg.getNodeID(), checkpointMsg.getUuid(), 
					checkpointMsg.getSequenceID(), CheckpointSendFileAckFlag.FAIL.getValue());
		} else {
			sendCheckpointAck(checkpointMsg.getNodeID(), checkpointMsg.getUuid(), 
					checkpointMsg.getSequenceID(), CheckpointSendFileAckFlag.OK.getValue());
			resetAskforLearnNoop(5000);
		}
	}

	public int sendCheckpointAck(long sendNodeID, long uuid, long sequence, int flag) {
		CheckpointMsg msg = new CheckpointMsg();
		msg.setMsgType(CheckpointMsgType.sendFileAck.getValue());
		msg.setNodeID(this.pConfig.getMyNodeID());
		msg.setUuid(uuid);
		msg.setSequenceID(sequence);
		msg.setFlag(flag);
		
		return sendMessage(sendNodeID, msg, MessageSendType.TCP.getValue());
	}

	public void onSendCheckpointAck(CheckpointMsg checkpointMsg) {
		if(this.checkpointSender != null && !this.checkpointSender.isEnd()) {
			if(checkpointMsg.getFlag() == CheckpointSendFileAckFlag.OK.getValue()) {
				this.checkpointSender.ack(checkpointMsg.getNodeID(), checkpointMsg.getUuid(), checkpointMsg.getSequenceID());
			} else {
				this.checkpointSender.end();
			}
		}
	}

	/**
	 * 创建check point sender，如果已经存在，返回null。方法处理逻辑轻，使用synchronized即可。
	 * @param sendNodeID
	 * @return
	 */
	public synchronized CheckpointSender getNewCheckpointSender(long sendNodeID) {
		if(this.checkpointSender == null || this.checkpointSender.isEnd()) {
			this.checkpointSender = new CheckpointSender(sendNodeID, this.pConfig, this, this.smFac, this.checkpointMgr);
			return this.checkpointSender;
		}
		
		return null;
	}

	public boolean isIMLatest() {
		return getInstanceID() + 1 >= this.highestSeenInstanceID;
	}

	public long getSeenLatestInstanceID() {
		return this.highestSeenInstanceID;
	}

	public void setSeenInstanceID(long instanceID, long fromNodeID) {
		if(instanceID > this.highestSeenInstanceID) {
			this.highestSeenInstanceID = instanceID;
			this.highestSeenInstanceIDFromNodeID = fromNodeID;
		}
	}

	public int onSendCheckpointBegin(CheckpointMsg checkpointMsg) {
		int ret = this.checkpointReceiver.newReceiver(checkpointMsg.getNodeID(), checkpointMsg.getUuid());
		if(ret == 0) {
			ret = this.checkpointMgr.setMinChosenInstanceID(checkpointMsg.getCheckpointInstanceID());
			if(ret != 0) {
				logger.error("SetMinChosenInstanceID fail, ret {} CheckpointInstanceID {}", ret, checkpointMsg.getCheckpointInstanceID());
				return ret;
			}
		}
		
		return ret;
	}

	public int onSendCheckpointIng(CheckpointMsg checkpointMsg) {
		Breakpoint.getInstance().getCheckpointBP().onSendCheckpointOneBlock();
		return this.checkpointReceiver.receiveCheckpoint(checkpointMsg);
	}

	public int onSendCheckpointEnd(CheckpointMsg checkpointMsg) {
		if(!this.checkpointReceiver.isReceiverFinish(checkpointMsg.getNodeID(), 
				checkpointMsg.getUuid(), checkpointMsg.getSequenceID())) {
			logger.error("receive end msg but receiver not finish");
			return -1;
		}
		
		Breakpoint.getInstance().getCheckpointBP().receiveCheckpointDone();
		
		long checkpointInstanceId = checkpointMsg.getCheckpointInstanceID();
		
		List<StateMachine> smList = this.smFac.getSmList();
		for(StateMachine sm : smList) {
			if(sm.getSMID() == Def.SYSTEM_V_SMID || sm.getSMID() == Def.MASTER_V_SMID) {
				continue;
			}
			
			String tmpDirPath = this.checkpointReceiver.getTmpDirPath(sm.getSMID());
			List<String> filePathList = FileUtils.iterDir(tmpDirPath);
			
			if(filePathList.size() == 0) {
				logger.info("this sm {} have no checkpoint", sm.getSMID());
	            continue;
			}
			
			int ret = sm.loadCheckpointState(this.pConfig.getMyGroupIdx(), tmpDirPath, 
					filePathList, checkpointInstanceId);
			if(ret != 0) {
				Breakpoint.getInstance().getCheckpointBP().receiveCheckpointAndLoadFail();
				return ret;
			}
		}
		
		Breakpoint.getInstance().getCheckpointBP().receiveCheckpointAndLoadSucc();
		this.checkpointMgr.setMaxChosenInstanceID(checkpointInstanceId);
		this.checkpointMgr.setMinChosenInstanceID(checkpointInstanceId);
		this.checkpointMgr.setMinChosenInstanceIDCache(checkpointInstanceId);
		this.instance.newInstanceFromInstanceId(checkpointInstanceId);
		this.checkpointMgr.exitCheckpointMode();
		logger.info("All sm init state ok, checkpointInstanceId={}.", checkpointInstanceId);
//		System.exit(-1);
		return 0;
	}

	public long getHighestSeenInstanceIDFromNodeID() {
		return highestSeenInstanceIDFromNodeID;
	}

	public void setHighestSeenInstanceIDFromNodeID(long highestSeenInstanceIDFromNodeID) {
		this.highestSeenInstanceIDFromNodeID = highestSeenInstanceIDFromNodeID;
	}

	public boolean isIMLearning() {
		return isIMLearning;
	}

	public void setIMLearning(boolean isIMLearning) {
		this.isIMLearning = isIMLearning;
	}
}













