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

import com.wuba.wpaxos.base.Base;
import com.wuba.wpaxos.base.BaseMsg;
import com.wuba.wpaxos.checkpoint.CheckpointMgr;
import com.wuba.wpaxos.checkpoint.Cleaner;
import com.wuba.wpaxos.checkpoint.Replayer;
import com.wuba.wpaxos.comm.InsideOptions;
import com.wuba.wpaxos.comm.MsgTransport;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.comm.breakpoint.Breakpoint;
import com.wuba.wpaxos.comm.enums.CheckpointMsgType;
import com.wuba.wpaxos.comm.enums.MsgCmd;
import com.wuba.wpaxos.comm.enums.PaxosMsgType;
import com.wuba.wpaxos.comm.enums.TimerType;
import com.wuba.wpaxos.communicate.ReceiveMessage;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.config.PaxosNodeFunctionRet;
import com.wuba.wpaxos.config.PaxosTryCommitRet;
import com.wuba.wpaxos.proto.AcceptorStateData;
import com.wuba.wpaxos.proto.CheckpointMsg;
import com.wuba.wpaxos.proto.Header;
import com.wuba.wpaxos.proto.PaxosMsg;
import com.wuba.wpaxos.proto.PaxosValue;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.store.PaxosLog;
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.storemachine.SMFac;
import com.wuba.wpaxos.storemachine.StateMachine;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import com.wuba.wpaxos.utils.OtherUtils;
import com.wuba.wpaxos.utils.TimeStat;

/**
 * paxos instance
 */
public class Instance {
	private final Logger logger = LogManager.getLogger(Instance.class); 
	private Config config;
	private MsgTransport msgTransport;
	private SMFac smFac;
	private IoLoop ioLoop;
	private Acceptor acceptor;
	private Learner learner;
	private Proposer proposer;
	private PaxosLog paxosLog;
	private int lastChecksum;
	private CommitCtx commitCtx;
	private long commitTimerID;
	private Committer committer;
	private CheckpointMgr checkpointMgr;
	private TimeStat timeStat = new TimeStat();
	private Options options;
	private boolean started = false;
	
	public Instance(Config config, LogStorage logStorage, MsgTransport msgTransport, Options options) {
		this.config = config;
		this.msgTransport = msgTransport;
		this.commitTimerID = 0;
		this.lastChecksum = 0;
		this.smFac = new SMFac(config.getMyGroupIdx());
		this.ioLoop = new IoLoop(config, this);
		this.ioLoop.setName("ioLoop-"+config.getMyGroupIdx());
		this.options = options;
		this.checkpointMgr = new CheckpointMgr(this.config, this.smFac, logStorage, options.isUseCheckpointReplayer());
		this.acceptor = new Acceptor(config, this.msgTransport, this, logStorage);
		this.learner = new Learner(config, this.msgTransport, this, acceptor, logStorage, ioLoop, checkpointMgr, smFac);
		this.proposer = new Proposer(config, msgTransport, this, this.learner, this.ioLoop);
		this.paxosLog = new PaxosLog(logStorage);
		this.commitCtx = new CommitCtx(config);
		this.committer = new Committer(config, commitCtx, ioLoop, smFac,options.getCommitTimeout());
		this.timeStat = new TimeStat();
	}

	public int init() throws Exception {
		int ret = this.acceptor.init();
		if (ret != 0) {
			logger.error("Acceptor init failed, ret {}.", ret);
			return ret;
		}
		
		ret = this.checkpointMgr.init();
		if (ret != 0) {
			logger.error("CheckpointMgr init failed, ret {}.", ret);
			return ret;
		}
		
		logger.info("before fixCheckpointByMinChosenInstanceId, log instanceID {} , Checkpoint instanceID {}, group id {}.", this.acceptor.getInstanceID(), this.checkpointMgr.getCheckpointInstanceID() + 1, this.config.getMyGroupIdx());
		this.checkpointMgr.fixCheckpointByMinChosenInstanceId();
		
		long cpInstanceID = this.checkpointMgr.getCheckpointInstanceID() + 1;
		
		logger.info("checkpoint mgr init ok, log instanceID {} , Checkpoint instanceID {}, group id {}.", this.acceptor.getInstanceID(), cpInstanceID, this.config.getMyGroupIdx());
		
		long nowInstanceID = cpInstanceID;
		if (nowInstanceID < this.acceptor.getInstanceID()) {
			ret = playLog(nowInstanceID, this.acceptor.getInstanceID());
			if (ret != 0) {
				return ret;
			}
			
			logger.info("PlayLog ok, begin instanceid {} end instanceid {}, group id {}.", nowInstanceID, this.acceptor.getInstanceID(), this.config.getMyGroupIdx());
			
			nowInstanceID = this.acceptor.getInstanceID();
		} else {
			if (nowInstanceID > this.acceptor.getInstanceID()) {
				ret = protectionLogicIsCheckpointInstanceIDCorrect(nowInstanceID, this.acceptor.getInstanceID());
				if (ret != 0) {
					return ret;
				}
				this.acceptor.initForNewPaxosInstance();
			}
			
			this.acceptor.setInstanceID(nowInstanceID);
			logger.info("instance acceptor init instanceId : {}.", nowInstanceID);
		}
		
		logger.info("NowInstanceID {}.", nowInstanceID);
		
		this.learner.setInstanceID(nowInstanceID);
		this.proposer.setInstanceID(nowInstanceID);
		this.proposer.setStartProposalID(this.acceptor.getAcceptorState().getPromiseBallot().getProposalID() + 1);
		
		this.checkpointMgr.setMaxChosenInstanceID(nowInstanceID - 1);
		
		ret = initLastCheckSum();
		if (ret != 0) {
			return ret;
		}
		
		this.learner.resetAskforLearnNoop(InsideOptions.getInstance().getAskforLearnInterval());
		
		logger.info("instance init OK");
		
		return 0;
	}

	public void start() {
		this.learner.startLearnerSender();
		this.ioLoop.start();
		this.checkpointMgr.start();
		this.started = true;
	}
	
	public void stop() {
		if (this.started) {
			try {
				this.ioLoop.shutdown();
				this.checkpointMgr.stop();
				this.learner.stop();
			} catch (Exception e) {
				logger.error("instance stop failed.", e);
			}
		}
	}

	public int initLastCheckSum() {
		if (this.acceptor.getInstanceID() == 0) {
			this.lastChecksum = 0;
			return 0;
		}
		
		if (this.acceptor.getInstanceID() <= this.checkpointMgr.getMinChosenInstanceID()) {
			this.lastChecksum = 0;
			return 0;
		}
		
		AcceptorStateData state = new AcceptorStateData();
		int ret = this.paxosLog.readState(this.config.getMyGroupIdx(), this.acceptor.getInstanceID()-1, state);
		if (ret != 0 && ret != 1) {
			return ret;
		}
		
		if (ret == 1) {
			logger.error("last checksum not exist, now instanceid {}.", this.acceptor.getInstanceID());
			this.lastChecksum = 0;
			return 0;
		}
		
		this.lastChecksum = state.getCheckSum();
		
		logger.info("ok, last checksum {}.", this.lastChecksum);
		
		return 0;
	}

	public long getNowInstanceID() {
		return this.acceptor.getInstanceID();
	}

	public int getLastChecksum() {
		return this.lastChecksum;
	}

	public int getInstanceValue(long instanceID, PaxosValue paxosValue) {
		if (instanceID >= this.acceptor.getInstanceID()) {
			return PaxosNodeFunctionRet.Paxos_GetInstanceValue_Value_Not_Chosen_Yet.getRet();
		}
		
		AcceptorStateData state = new AcceptorStateData();
		int ret = this.paxosLog.readState(this.config.getMyGroupIdx(), instanceID, state);
		if (ret != 0 && ret != 1) {
			return -1;
		}
		
		if (ret == 1) {
			return PaxosNodeFunctionRet.Paxos_GetInstanceValue_Value_NotExist.getRet();
		}

		this.smFac.unPackPaxosValue(state.getAcceptedValue(), paxosValue);
		
		return 0;
	}

	public Committer getCommitter() {
		return this.committer;
	}

	public Cleaner getCheckpointCleaner() {
		return this.checkpointMgr.getCleaner();
	}

	public Replayer getCheckpointReplayer() {
		return this.checkpointMgr.getReplayer();
	}

	public void checkNewValue() {
		if (!this.commitCtx.isNewCommit()) {
			return;
		}
		
		if (!this.learner.isIMLatest()) {
			return;
		}
		
		if (this.config.isIMFollower()) {
			logger.error("I'm follower, skip this new value.");
			this.commitCtx.setResultOnlyRet(PaxosTryCommitRet.PaxosTryCommitRet_Follower_Cannot_Commit.getRet());
			return;
		}
		
		if (!this.config.checkConfig()) {
			logger.error("I'm not in membership, skip this new value.");
			this.commitCtx.setResultOnlyRet(PaxosTryCommitRet.PaxosTryCommitRet_Im_Not_In_Membership.getRet());
			return;
		}
		
		if (this.commitCtx.getCommitValue().length > InsideOptions.getInstance().getMaxBufferSize()) {
			logger.error("value size {} to large, skip this new value.", this.commitCtx.getCommitValue().length);
			this.commitCtx.setResultOnlyRet(PaxosTryCommitRet.PaxosTryCommitRet_Value_Size_TooLarge.getRet());
			return;	
		}
		
		this.commitCtx.startCommit(this.proposer.getInstanceID());
		
		JavaOriTypeWrapper<Long> timerIdWraper = new JavaOriTypeWrapper<Long>(0L);
		if (this.commitCtx.getTimeoutMs() != -1) {
			this.ioLoop.addTimer(this.commitCtx.getTimeoutMs(), TimerType.instanceCommitTimeout.getValue(), timerIdWraper);
		}
		this.commitTimerID = timerIdWraper.getValue();
		
		this.timeStat.point();
		
		if (this.config.getIsUseMembership() && (this.proposer.getInstanceID() == 0 || this.config.getGid() == 0)) {
			// Init system variables.
			logger.info("Need to init system variables, Now.InstanceID {} Now.Gid {}.", this.proposer.getInstanceID(), this.config.getGid());
			
			long gid = OtherUtils.genGid(this.config.getMyNodeID());
			byte[] initSVOpValue = this.config.getSystemVSM().createGidReTurnOpValue(gid);
			if (initSVOpValue != null) {
				initSVOpValue = this.smFac.packPaxosValue(initSVOpValue, initSVOpValue.length, this.config.getSystemVSM().getSMID());
				this.proposer.newValue(initSVOpValue);
			} 
		} else {
			if (this.options.isOpenChangeValueBeforePropose()) {
				this.smFac.beforePropose(this.config.getMyGroupIdx(),this.commitCtx);
			}
			this.proposer.newValue(this.commitCtx.getCommitValue());
		}
	}

	public void onNewValueCommitTimeout() {
		Breakpoint.getInstance().getInstanceBP().onNewValueCommitTimeout(this.config.getMyGroupIdx(), this.getNowInstanceID());
		
		this.proposer.exitPrepare();
		this.proposer.exitAccept();
		
		this.commitCtx.setResult(PaxosTryCommitRet.PaxosTryCommitRet_Timeout.getRet(), this.proposer.getInstanceID(), null);
	}

	// this funciton only enqueue, do nothing.
	public int onReceiveMessage(ReceiveMessage receiveMsg) {
		this.ioLoop.addMessage(receiveMsg);
		
		return 0;
	}

	public void onReceive(byte[] buf) {
		Breakpoint.getInstance().getInstanceBP().onReceive(this.config.getMyGroupIdx(), this.proposer.getInstanceID());
		
		if (buf.length <= 0) {
			logger.error("buffer size {} too short.", buf.length);
			return;
		}
		
		BaseMsg baseMsg = Base.unPackBaseMsg(buf);
		if (baseMsg == null) {
			return;
		}
		Header header = baseMsg.getHeader();	
		int cmd = header.getCmdid();
		if (cmd == MsgCmd.paxosMsg.getValue()) {
			if (this.checkpointMgr.inAskforcheckpointMode()) {
				logger.debug("in ask for checkpoint mode, ignored paxosmsg.");
				return;
			}
			
			PaxosMsg paxosMsg = (PaxosMsg) baseMsg.getBodyProto();
			if (paxosMsg == null) {
				return;
			}
			
			if (!receiveMsgHeaderCheck(header, paxosMsg.getNodeID())) {
				return;
			}
			onReceivePaxosMsg(paxosMsg, false);
		} else if (cmd == MsgCmd.checkpointMsg.getValue()) {
			CheckpointMsg checkpointMsg = (CheckpointMsg) baseMsg.getBodyProto();
			if (checkpointMsg == null) {
				return;
			}
			
			if (!receiveMsgHeaderCheck(header, checkpointMsg.getNodeID())) {
				return;
			}
			
			onReceiveCheckpointMsg(checkpointMsg);
		}
	}
	
	public void onReceive(ReceiveMessage receiveBuf) {
		onReceive(receiveBuf.getReceiveBuf());
	}

	public void onReceiveCheckpointMsg(CheckpointMsg checkpointMsg) {
		logger.debug("Now.InstanceID {} MsgType {} Msg.from_nodeid {} My.nodeid {} flag {}" + 
	            " uuid {} sequence {} checksum {} offset {} buffsize {} filepath {}.",
	            this.acceptor.getInstanceID(), checkpointMsg.getMsgType(), checkpointMsg.getNodeID(),
	            this.config.getMyNodeID(), checkpointMsg.getFlag(), checkpointMsg.getUuid(), checkpointMsg.getSequenceID(), checkpointMsg.getChecksum(),
	            checkpointMsg.getOffset(), checkpointMsg.getBuffer().length, checkpointMsg.getFilePath());
		
		try {
			if (checkpointMsg.getMsgType() == CheckpointMsgType.sendFile.getValue()) {
				if (!this.checkpointMgr.inAskforcheckpointMode()) {
					logger.info("not in ask for checkpoint mode, ignored checkpoint msg.");
					return;
				}
				this.learner.onSendCheckpoint(checkpointMsg);
			} else if (checkpointMsg.getMsgType() == CheckpointMsgType.sendFileAck.getValue()) {
				this.learner.onSendCheckpointAck(checkpointMsg);
			}
		} catch(Exception e) {
			logger.error("onReceiveCheckpointMsg execute failed.", e);
		}
	}

	public int onReceivePaxosMsg(PaxosMsg paxosMsg, boolean isRetry) {
		Breakpoint.getInstance().getInstanceBP().onReceivePaxosMsg(this.config.getMyGroupIdx(), this.getNowInstanceID());
		logger.debug("Now.InstanceID {} Msg.InstanceID {} MsgType {} Msg.from_nodeid {} My.nodeid {} Seen.LatestInstanceID {}.",
	            this.proposer.getInstanceID(), paxosMsg.getInstanceID(), paxosMsg.getMsgType(),
	            paxosMsg.getNodeID(), this.config.getMyNodeID(), this.learner.getSeenLatestInstanceID());
		
		if (this.proposer.getInstanceID() != paxosMsg.getInstanceID()) {
			logger.debug("Now.InstanceID {} nonEquals Msg.InstanceID {}.", this.proposer.getInstanceID(), paxosMsg.getInstanceID());
		}
		
		if (paxosMsg.getMsgType() == PaxosMsgType.paxosPrepareReply.getValue()
				|| paxosMsg.getMsgType() == PaxosMsgType.paxosAcceptReply.getValue()
				|| paxosMsg.getMsgType() == PaxosMsgType.paxosProposalSendNewValue.getValue()) {
			if (!this.config.isValidNodeID(paxosMsg.getNodeID())) {
				Breakpoint.getInstance().getInstanceBP().onReceivePaxosMsgNodeIDNotValid(this.config.getMyGroupIdx(), this.getNowInstanceID());
				logger.error("acceptor reply type msg, from nodeid not in my membership, skip this message");
				return 0;
			}
			
			return receiveMsgForProposer(paxosMsg);
		} else if (paxosMsg.getMsgType() == PaxosMsgType.paxosPrepare.getValue()
				|| paxosMsg.getMsgType() == PaxosMsgType.paxosAccept.getValue()) {
			 //if my gid is zero, then this is a unknown node.
			if (this.config.getGid() == 0) {
				this.config.addTmpNodeOnlyForLearn(paxosMsg.getNodeID());
			}
			
			if (!this.config.isValidNodeID(paxosMsg.getNodeID())) {
				logger.error("prepare/accept type msg, from nodeid not in my membership(or i'm null membership), " + 
	                    "skip this message and add node to tempnode, my gid {}",
	                    this.config.getGid());
				
				this.config.addTmpNodeOnlyForLearn(paxosMsg.getNodeID());
				return 0;
			}
			
			checksumLogic(paxosMsg);
			return receiveMsgForAcceptor(paxosMsg, isRetry);
		} else if (paxosMsg.getMsgType() == PaxosMsgType.paxosLearnerAskforLearn.getValue()
				|| paxosMsg.getMsgType() == PaxosMsgType.paxosLearnerSendLearnValue.getValue()
				|| paxosMsg.getMsgType() == PaxosMsgType.paxosLearnerProposerSendSuccess.getValue()
				|| paxosMsg.getMsgType() == PaxosMsgType.paxosLearnerComfirmAskforLearn.getValue()
				|| paxosMsg.getMsgType() == PaxosMsgType.paxosLearnerSendNowInstanceID.getValue()
				|| paxosMsg.getMsgType() == PaxosMsgType.paxosLearnerSendLearnValueAck.getValue()
				|| paxosMsg.getMsgType() == PaxosMsgType.paxosLearnerAskforCheckpoint.getValue()) {
			checksumLogic(paxosMsg);
			return receiveMsgForLearner(paxosMsg);
		} else {
			Breakpoint.getInstance().getInstanceBP().onReceivePaxosMsgTypeNotValid(this.config.getMyGroupIdx(), this.getNowInstanceID());
			logger.error("Invalid msgtype {}.", paxosMsg.getMsgType());
		}
		
		return 0;
	}

	public int receiveMsgForProposer(PaxosMsg paxosMsg) {
		if (paxosMsg.getNodeID() != this.config.getMyNodeID()) {
			logger.debug("1 Msg from other acceptor {}_{}.", paxosMsg.getNodeID(), this.config.getMyNodeID());
		}
		if (this.config.isIMFollower()) {
			logger.error("I'm follower, skip this message.");
			return 0;
		}
		
		if (paxosMsg.getInstanceID() != this.proposer.getInstanceID()) {
			if (paxosMsg.getInstanceID() + 1 == this.proposer.getInstanceID()) {
				if (paxosMsg.getMsgType() == PaxosMsgType.paxosPrepareReply.getValue()) {
					this.proposer.onExpiredPrepareReply(paxosMsg);
				} else if (paxosMsg.getMsgType() == PaxosMsgType.paxosAcceptReply.getValue()) {
					this.proposer.onExpiredAcceptReply(paxosMsg);
				}
			}
			
			Breakpoint.getInstance().getInstanceBP().onReceivePaxosProposerMsgInotsame(this.config.getMyGroupIdx(), this.getNowInstanceID());
			return 0;
		}
		
		if (paxosMsg.getMsgType() == PaxosMsgType.paxosPrepareReply.getValue()) {
			this.proposer.onPrepareReply(paxosMsg);
		} else if (paxosMsg.getMsgType() == PaxosMsgType.paxosAcceptReply.getValue()) {
			if (paxosMsg.getNodeID() != this.config.getMyNodeID()) {
				logger.debug("2 Msg from other acceptor {}_{}.", paxosMsg.getNodeID(), this.config.getMyNodeID());
			}
			this.proposer.onAcceptReply(paxosMsg);
		}
		
		return 0;
	}

	public int receiveMsgForAcceptor(PaxosMsg paxosMsg, boolean isRetry) {
		if (this.config.isIMFollower()) {
			logger.error("I'm follower, skip this message.");
			return 0;
		}
		
		if (paxosMsg.getInstanceID() != this.acceptor.getInstanceID()) {
			Breakpoint.getInstance().getInstanceBP().onReceivePaxosAcceptorMsgInotsame(this.config.getMyGroupIdx(), paxosMsg.getInstanceID());
		}
		
		if (paxosMsg.getInstanceID() == this.acceptor.getInstanceID() + 1) {
			// skip success message
			PaxosMsg newPaxosMsg = new PaxosMsg();
			newPaxosMsg.setInstanceID(this.acceptor.getInstanceID());
			newPaxosMsg.setMsgType(PaxosMsgType.paxosLearnerProposerSendSuccess.getValue());
			newPaxosMsg.setNodeID(paxosMsg.getNodeID());
			newPaxosMsg.setProposalID(paxosMsg.getProposalID());
			newPaxosMsg.setProposalNodeID(paxosMsg.getProposalNodeID());
			
			receiveMsgForLearner(newPaxosMsg);
		}
		
		if (paxosMsg.getInstanceID() == this.acceptor.getInstanceID()) {
			if (paxosMsg.getMsgType() == PaxosMsgType.paxosPrepare.getValue()) {
				return this.acceptor.onPrepare(paxosMsg);
			} else if (paxosMsg.getMsgType() == PaxosMsgType.paxosAccept.getValue()) {
				this.acceptor.onAccept(paxosMsg);
			}
		} else if ((!isRetry) && (paxosMsg.getInstanceID() > this.acceptor.getInstanceID())) {
			// retry msg can't retry again.
			if (paxosMsg.getInstanceID() >= this.learner.getSeenLatestInstanceID()) {
				if (paxosMsg.getInstanceID() < this.acceptor.getInstanceID() + IoLoop.RETRY_QUEUE_MAX_LEN) {
	                //need retry msg precondition
	                //1. prepare or accept msg
	                //2. msg.instanceid > nowinstanceid. 
	                //    (if < nowinstanceid, this msg is expire)
	                //3. msg.instanceid >= seen latestinstanceid. 
	                //    (if < seen latestinstanceid, proposer don't need reply with this instanceid anymore.)
	                //4. msg.instanceid close to nowinstanceid.
					this.ioLoop.addRetryPaxosMsg(paxosMsg);
					Breakpoint.getInstance().getInstanceBP().onReceivePaxosAcceptorMsgAddRetry(this.config.getMyGroupIdx(), this.getNowInstanceID());
				} else {
					// retry msg not series, no use.
					this.ioLoop.clearRetryQueue();
				}
			}
		}
		
		return 0;
	}

	public int receiveMsgForLearner(PaxosMsg paxosMsg) {
		if (paxosMsg.getMsgType() == PaxosMsgType.paxosLearnerAskforLearn.getValue()) {
			this.learner.onAskforLearn(paxosMsg);
		} else if (paxosMsg.getMsgType() == PaxosMsgType.paxosLearnerSendLearnValue.getValue()) {
			this.learner.onSendLearnValue(paxosMsg);
		} else if (paxosMsg.getMsgType() == PaxosMsgType.paxosLearnerProposerSendSuccess.getValue()) {
			this.learner.onProposerSendSuccess(paxosMsg);
		} else if (paxosMsg.getMsgType() == PaxosMsgType.paxosLearnerSendNowInstanceID.getValue()) {
			this.learner.onSendNowInstanceID(paxosMsg);
		} else if (paxosMsg.getMsgType() == PaxosMsgType.paxosLearnerComfirmAskforLearn.getValue()) {
			this.learner.onComfirmAskForLearn(paxosMsg);
		} else if (paxosMsg.getMsgType() == PaxosMsgType.paxosLearnerSendLearnValueAck.getValue()) {
			this.learner.onSendLearnValueAck(paxosMsg);
		} else if (paxosMsg.getMsgType() == PaxosMsgType.paxosLearnerAskforCheckpoint.getValue()) {
			try {
				this.learner.onAskforCheckpoint(paxosMsg);
			} catch (Exception e) {
				logger.error("learner onAskforCheckpoint error", e);
			}
		}
		
		if (this.learner.isLearned()) {
			Breakpoint.getInstance().getInstanceBP().onInstanceLearned(this.config.getMyGroupIdx(), this.getNowInstanceID());
			SMCtx smCtx = new SMCtx();
			boolean isMyCommit = this.commitCtx.isMycommit(this.learner.getInstanceID(), this.learner.getLearnValue(), smCtx);
			
			if (!isMyCommit) {
				Breakpoint.getInstance().getInstanceBP().onInstanceLearnedNotMyCommit(this.config.getMyGroupIdx(), this.getNowInstanceID());
				logger.debug("this value is not my commit, group : " + this.config.getMyGroupIdx());
			} else {
				int useTimeMs = this.timeStat.point();
				Breakpoint.getInstance().getInstanceBP().onInstanceLearnedIsMyCommit(useTimeMs, this.config.getMyGroupIdx(), this.getNowInstanceID());
				logger.debug("My commit ok, usetime {}ms, group : {}.", useTimeMs, this.config.getMyGroupIdx());
			}
			
			if (!smExecute(this.learner.getInstanceID(), this.learner.getLearnValue(), isMyCommit, smCtx)) {
				Breakpoint.getInstance().getInstanceBP().onInstanceLearnedSMExecuteFail(this.config.getMyGroupIdx(), this.getNowInstanceID());
				
				logger.error("SMExecute failed, instanceid {}, not increase instanceid.", this.learner.getInstanceID());
				this.commitCtx.setResult(PaxosTryCommitRet.PaxosTryCommitRet_ExecuteFail.getRet(), this.learner.getInstanceID(), this.learner.getLearnValue());
				
				this.proposer.cancelSkipPrepare();
				return -1;
			}
			
			{
				 //this paxos instance end, tell proposal done
				this.commitCtx.setResult(PaxosTryCommitRet.PaxosTryCommitRet_OK.getRet(), this.learner.getInstanceID(), this.learner.getLearnValue());
				
				if (this.commitTimerID > 0) {
					JavaOriTypeWrapper<Long> timerWrapper = new JavaOriTypeWrapper<Long>(this.commitTimerID);
					timerWrapper.setValue(this.commitTimerID);
					this.ioLoop.removeTimer(timerWrapper);
					this.commitTimerID = timerWrapper.getValue();
				}
			}
			
			logger.debug("[Learned] New paxos starting, Now.Proposer.InstanceID {}, Now.Acceptor.InstanceID {}, Now.Learner.InstanceID {}, group {}.",
					this.proposer.getInstanceID(), this.acceptor.getInstanceID(), this.learner.getInstanceID(), this.config.getMyGroupIdx());
			
			logger.debug("[Learned] Checksum change, last checksum {} new checksum {}, group {}.", this.lastChecksum, this.learner.getNewChecksum(), this.config.getMyGroupIdx());
			
			this.lastChecksum = this.learner.getNewChecksum();
			
			newInstance();
			
	        logger.debug("[Learned] New paxos instance has started, Now.Proposer.InstanceID {} Now.Acceptor.InstanceID {} Now.Learner.InstanceID {}.",
	                this.proposer.getInstanceID(), this.acceptor.getInstanceID(), this.learner.getInstanceID());
	        
	        this.checkpointMgr.setMaxChosenInstanceID(this.acceptor.getInstanceID());
	        
	        Breakpoint.getInstance().getInstanceBP().newInstance(this.config.getMyGroupIdx(), this.getNowInstanceID());
		}
		
		return 0;
	}

	public void onTimeout(long timerID, int type) {
		if (type == TimerType.proposerPrepareTimeout.getValue()) {
			logger.debug("TimerType.proposerPrepareTimeout timerID {}, type {}.", timerID, type);
			this.proposer.onPrepareTimeout();
		} else if (type == TimerType.proposerAcceptTimeout.getValue()) {
			logger.debug("TimerType.proposerAcceptTimeout timerID {}, type {}.", timerID, type);
			this.proposer.onAcceptTimeout();
		} else if (type == TimerType.learnerAskforlearnNoop.getValue()) {
			logger.debug("TimerType.learnerAskforlearnNoop timerID {}, type {}.", timerID, type);
			this.learner.askforLearnNoop(false);
		} else if (type == TimerType.instanceCommitTimeout.getValue()) {
			logger.debug("TimerType.instanceCommitTimeout timerID {}, type {}.", timerID, type);
			onNewValueCommitTimeout();
		} else {
			logger.error("unknown timer type {}, timeid {}.", type, timerID);
		}
	}

	public void addStateMachine(StateMachine sm) {
		this.smFac.addSM(sm);
	}

	public boolean smExecute(long instanceID, byte[] value, boolean isMyCommit, SMCtx smCtx) {
		return this.smFac.execute(this.config.getMyGroupIdx(), instanceID, value, smCtx);
	}

	public void checksumLogic(PaxosMsg paxosMsg) {
		if (paxosMsg.getLastChecksum() == 0) {
			return;
		}
		
		if (paxosMsg.getInstanceID() != this.acceptor.getInstanceID()) {
			return;
		}
		
		if (this.acceptor.getInstanceID() > 0 && this.getLastChecksum() == 0) {
			logger.error("I have no last checksum, other last checksum {}.", paxosMsg.getLastChecksum());
			this.lastChecksum = paxosMsg.getLastChecksum();
			return;
		}
		
		logger.debug("my last checksum {} other last checksum {} groupIdx {}.", this.getLastChecksum(), paxosMsg.getLastChecksum(), this.config.getMyGroupIdx());
		
		if (paxosMsg.getLastChecksum() != this.getLastChecksum()) {
			logger.error("checksum failed, my last checksum {} other last checksum {} groupIdx {} node {} instanceID {}.", getLastChecksum(), paxosMsg.getLastChecksum(), this.config.getMyGroupIdx(), paxosMsg.getProposalNodeID(), paxosMsg.getInstanceID());
			Breakpoint.getInstance().getInstanceBP().checksumLogicFail(this.config.getMyGroupIdx(), this.getNowInstanceID());
		}
	}

	public int playLog(long beginInstanceID, long endInstanceID) {
		if (beginInstanceID < this.checkpointMgr.getMinChosenInstanceID()) {
			logger.error("now instanceid {} small than min chosen instanceid {}.", beginInstanceID, this.checkpointMgr.getMinChosenInstanceID());
			return -2;
		}
			
		long minChosenInstanceId = this.checkpointMgr.getMinChosenInstanceID();
		long start = beginInstanceID;
		logger.info("play log, endInstanceID={}, minChosenInstanceId={}, execute checkpoint from start={}, groupid={}.",
				endInstanceID, minChosenInstanceId, start, this.config.getMyGroupIdx());
		
		for (long instanceID = start; instanceID < endInstanceID; instanceID++) {
			AcceptorStateData state = new AcceptorStateData();
			int ret = this.paxosLog.readState(this.config.getMyGroupIdx(), instanceID, state);
			if (ret != 0) {
				logger.error("log read failed, instanceid {} ret {}.", instanceID, ret);
				return ret;
			}
			
			boolean excuteRet = this.smFac.execute(this.config.getMyGroupIdx(), instanceID, state.getAcceptedValue(), null);
			if (!excuteRet) {
				logger.error("Execute failed, instanceid {}.", instanceID);
				return -1;
			}
		}
		
		return 0;
	}

	public boolean receiveMsgHeaderCheck(Header header, long fromNodeID) {
		if (this.config.getGid() == 0 || header.getGid() == 0) {
			return true;
		}
		
		if (this.config.getGid() != header.getGid()) {
			Breakpoint.getInstance().getAlgorithmBaseBP().headerGidNotSame();
			logger.error("Header check failed, header.gid {} config gid {}, msg from_nodeid {}.", header.getGid(), this.config.getGid(), fromNodeID);
			return false;
		}
		
		return true;
	}

	public int protectionLogicIsCheckpointInstanceIDCorrect(long cpInstanceID, long logMaxInstanceID) {
	    if (cpInstanceID <= logMaxInstanceID + 1)
	    {
	        return 0;
	    }

	    //checkpoint_instanceid larger than log_maxinstanceid+1 will appear in the following situations 
	    //1. Pull checkpoint from other node automatically and restart. (normal case)
	    //2. Paxos log was manually all deleted. (may be normal case)
	    //3. Paxos log is lost because Options::bSync set as false. (bad case)
	    //4. Checkpoint data corruption results an error checkpoint_instanceid. (bad case)
	    //5. Checkpoint data copy from other node manually. (bad case)
	    //In these bad cases, paxos log between [log_maxinstanceid, checkpoint_instanceid) will not exist
	    //and checkpoint data maybe wrong, we can't ensure consistency in this case.

	    if (logMaxInstanceID == 0)
	    {
	        //case 1. Automatically pull checkpoint will delete all paxos log first.
	        //case 2. No paxos log. 
	        //If minchosen instanceid < checkpoint instanceid.
	        //Then Fix minchosen instanceid to avoid that paxos log between [log_maxinstanceid, checkpoint_instanceid) not exist.
	        //if minchosen isntanceid > checkpoint.instanceid.
	        //That probably because the automatic pull checkpoint did not complete successfully.
	        long minChosenInstanceID = this.checkpointMgr.getMinChosenInstanceID();
	        if (this.checkpointMgr.getMinChosenInstanceID() != cpInstanceID)
	        {
	            int ret = this.checkpointMgr.setMinChosenInstanceID(cpInstanceID);
	            if (ret != 0)
	            {
	                logger.error("SetMinChosenInstanceID fail, now minchosen {} max instanceid {} checkpoint instanceid {}.",
	                		this.checkpointMgr.getMinChosenInstanceID(), logMaxInstanceID, cpInstanceID);
	                return -1;
	            }

	            logger.info("Fix minchonse instanceid ok, old minchosen {} now minchosen {} max {} checkpoint {}.",
	                    minChosenInstanceID, this.checkpointMgr.getMinChosenInstanceID(), logMaxInstanceID, cpInstanceID);
	        }

	        return 0;
	    }
	    else
	    {
	        //other case.
	        logger.error("checkpoint instanceid {} larger than log max instanceid {}. " + 
	                "Please ensure that your checkpoint data is correct. " +
	                "If you ensure that, just delete all paxos log data and restart.",
	                cpInstanceID, logMaxInstanceID);
	        return -2;
	    }
	}

	public void newInstance() {
		this.acceptor.newInstance();
		this.learner.newInstance();
		this.proposer.newInstance();
	}
	
	public void newInstanceFromInstanceId(long instanceId) {
		this.acceptor.newInstanceFromInstanceId(instanceId);
		this.learner.newInstanceFromInstanceId(instanceId);
		this.proposer.newInstanceFromInstanceId(instanceId);
	}
	
	public long getMinChosenInstanceID() {
		return this.checkpointMgr.getMinChosenInstanceID();
	}

	public boolean isLearning(){
		return learner.isIMLearning();
	}
}
