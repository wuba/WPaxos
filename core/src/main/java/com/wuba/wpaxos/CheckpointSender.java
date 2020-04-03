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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.checkpoint.CheckpointMgr;
import com.wuba.wpaxos.comm.breakpoint.Breakpoint;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.config.Def;
import com.wuba.wpaxos.storemachine.SMFac;
import com.wuba.wpaxos.storemachine.StateMachine;
import com.wuba.wpaxos.utils.Crc32;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import com.wuba.wpaxos.utils.OtherUtils;
import com.wuba.wpaxos.utils.Time;

/**
 * checkpoint发送
 */
public class CheckpointSender implements Runnable {
	private static final Logger logger = LogManager.getLogger(CheckpointSender.class);
	
	private static final int CHECKPOINT_ACK_LEAD = 10;
	private static final int CHECKPOINT_ACK_TIMEOUT = 120000;
	
	private long sendNodeID;	
	private Config config;	
	private Learner learner;	
	private SMFac smFac;	
	private CheckpointMgr checkpointMgr;
	private volatile boolean isEnd;	
	private volatile boolean isEnded;	
	private volatile boolean isStarted;	
	private long uuid;	
	private long sequenceId;	
	private long ackSequenceId;	
	private long absLastAckTime;	
	private Map<String, Boolean> alreadySendedFiles;
	
	public CheckpointSender(long sendNodeID, Config config, Learner learner,
			SMFac smFac, CheckpointMgr checkpointMgr) {
		super();
		this.sendNodeID = sendNodeID;
		this.config = config;
		this.learner = learner;
		this.smFac = smFac;
		this.checkpointMgr = checkpointMgr;
		
		this.isEnd = false;
		this.isEnded = false;
		this.isStarted = false;
		this.uuid = (this.config.getMyNodeID() ^ this.learner.getInstanceID()) + OtherUtils.fastRand();
		this.sequenceId = 0;
		
		this.ackSequenceId = 0;
		this.absLastAckTime = 0;
		this.alreadySendedFiles = new HashMap<String, Boolean>();
	}

	public void stop() throws Exception {
		if(this.isStarted && !this.isEnded) {
			this.isEnd = true;
			join();
		}
	}
	
	public void join() throws Exception {
    	Thread.currentThread().join();
    }
	
	@Override
	public void run() {
		try {
			this.isStarted = true;
			this.absLastAckTime = Time.getSteadyClockMS();
			
			//pause checkpoint replayer
			boolean needContinue = false;
			while(!this.checkpointMgr.getReplayer().isPaused()) {
				if(this.isEnd) {
					this.isEnded = true;
					return ;
				}
				
				needContinue = true;
				this.checkpointMgr.getReplayer().pause();
				logger.debug("wait replayer paused.");
				Time.sleep(100);
			}
			
			int ret = lockCheckpoint();
			if(ret == 0) {
				try {
					sendCheckpoint();
				} finally {
					unlockCheckpoint();
				}
				
			}
			
			//continue checkpoint replayer
			if(needContinue) {
				this.checkpointMgr.getReplayer().toContinue();
			}
			
			logger.info("Checkpoint.Sender [END]");
		} catch (Exception e) {
			logger.error("CheckpointSender run error", e);
		} finally {
			this.isEnded = true;
		}
	}
	
	public void end() {
		this.isEnd = true;
	}
	
	public boolean isEnd() {
		return this.isEnded;
	}
	
	public void ack(long sendNodeID, long uuid, long sequenceId) {
		if(sendNodeID != this.sendNodeID) {
			logger.error("send nodeid not same, ack.sendnodeid {} self.sendnodeid {}.", sendNodeID, this.sendNodeID);
			return ;
		}
		
		if(uuid != this.uuid) {
			logger.error("uuid not same, ack.uuid {} self.uuid {}.", uuid, this.uuid);
			return ;
		}
		
		if(sequenceId != this.ackSequenceId) {
			logger.error("ack_sequence not same, ack.ack_sequence {} self.ack_sequence {}.", sequenceId, this.ackSequenceId);
			return ;
		}
		
		this.ackSequenceId ++;
		this.absLastAckTime = Time.getSteadyClockMS();
	}
	
	public void sendCheckpoint() {
		int ret = this.learner.sendCheckpointBegin(this.sendNodeID, uuid, this.sequenceId, 
						this.smFac.getCheckpointInstanceID(this.config.getMyGroupIdx()));
		if(ret != 0) {
			logger.error("SendCheckpointBegin fail, ret= {}.", ret);
			return ;
		}
		
		Breakpoint.getInstance().getCheckpointBP().sendCheckpointBegin();
		
		this.sequenceId++;
		
		List<StateMachine> smList = this.smFac.getSmList();
		for(StateMachine sm : smList) {
			ret = sendCheckpointFaSM(sm);
			if(ret != 0) {
				return ;
			}
		}
		
		ret = this.learner.sendCheckpointEnd(this.sendNodeID, this.uuid, this.sequenceId, this.smFac.getCheckpointInstanceID(this.config.getMyGroupIdx()));
		if(ret != 0) {
			logger.error("SendCheckpointEnd fail, sequence {} ret {}.", this.sequenceId, ret);
		}
		Breakpoint.getInstance().getCheckpointBP().sendCheckpointEnd();
	}
	
	public int lockCheckpoint() {
		List<StateMachine> smList = this.smFac.getSmList();
		List<StateMachine> lockSmList = new ArrayList<StateMachine>();
		int ret = 0;
		for(StateMachine sm : smList) {
			ret = sm.lockCheckpointState();
			if(ret != 0) break;
			
			lockSmList.add(sm);
		}
		
		if(ret != 0) {
			for(StateMachine sm : lockSmList) {
				sm.unLockCheckpointState();
			}
		}
		
		return ret;
	}
	
	public void unlockCheckpoint() {
		List<StateMachine> smList = this.smFac.getSmList();
		for(StateMachine sm : smList) {
			sm.unLockCheckpointState();
		}
	}
	
	public int sendCheckpointFaSM(StateMachine sm) {
		JavaOriTypeWrapper<String> dirPath = new JavaOriTypeWrapper<String>();
		List<String> fileList = new ArrayList<String>();
		
		int ret = sm.getCheckpointState(this.config.getMyGroupIdx(), dirPath, fileList);
		if(ret != 0) {
			logger.error("GetCheckpointState fail ret {}, smid {}.", ret, sm.getSMID());
			return -1;
		}
		
		String oriDirPath = dirPath.getValue();
		if(oriDirPath == null || oriDirPath.length() == 0) {
			logger.info("No Checkpoint, smid {}.", sm.getSMID());
			return 0;
		}
		
		if(!oriDirPath.endsWith("/")) {
			oriDirPath += "/";
		}
		
		for(String filePath : fileList) {
			if(filePath == null || "".equals(filePath)) continue;
			ret = sendFile(sm, oriDirPath, filePath);
			if(ret != 0) {
				logger.error("SendFile fail, ret {} smid {}.", ret, sm.getSMID());
				return -1;
			}
		}
		
		logger.info("END, send ok, smid {} filelistcount {}.", sm.getSMID(), fileList.size());
		return 0;
	}
	
	public int sendFile(StateMachine sm, String dirPath, String filePath) {
		String path = dirPath + filePath;
		if(this.alreadySendedFiles.containsKey(path)) {
			logger.error("file already send, filepath {}.", path);
			return 0;
		}
		
		File file = new File(path);
		FileInputStream fis = null;;
		byte[] buffer = null;
		try {
			fis = new FileInputStream(file);
			int len = (int)file.length();
			buffer = new byte[len];
			fis.read(buffer, 0, len);
		} catch (Exception e) {
			logger.error("sendFile error", e);
			return -1;
		} finally {
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					logger.error("sendFile close file error", e);
				}
			}
		}
		
		sendBuffer(sm.getSMID(), sm.getCheckpointInstanceID(this.config.getMyGroupIdx()), filePath, 0, buffer);
		this.alreadySendedFiles.put(path, true);
		return 0;
	}
	
	public int sendBuffer(int smId, long checkpointInstanceID, String filePath, long offset, byte[] buffer) {
		int checkSum = Crc32.crc32(0, buffer, buffer.length, Def.CRC32SKIP);
		int ret = 0;
		
		while(true) {
			try {
				if(this.isEnd) {
					return -1;
				}
				
				if(!checkAck(this.sequenceId)) {
					return -1;
				}
				
				ret = this.learner.sendCheckpoint(this.sendNodeID, this.uuid, this.sequenceId, checkpointInstanceID, 
						checkSum, filePath, smId, offset, buffer);
				
				Breakpoint.getInstance().getCheckpointBP().sendCheckpointOneBlock();
				
				if(ret == 0) {
					this.sequenceId ++;
					break;
				} else {
					logger.error("SendCheckpoint fail, ret {} need sleep 30s", ret);
					Time.sleep(30000);
				}
			} catch (Exception e) {
				logger.error("sendBuffer throws exception", e);
			}
		}
		
		return ret;
	}
	
	public boolean checkAck(long sendSequenceId) {
		while(sendSequenceId > this.ackSequenceId + CHECKPOINT_ACK_LEAD) {
			try {
				long nowTime = Time.getSteadyClockMS();
				long passTime = nowTime > this.absLastAckTime ? nowTime - this.absLastAckTime : 0;
				
				if(this.isEnd) {
					return false;
				}
				
				if(passTime >= CHECKPOINT_ACK_TIMEOUT) {
					logger.error("Ack timeout, last acktime {}.", this.absLastAckTime);
					return false;
				}
				
				Time.sleep(20);
			} catch (Exception e) {
				logger.error("checkAck throws exception", e);
			}
		}
		
		return true;
	}
	
}


















