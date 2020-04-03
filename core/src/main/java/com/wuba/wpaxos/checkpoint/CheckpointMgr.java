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
package com.wuba.wpaxos.checkpoint;

import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.storemachine.SMFac;
import com.wuba.wpaxos.utils.Time;

/**
 * checkpoint管理器
 */
public class CheckpointMgr {
	private static final Logger logger = LogManager.getLogger(CheckpointMgr.class);
	
	private Config config;
	private LogStorage logStorage;
	private SMFac smFac;
	private Replayer replayer;
	private Cleaner cleaner;
	private long minChosenInstanceID;
	private long maxChosenInstanceID;
	private boolean inAskforCheckpointMode;
	private Set<Long> needAckSet = new HashSet<Long>();
	private long lastAskforCheckpointTime;
	private boolean useCheckpointRelayer;
	private boolean useCheckpointCleaner;

	public CheckpointMgr(Config config, SMFac smFac, LogStorage logStorage, boolean useCheckpointRelayer) {
		super();
		this.config = config;
		this.logStorage = logStorage;
		this.smFac = smFac;
		this.replayer = new Replayer(config, smFac, logStorage, this);
		this.cleaner = new Cleaner(config, smFac, logStorage, this);
		this.minChosenInstanceID = 0;
		this.maxChosenInstanceID = 0;
		this.inAskforCheckpointMode = false;
		this.useCheckpointRelayer = useCheckpointRelayer;
		this.lastAskforCheckpointTime = 0;
		this.useCheckpointCleaner = config.isUseCheckpointCleaner();
	}
	
	public int init() {
		this.minChosenInstanceID = this.logStorage.getMinChosenInstanceID(this.config.getMyGroupIdx());
		return this.cleaner.fixMinChosenInstanceID(this.minChosenInstanceID);
	}
	
	public void start() {
		if(this.useCheckpointRelayer) {
			Thread rt = new Thread(this.replayer);
			rt.setName("CheckpointMgr-replayer");
			rt.start();
		}
		
		if (this.useCheckpointCleaner) {
			Thread ct = new Thread(this.cleaner);
			ct.setName("CheckpointMgr-cleaner");
			ct.start();
		}
	}
	
	public void stop() throws Exception {
		if(this.useCheckpointRelayer) {
			this.replayer.stop();
		}
		this.cleaner.stop();
	}
	
	public Replayer getReplayer() {
		return this.replayer;
	}
	
	public Cleaner getCleaner() {
		return this.cleaner;
	}
	
	public int prepareAskForCheckpoint(Long sendNodeID) {
		this.needAckSet.add(sendNodeID);
		
		if(this.lastAskforCheckpointTime == 0) {
			this.lastAskforCheckpointTime = Time.getSteadyClockMS();
		}
		
		long nowTime = Time.getSteadyClockMS();
		if(nowTime > this.lastAskforCheckpointTime + 60000) {
			logger.info("no majority reply, just ask for checkpoint, groupId {}.", this.config.getMyGroupIdx());
		} else {
			if(this.needAckSet.size() < this.config.getMajorityCount()) {
				logger.info("Need more other tell us need to askforcheckpoint, groupId {}.", this.config.getMyGroupIdx());
				return -2;
			}
		}
		
		this.lastAskforCheckpointTime = 0;
		this.inAskforCheckpointMode = true;
		this.needAckSet.clear();
		return 0;
	}
	
	public boolean inAskforcheckpointMode() {
		return this.inAskforCheckpointMode;
	}
	
	public void exitCheckpointMode() {
		this.inAskforCheckpointMode = false;
	}
	
	public long getMinChosenInstanceID() {
		return this.logStorage.getMinChosenInstanceID(this.config.getMyGroupIdx());
	}
    
	public int setMinChosenInstanceID(long minChosenInstanceID) {
		WriteOptions wo = new WriteOptions();
		wo.setSync(true);
		
		int ret = this.logStorage.setMinChosenInstanceID(wo, this.config.getMyGroupIdx(), minChosenInstanceID);
		if(ret != 0) {
			return ret;
		}
		
		this.minChosenInstanceID = minChosenInstanceID;
		return 0;
	}
    
	public void setMinChosenInstanceIDCache(long minChosenInstanceID) {
		this.minChosenInstanceID = minChosenInstanceID;
	}

	public long getCheckpointInstanceID() {
		return this.smFac.getCheckpointInstanceID(this.config.getMyGroupIdx());
	}

	public long getMaxChosenInstanceID() {
		return this.maxChosenInstanceID;
	}

	public void setMaxChosenInstanceID(long maxChosenInstanceID) {
		this.maxChosenInstanceID = maxChosenInstanceID;
	}

	public void fixCheckpointByMinChosenInstanceId() {
		this.smFac.fixCheckpointByMinChosenInstanceId(this.minChosenInstanceID);
	}
}















