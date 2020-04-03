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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.proto.AcceptorStateData;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.store.PaxosLog;
import com.wuba.wpaxos.storemachine.SMFac;
import com.wuba.wpaxos.utils.Time;

/**
 * 异步checkpoint生成器
 */
public class Replayer implements Runnable {
	private static final Logger logger = LogManager.getLogger(Replayer.class);
	
    private Config config;
    private SMFac smFac;
    private PaxosLog paxosLog;
    private CheckpointMgr checkpointMgr;
    private volatile boolean canRun;
    private volatile boolean isPaused;
    private volatile boolean isEnd;
    
    public Replayer(Config config, SMFac smFac, LogStorage logStorage, CheckpointMgr checkpointMgr) {
		super();
		this.config = config;
		this.smFac = smFac;
		this.paxosLog = new PaxosLog(logStorage);
		this.checkpointMgr = checkpointMgr;
		this.canRun = false;
		this.isPaused = true;
		this.isEnd = false;
	}

	public void stop() throws Exception {
		this.isEnd = true;
		join();
	}
	
	private void join() throws Exception {
    	Thread.currentThread().join();
    }

    @Override
    public void run() {
    	long instanceId = this.smFac.getCheckpointInstanceID(this.config.getMyGroupIdx()) + 1;
    	
    	while(true) {
    		try {
				if(this.isEnd) {
					logger.debug("Checkpoint.Replayer [END]");
					return ;
				}
				
				if(!this.canRun) {
					this.isPaused = true;
					Time.sleep(1000);
					continue ;
				}
				
				if(instanceId >= this.checkpointMgr.getMaxChosenInstanceID()) {
					Time.sleep(1000);
					continue ;
				}
				
				boolean playRet = playOne(instanceId);
				if(playRet) {
					logger.info("Play one done, instanceid {}.", instanceId);
					instanceId ++;
				} else {
					logger.info("Play one fail, instanceid {}.", instanceId);
					Time.sleep(500);
				}
			} catch (Exception e) {
				logger.error("run throws exception", e);
			}
    	}
    }

    public void pause() {
    	this.canRun = false;
    }

    public void toContinue() {
    	this.isPaused = false;
    	this.canRun = true;
    }

    public boolean isPaused() {
    	return this.isPaused;
    }

    public boolean playOne(long instanceID) {
    	AcceptorStateData state = new AcceptorStateData();
    	int ret = this.paxosLog.readState(this.config.getMyGroupIdx(), instanceID, state);
    	if(ret != 0) {
    		return false;
    	}
    	
    	boolean executeRet = this.smFac.executeForCheckpoint(this.config.getMyGroupIdx(), instanceID, state.getAcceptedValue());
    	if(!executeRet) {
    		logger.error("Checkpoint sm excute fail, instanceid {}.", instanceID);
    	}
    	
    	return executeRet;
    }
    
}
















