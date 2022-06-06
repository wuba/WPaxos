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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.Committer;
import com.wuba.wpaxos.Instance;
import com.wuba.wpaxos.checkpoint.Cleaner;
import com.wuba.wpaxos.checkpoint.Replayer;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.communicate.Communicate;
import com.wuba.wpaxos.communicate.NetWork;
import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.storemachine.InsideSM;
import com.wuba.wpaxos.storemachine.StateMachine;

/**
 * paxos group封装
 */
public class Group {
	private final Logger logger = LogManager.getLogger(Group.class);
    private Communicate communicate;
    private Config config;
    private Instance instance;
    private volatile int initRet;
    private Thread thread;
    
    public Group(LogStorage logStorage, NetWork netWork, InsideSM masterSM, int groupIdx, Options options) {
	    this.config = new Config(logStorage, options.isWriteSync(), options.getSyncInterval(), options.isUseMembership(),
			    options.getMyNode(), options.getNodeInfoMap().get(groupIdx), options.getFollowerNodeInfoList(),
			    groupIdx, options.getGroupCount(), options.getMembershipChangeCallback(), options.getPaxosLogCleanType(),
			    options.getLearnerSendSpeed(),netWork);
    	this.config.setPrepareTimeout(options.getPrepareTimeout());
    	this.config.setAcceptTimeout(options.getAcceptTimeout());
    	this.config.setAskForLearnTimeout(options.getAskForLearnTimeout());
    	this.communicate = new Communicate(config, netWork, options.getMyNode().getNodeID(), options.getUDPMaxSize());
    	instance = new Instance(config, logStorage, communicate, options);
    	initRet = -1;
    	config.setMasterSM(masterSM);
    }
    
    public void startInit() {
    	thread = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					Group.this.init();
				} catch (Exception e) {
					logger.error("Group startInit error", e);
				}
			}
    	});
    	this.thread.start();
    }

    public void init() throws Exception {
    	config.init();
    	
    	addStateMachine(config.getSystemVSM());
    	addStateMachine(config.getMasterSM());
    	
    	this.initRet = this.instance.init();
    }

    public int getInitRet() throws InterruptedException {
    	this.thread.join();
    	return initRet;
    }

    public void start() {
    	this.instance.start();
    }

    public void stop() {
    	this.instance.stop();
    }

    public Config getConfig() {
    	return this.config;
    }

    public Instance getInstance() {
    	return this.instance;
    }

    public Committer getCommitter() {
    	return this.instance.getCommitter();
    }

    public Cleaner getCheckpointCleaner() {
    	return this.instance.getCheckpointCleaner();
    }

    public Replayer getCheckpointReplayer() {
    	return this.instance.getCheckpointReplayer();
    }

    public void addStateMachine(StateMachine poSM) {
    	this.instance.addStateMachine(poSM);
    }
}
