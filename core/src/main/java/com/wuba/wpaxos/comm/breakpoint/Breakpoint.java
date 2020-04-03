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
package com.wuba.wpaxos.comm.breakpoint;

/**
 * 断点执行日志跟踪
 */
public class Breakpoint {

	private ProposerBP proposerBP = new ProposerBP();

	private AcceptorBP acceptorBP = new AcceptorBP();

	private LearnerBP learnerBP = new LearnerBP();

	private InstanceBP instanceBP = new InstanceBP();

	private CommiterBP commiterBP = new CommiterBP();

	private IOLoopBP ioLoopBP = new IOLoopBP();

	private NetworkBP networkBP = new NetworkBP();

	private LogStorageBP logStorageBP = new LogStorageBP();

	private AlgorithmBaseBP algorithmBaseBP = new AlgorithmBaseBP();

	private CheckpointBP checkpointBP = new CheckpointBP();

	private MasterBP masterBP = new MasterBP();

	private static Breakpoint breakpoint = new Breakpoint();
	
	private static boolean isLogOpen = true; 

	private Breakpoint() {
	}

	public static Breakpoint getInstance() {
		return breakpoint;
	}
	
	public void setInstance(Breakpoint newBreakpoint) {
		breakpoint = newBreakpoint;
	}

	public ProposerBP getProposerBP() {
		return this.proposerBP;
	}

	public AcceptorBP getAcceptorBP() {
		return this.acceptorBP;
	}

	public LearnerBP getLearnerBP() {
		return this.learnerBP;
	}

	public InstanceBP getInstanceBP() {
		return this.instanceBP;
	}

	public CommiterBP getCommiterBP() {
		return this.commiterBP;
	}

	public IOLoopBP getIOLoopBP() {
		return this.ioLoopBP;
	}

	public NetworkBP getNetworkBP() {
		return this.networkBP;
	}

	public LogStorageBP getLogStorageBP() {
		return this.logStorageBP;
	}

	public AlgorithmBaseBP getAlgorithmBaseBP() {
		return this.algorithmBaseBP;
	}

	public CheckpointBP getCheckpointBP() {
		return this.checkpointBP;
	}

	public MasterBP getMasterBP() {
		return this.masterBP;
	}

	public void setInstanceBP(InstanceBP instanceBP) {
		this.instanceBP = instanceBP;
	}

	public static boolean isLogOpen() {
		return isLogOpen;
	}

	public static void setLogOpen(boolean isLogOpen) {
		Breakpoint.isLogOpen = isLogOpen;
	}
}
