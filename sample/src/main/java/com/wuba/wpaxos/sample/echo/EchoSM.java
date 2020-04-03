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
package com.wuba.wpaxos.sample.echo;

import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.storemachine.StateMachine;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

import java.util.List;

public class EchoSM implements StateMachine {
	public static final int SMID = 1;
	private int groupId;
	
	public EchoSM(int i) {
		this.groupId = i;
	}
	
	@Override
	public int getSMID() {
		return SMID;
	}

	@Override
	public boolean execute(int groupIdx, long instanceID, byte[] paxosValue, SMCtx smCtx) {
		System.out.println("[SM Execute] ok, smid " + this.getSMID() + " instanceid " + instanceID + " value " + new String(paxosValue));
		
		//only commiter node have SMCtx.
		if(smCtx != null && smCtx.getpCtx() != null) {
			EchoSMCtx peCtx = (EchoSMCtx)smCtx.getpCtx();
			peCtx.setExecuteRet(0);
			peCtx.setEchoRespValue(paxosValue);
		}
		
		executeForCheckpoint(groupIdx, instanceID, paxosValue);
		return true;
	}

	@Override
	public boolean executeForCheckpoint(int groupIdx, long instanceID, byte[] paxosValue) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long getCheckpointInstanceID(int groupIdx) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int lockCheckpointState() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getCheckpointState(int groupIdx, JavaOriTypeWrapper<String> dirPath, List<String> fileList) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void unLockCheckpointState() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int loadCheckpointState(int groupIdx, String checkpointTmpFileDirPath, List<String> fileList,
			long checkpointInstanceID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte[] beforePropose(int groupIdx, byte[] sValue) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean needCallBeforePropose() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void fixCheckpointByMinChosenInstanceId(long minChosenInstanceID) {
		// TODO Auto-generated method stub
		
	}

	public int getGroupId() {
		return groupId;
	}

	public void setGroupId(int groupId) {
		this.groupId = groupId;
	}
}
