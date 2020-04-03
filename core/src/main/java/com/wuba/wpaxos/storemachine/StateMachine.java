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
package com.wuba.wpaxos.storemachine;

import java.util.List;

import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/**
 * 状态机接口
 */
public interface StateMachine {
	
	//Different state machine return different SMID().
	public int getSMID();
	
    //Return true means execute success. 
    //This 'success' means this execute don't need to retry.
    //Sometimes you will have some logical failure in your execute logic, 
    //and this failure will definite occur on all node, that means this failure is acceptable, 
    //for this case, return true is the best choice.
    //Some system failure will let different node's execute result inconsistent,
    //for this case, you must return false to retry this execute to avoid this system failure.
	public boolean execute(int groupIdx, long instanceID, byte[] paxosValue, SMCtx smCtx);
	
	public boolean executeForCheckpoint(int groupIdx, long instanceID, byte[] paxosValue);
	
    //Only need to implement this function while you have checkpoint.
    //Return your checkpoint's max executed instanceid.
    //Notice PhxPaxos will call this function very frequently.
	public long getCheckpointInstanceID(int groupIdx);
	
    //After called this function, the vecFileList that GetCheckpointState return's, can't be delelted, moved and modifyed.
	public int lockCheckpointState();
	
    //sDirpath is checkpoint data root dir path.
    //vecFileList is the relative path of the sDirPath.
	public int getCheckpointState(int groupIdx, JavaOriTypeWrapper<String> dirPath, List<String> fileList); 
	
    public void unLockCheckpointState();
    
    //Checkpoint file was on dir(sCheckpointTmpFileDirPath).
    //vecFileList is all the file in dir(sCheckpointTmpFileDirPath).
    //vecFileList filepath is absolute path.
    //After called this fuction, paxoslib will kill the processor. 
    //State machine need to understand this when restart.
    public int loadCheckpointState(int groupIdx, String checkpointTmpFileDirPath, List<String> fileList, long checkpointInstanceID);

    //You can modify your request at this moment.
    //At this moment, the state machine data will be up to date.
    //If request is batch, propose requests for multiple identical state machines will only call this function once.
    //Ensure that the execute function correctly recognizes the modified request.
    //Since this function is not always called, the execute function must handle the unmodified request correctly.
    public byte[] beforePropose(int groupIdx, byte[] sValue);

    //Because function BeforePropose much waste cpu,
    //Only NeedCallBeforePropose return true then weill call function BeforePropose.
    //You can use this function to control call frequency.
    //Default is false.
    public boolean needCallBeforePropose();

	public void fixCheckpointByMinChosenInstanceId(long minChosenInstanceID);
	
}
