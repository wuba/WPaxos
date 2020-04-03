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
package com.wuba.wpaxos.config;

/**
 * paxos commit结果状态
 */
public enum PaxosTryCommitRet {
    PaxosTryCommitRet_OK(0),
    PaxosTryCommitRet_Reject(-2),
    PaxosTryCommitRet_Conflict(14),
    PaxosTryCommitRet_ExecuteFail(15),
    PaxosTryCommitRet_Follower_Cannot_Commit(16),
    PaxosTryCommitRet_Im_Not_In_Membership(17),
    PaxosTryCommitRet_Value_Size_TooLarge(18),
    PaxosTryCommitRet_Timeout(404),
    PaxosTryCommitRet_TooManyThreadWaiting_Reject(405);
	
    private int ret;
    
    private PaxosTryCommitRet(int ret) {
    	this.ret = ret;
    }
    
	public int getRet() {
		return ret;
	}
	
	public void setRet(int ret) {
		this.ret = ret;
	}
}
