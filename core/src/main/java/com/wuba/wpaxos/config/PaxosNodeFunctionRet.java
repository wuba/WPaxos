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
 * Paxos运行过程异常状态
 */
public enum PaxosNodeFunctionRet {
    Paxos_SystemError(-1),
    Paxos_GroupIdxWrong(-5),
    Paxos_MembershipOp_GidNotSame(-501),
    Paxos_MembershipOp_VersionConflit(-502),
    Paxos_MembershipOp_NoGid(1001),
    Paxos_MembershipOp_Add_NodeExist(1002),
    Paxos_MembershipOp_Remove_NodeNotExist(1003),
    Paxos_MembershipOp_Change_NoChange(1004),
    Paxos_GetInstanceValue_Value_NotExist(1005),
    Paxos_GetInstanceValue_Value_Not_Chosen_Yet(1006);
    
    private int ret;
    
    private PaxosNodeFunctionRet(int ret) {
    	this.ret = ret;
    }
    
	public int getRet() {
		return ret;
	}
	
	public void setRet(int ret) {
		this.ret = ret;
	}
}
