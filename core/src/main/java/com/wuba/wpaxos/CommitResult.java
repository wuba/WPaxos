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

/**
 * commit result
 */
public class CommitResult {
	int commitRet;
	long succInstanceID;
	
	public CommitResult(int commitRet, long succInstanceID) {
		super();
		this.commitRet = commitRet;
		this.succInstanceID = succInstanceID;
	}

	public int getCommitRet() {
		return commitRet;
	}

	public void setCommitRet(int commitRet) {
		this.commitRet = commitRet;
	}

	public long getSuccInstanceID() {
		return succInstanceID;
	}

	public void setSuccInstanceID(long succInstanceID) {
		this.succInstanceID = succInstanceID;
	}
}
