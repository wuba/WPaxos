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
 * propose result
 */
public class ProposeResult {
	// ret, 0 if success
	private int result;
	// return when propose success
	private long instanceID = 0;
	
	public ProposeResult(int result, long instanceID) {
		super();
		this.result = result;
		this.instanceID = instanceID;
	}
	
	public ProposeResult(int result) {
		super();
		this.result = result;
	}

	public int getResult() {
		return result;
	}
	
	public void setResult(int result) {
		this.result = result;
	}
	
	public long getInstanceID() {
		return instanceID;
	}
	
	public void setInstanceID(long instanceID) {
		this.instanceID = instanceID;
	}
}
