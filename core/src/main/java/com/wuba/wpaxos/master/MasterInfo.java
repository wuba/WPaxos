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
package com.wuba.wpaxos.master;

/**
 * master current info wrapper
 */
public class MasterInfo {
	private long masterNodeID = 0;
	private long masterVersion = 0;
	
	public MasterInfo() {}
	
	public MasterInfo(long masterNodeID, long masterVersion) {
		super();
		this.masterNodeID = masterNodeID;
		this.masterVersion = masterVersion;
	}
	
	public long getMasterNodeID() {
		return masterNodeID;
	}
	
	public void setMasterNodeID(long masterNodeID) {
		this.masterNodeID = masterNodeID;
	}
	
	public long getMasterVersion() {
		return masterVersion;
	}
	
	public void setMasterVersion(long masterVersion) {
		this.masterVersion = masterVersion;
	}

	@Override
	public String toString() {
		return "MasterInfo [masterNodeID=" + masterNodeID + ", masterVersion=" + masterVersion + "]";
	}
}
