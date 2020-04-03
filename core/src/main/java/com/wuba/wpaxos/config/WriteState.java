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
 * 写入是否携带instance有效数据
 */
public class WriteState {
	private boolean hasPayLoad = false;
	private long instanceID;
	
	public WriteState() {}
	
	public WriteState(boolean hasPayLoad) {
		this.hasPayLoad = hasPayLoad;
	}

	public WriteState(boolean hasPayLoad, long instanceID) {
		super();
		this.hasPayLoad = hasPayLoad;
		this.instanceID = instanceID;
	}

	public boolean isHasPayLoad() {
		return hasPayLoad;
	}

	public void setHasPayLoad(boolean hasPayLoad) {
		this.hasPayLoad = hasPayLoad;
	}

	public long getInstanceID() {
		return instanceID;
	}

	public void setInstanceID(long instanceID) {
		this.instanceID = instanceID;
	}
}
