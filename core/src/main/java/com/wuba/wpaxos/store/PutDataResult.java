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
package com.wuba.wpaxos.store;

/**
 * put data result, instance file index and put status
 */
public class PutDataResult {
	private FileID fileID;
	private PutDataStatus putDataStatus;
	private AppendDataResult appendDataResult;
	
	public PutDataResult(PutDataStatus putDataStatus, AppendDataResult appendDataResult) {
		super();
		this.putDataStatus = putDataStatus;
		this.appendDataResult = appendDataResult;
	}

	public FileID getFileID() {
		return fileID;
	}

	public void setFileID(FileID fileID) {
		this.fileID = fileID;
	}

	public PutDataStatus getPutDataStatus() {
		return putDataStatus;
	}

	public void setPutDataStatus(PutDataStatus putDataStatus) {
		this.putDataStatus = putDataStatus;
	}

	public AppendDataResult getAppendDataResult() {
		return appendDataResult;
	}

	public void setAppendDataResult(AppendDataResult appendDataResult) {
		this.appendDataResult = appendDataResult;
	}
	
	public boolean isOk() {
		return this.appendDataResult != null && this.appendDataResult.isOk();
	}

	@Override
	public String toString() {
		return "PutDataResult [putDataStatus=" + putDataStatus + ", appendDataResult=" + appendDataResult + "]";
	}
}
