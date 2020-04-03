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
package com.wuba.wpaxos.comm;

/**
 * checkpoint文件相关信息
 */
public class CheckpointFileInfo {
	private String sFilePath;
	private int fileSize;
	
	public CheckpointFileInfo(String sFilePath, int fileSize) {
		super();
		this.sFilePath = sFilePath;
		this.fileSize = fileSize;
	}

	public String getsFilePath() {
		return sFilePath;
	}
	
	public void setsFilePath(String sFilePath) {
		this.sFilePath = sFilePath;
	}
	
	public int getFileSize() {
		return fileSize;
	}
	
	public void setFileSize(int fileSize) {
		this.fileSize = fileSize;
	}
}
