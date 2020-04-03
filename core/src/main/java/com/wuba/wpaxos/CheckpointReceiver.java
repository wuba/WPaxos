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

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.config.Config;
import com.wuba.wpaxos.proto.CheckpointMsg;
import com.wuba.wpaxos.store.LogStorage;
import com.wuba.wpaxos.utils.FileUtils;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;

/**
 * checkpoint接收
 */
public class CheckpointReceiver {
	private static final Logger logger = LogManager.getLogger(CheckpointReceiver.class);
	private Config config;
	private LogStorage logStorage;
	private long sendNodeID;
	private long uuid;
	private long sequenceID;
	private Map<String, Boolean> hasInitDir;
	
	public CheckpointReceiver(Config config, LogStorage logStorage) {
		super();
		this.config = config;
		this.logStorage = logStorage;
		this.hasInitDir = new HashMap<String, Boolean>();
		reset();
	}
	
	public void reset() {
		this.hasInitDir.clear();
		this.sendNodeID = 0;
		this.uuid = 0;
		this.sequenceID = 0;
	}
	
	public int newReceiver(long senderNodeID, long uuid) {
		int ret = clearCheckpointTmp();
		if(ret != 0) return ret;
		
		ret = this.logStorage.clearAllLog(this.config.getMyGroupIdx());
		if(ret != 0) {
			logger.error("ClearAllLog fail, groupidx {} ret {}.", this.config.getMyGroupIdx(), ret);
			return ret;
		}
		
		this.hasInitDir.clear();
		this.sendNodeID = senderNodeID;
		this.uuid = uuid;
		this.sequenceID = 0;
		
		return 0;
	}
	
	public boolean isReceiverFinish(long senderNodeID, long uuid, long endSequence) {
		if(senderNodeID == this.sendNodeID && this.uuid == uuid && endSequence == this.sequenceID + 1) {
			return true;
		} else {
			return false;
		}
	}
	
	public String getTmpDirPath(int smid) {
		String logStoragePath = this.logStorage.getLogStorageDirPath(this.config.getMyGroupIdx());
		String ret = logStoragePath + "/cp_tmp_" + smid;
		return ret;
	}
	
	public int receiveCheckpoint(CheckpointMsg checkpointMsg) {
		if(checkpointMsg.getNodeID() != this.sendNodeID || checkpointMsg.getUuid() != this.uuid) {
			logger.error("msg not valid, Msg.SenderNodeID {} Receiver.SenderNodeID {} Msg.UUID {} Receiver.UUID {}.", 
							checkpointMsg.getNodeID(), this.sendNodeID, checkpointMsg.getUuid(), this.uuid);
			return -2;
		}
		
		if(checkpointMsg.getSequenceID() == this.sequenceID) {
			logger.error("msg already receive, skip, Msg.Sequence {} Receiver.Sequence {}.", checkpointMsg.getSequenceID(), this.sequenceID);
			return 0;
		}
		
		if(checkpointMsg.getSequenceID() != this.sequenceID + 1) {
			logger.error("msg sequence wrong, Msg.Sequence {} Receiver.Sequence {}.", checkpointMsg.getSequenceID(), this.sequenceID);
			return -2;
		}
		
		String fileDir = getTmpDirPath(checkpointMsg.getSmID());
		JavaOriTypeWrapper<String> ffpWrap = new JavaOriTypeWrapper<String>();
		initFilePath(fileDir, ffpWrap);
		
		String filePath = fileDir + "/" + checkpointMsg.getFilePath();
		File file = new File(filePath);
		FileOutputStream out = null;
		try {
			if(!file.exists()) {
				file.createNewFile();
			}
			file.setReadable(true);
			file.setWritable(true);
			if(file.length() != checkpointMsg.getOffset()) {
				logger.error("file.length {} not equal to msg.offset {}.", file.length(), checkpointMsg.getOffset());
				return -2;
			}
			
			out = new FileOutputStream(file, true);
			out.write(checkpointMsg.getBuffer());
			out.flush();
			
			this.sequenceID ++;
			
		} catch (Exception e) {
			logger.error("receiveCheckpoint error", e);
			return -2;
		} finally {
			if(out != null) {
				try {
					out.close();
				} catch (Exception e) {
					logger.error("receiveCheckpoint close file error", e);
				}
			}
			
		}
		
		return 0;
	}
	
	public int initFilePath(String filePath, JavaOriTypeWrapper<String> formatFilePath) {
		String path = filePath + "/";
		formatFilePath.setValue(path);
		File dir = new File(path);
		dir.mkdirs();
		return 0;
	}
	
	public int clearCheckpointTmp() {
		String logStoragePath = this.logStorage.getLogStorageDirPath(this.config.getMyGroupIdx());
		
		File dir = new File(logStoragePath);
		File[] files = dir.listFiles();
		if (files == null) {
			return 0;
		}
		for(File file : files) {
			String fileName = file.getName();
			if(fileName.contains("cp_tmp_")) {
				FileUtils.deleteDir(logStoragePath + "/" + fileName);
			}
		}
		
		return 0;
	}
	
	public int createDir(String dirPath) {
		File file = new File(dirPath);
		file.mkdir();
		return 0;
	}
}




















