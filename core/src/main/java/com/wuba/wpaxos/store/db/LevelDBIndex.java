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
package com.wuba.wpaxos.store.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.store.FileID;
import com.wuba.wpaxos.utils.ByteConverter;

import java.io.File;
import java.io.IOException;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 * 通过leveldb方式存储paxoslog索引
 */
public class LevelDBIndex implements IndexDB {
	private final Logger logger = LogManager.getLogger(LevelDBIndex.class);
	public static final long MINCHOSEN_KEY = Long.MAX_VALUE - 1;
	public static final long SYSTEMVARIABLES_KEY = Long.MAX_VALUE - 2;
	public static final long MASTERVARIABLES_KEY = Long.MAX_VALUE - 3;
	public static final String MAX_INSTANCEID_KEY = "MAX_INSTANCEID_KEY";

	private DB levelDB;
	private final int groupId;
	private final String storePath;

	public LevelDBIndex(int groupId, String storePath) {
		super();
		this.groupId = groupId;
		this.storePath = storePath;
	}

	@Override
	public void correctMaxInstanceID(long maxInstanceID) {
		levelDB.put(MAX_INSTANCEID_KEY.getBytes(), ByteConverter.longToBytesLittleEndian(maxInstanceID));
	}

	@Override
	public boolean putIndex(WriteOptions writeOptions, long instanceID, FileID value) {
		byte[] key = ByteConverter.longToBytesLittleEndian(instanceID);
		try {
			org.iq80.leveldb.WriteOptions leveldbOption = new org.iq80.leveldb.WriteOptions();
			leveldbOption.sync(writeOptions.isSync());
			this.levelDB.put(key, value.fileIdToBytes());this.levelDB.put(key, value.fileIdToBytes(), leveldbOption);
			return true;
		} catch (Exception e) {
		}
		return false;
	}

	@Override
	public FileID getIndex(long instanceID) {
		FileID fileId = null;
		try {
			byte[] fileIdBytes = levelDB.get(ByteConverter.longToBytesLittleEndian(instanceID));
			if (fileIdBytes == null) {
				return null;
			}
			fileId = FileID.parseFileIdFromBytes(fileIdBytes);
		} catch (Exception e) {
		}

		return fileId;
	}

	@Override
	public void deleteOneIndex(long instanceId) {
		levelDB.delete(ByteConverter.longToBytesLittleEndian(instanceId));
	}

	private void putToLevelDB(boolean sync, byte[] key, byte[] value) {
		org.iq80.leveldb.WriteOptions wo = new org.iq80.leveldb.WriteOptions();
		wo.sync(sync);
		this.levelDB.put(key, value, wo);
	}

	@Override
	public boolean init() {
		Options options = new Options();
		options.createIfMissing(true);
		options.writeBufferSize(8 * 1024 * 1024 + groupId * 10 * 1024);
		try {
			this.levelDB = factory.open(new File(storePath), options);
			return true;
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		}
		return false;
	}

	@Override
	public byte[] getMaxInstanceID() {
		return this.levelDB.get(MAX_INSTANCEID_KEY.getBytes());
	}

	@Override
	public void setMaxInstanceID(WriteOptions writeOptions, long instanceId) {
		putToLevelDB(true, MAX_INSTANCEID_KEY.getBytes(), ByteConverter.longToBytesLittleEndian(instanceId));
	}

	@Override
	public void setMastervariables(WriteOptions writeOptions, byte[] buffer) {
		putToLevelDB(true, ByteConverter.longToBytesLittleEndian(MASTERVARIABLES_KEY), buffer);
	}

	@Override
	public byte[] getMastervariables() {
		return levelDB.get(ByteConverter.longToBytesLittleEndian(MASTERVARIABLES_KEY));
	}

	@Override
	public byte[] getSystemvariables() {
		return levelDB.get(ByteConverter.longToBytesLittleEndian(SYSTEMVARIABLES_KEY));
	}

	@Override
	public void setSystemvariables(WriteOptions writeOptions, byte[] buffer) {
		putToLevelDB(true, ByteConverter.longToBytesLittleEndian(SYSTEMVARIABLES_KEY), buffer);
	}

	@Override
	public void setMinChosenInstanceID(WriteOptions writeOptions, long instanceId) {
		putToLevelDB(true, ByteConverter.longToBytesLittleEndian(MINCHOSEN_KEY), ByteConverter.longToBytesLittleEndian(instanceId));
	}

	@Override
	public byte[] getMinChosenInstanceID() {
		return levelDB.get(ByteConverter.longToBytesLittleEndian(MINCHOSEN_KEY));
	}

	@Override
	public void destroy() {
		try {
			levelDB.close();
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		}
	}

	@Override
	public void deleteExpire(WriteOptions writeOptions, long maxInstanceId) {
		byte[] bytes = levelDB.get(ByteConverter.longToBytesLittleEndian(MINCHOSEN_KEY));
		long minInstanceId = 0L;
		if (bytes != null) {
			minInstanceId = ByteConverter.bytesToLongLittleEndian(bytes);	
		}
		for (long instanceId = minInstanceId; instanceId <= maxInstanceId; instanceId++) {
			levelDB.delete(ByteConverter.longToBytesLittleEndian(instanceId));
		}
	}
}
