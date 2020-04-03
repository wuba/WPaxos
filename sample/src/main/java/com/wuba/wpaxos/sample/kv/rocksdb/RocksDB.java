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
package com.wuba.wpaxos.sample.kv.rocksdb;

import com.wuba.wpaxos.utils.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.List;

public class RocksDB {
	private static final Logger logger = LogManager.getLogger(RocksDB.class);
	private org.rocksdb.RocksDB rocksDB;

	private final int groupId;

	private String path;
	private WriteOptions writeOptions;

	public RocksDB(int groupId, String kvPath) {
		this.groupId = groupId;
		path = kvPath + File.separator + groupId;
	}

	public void init() throws RocksDBException {
		org.rocksdb.RocksDB.loadLibrary();
		Options options = new Options();
		writeOptions = new WriteOptions();
		writeOptions.setSync(false);
		writeOptions.setDisableWAL(true);
		options.setCreateIfMissing(RocksDbConfig.createIfMissing).setMaxOpenFiles(RocksDbConfig.maxOpenFile)
				.setWriteBufferSize(RocksDbConfig.writeBufferSize)
				.setMaxWriteBufferNumber(RocksDbConfig.maxWriteBufferNumber)
				.setMinWriteBufferNumberToMerge(RocksDbConfig.writeBufferNumberToMerge).
				setLevelZeroFileNumCompactionTrigger(RocksDbConfig.levelZeroFileNumCompactionTrigger)
				.setLevel0SlowdownWritesTrigger(RocksDbConfig.level0SlowdownWritesTrigger)
				.setLevel0StopWritesTrigger(RocksDbConfig.level0StopWritesTrigger)
				.setMaxBytesForLevelBase(RocksDbConfig.targetFileSizeBase)
				.setMaxBackgroundCompactions(RocksDbConfig.maxBackgroundCompactions)
				.setMaxBackgroundFlushes(RocksDbConfig.maxBackgroundFlushes)
				.setMemtablePrefixBloomSizeRatio(RocksDbConfig.memtablePrefixBloomSizeRatio)
				.setCompressionType(RocksDbConfig.compressionType)
				.setCompactionStyle(RocksDbConfig.compactionStyle)
				.optimizeLevelStyleCompaction()
				.setUseFsync(RocksDbConfig.useFsync)
				.setBloomLocality(RocksDbConfig.boolmFilter)
				.setStatsDumpPeriodSec(180)
				.setTargetFileSizeBase(RocksDbConfig.targetFileSizeBase);

		File file = new File(path);
		if (!file.exists()) {
			file.mkdirs();
		}
		this.rocksDB = org.rocksdb.RocksDB.open(options, path);
	}


	public byte[] get(byte[] key) throws RocksDBException {
		long start = System.currentTimeMillis();
		byte[] retBuf = this.rocksDB.get(key);
		long cost = System.currentTimeMillis() - start;
		if (cost > 200) {
			logger.warn("rocksdb get cost {}", cost);
		}
		return retBuf;
	}

	public void put(byte[] key, byte[] value) throws RocksDBException {
		long start = System.currentTimeMillis();
		this.rocksDB.put(writeOptions, key, value);
		long cost = System.currentTimeMillis() - start;
		if (cost > 200) {
			logger.warn("rocksdb put cost {}", cost);
		}
	}

	public void delete(byte[] key) throws RocksDBException {
		long start = System.currentTimeMillis();
		this.rocksDB.delete(key);
		long cost = System.currentTimeMillis() - start;
		if (cost > 200) {
			logger.warn("rocksdb delete cost {}", cost);
		}
	}

	public void close() {
		this.rocksDB.close();
		this.rocksDB = null;
		logger.info("rocksdb close.groupid {}", groupId);
	}

	public void makeCheckPoint(String path) throws RocksDBException {
		Checkpoint checkpoint = Checkpoint.create(rocksDB);
		checkpoint.createCheckpoint(path);
	}

	public boolean recoverCheckpoint(String checkPointPath, List<String> fileList) throws RocksDBException {
		if (rocksDB != null) {
			logger.info("rocksdb close");
			rocksDB.close();
			rocksDB = null;
		}
		File file = new File(path);
		FileUtils.deleteDir(path+".bak");
		if (!file.renameTo(new File(path + ".bak"))) {
			logger.error("rename file error");
			return false;
		}
		for (String s : fileList) {
			logger.info("checkpoint file {}",s);
		}
		File checkpoint = new File (checkPointPath);
		boolean b = checkpoint.renameTo(new File(path));
		if(!b){
			logger.error("rename chaeckpoint error");
			return false;
		}
		init();
		return true;
	}

	public org.rocksdb.RocksDB getRocksDB() {
		return rocksDB;
	}

	public void setRocksDB(org.rocksdb.RocksDB rocksDB) {
		this.rocksDB = rocksDB;
	}
}
