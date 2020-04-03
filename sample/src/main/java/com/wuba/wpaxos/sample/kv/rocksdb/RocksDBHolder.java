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

import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public final class RocksDBHolder {
	private static final Logger logger = LogManager.getLogger(RocksDBHolder.class);
	private static RocksDB[] rocksDBS;

	private static String kvPath;

	public static void init(int groupCount, String rootPath) throws RocksDBException {
		kvPath = rootPath + File.separator + "kv";
		rocksDBS = new RocksDB[groupCount];
		for (int i = 0; i < groupCount; i++) {
			rocksDBS[i] = new RocksDB(i, kvPath);
			rocksDBS[i].init();
		}
	}

	public static void stop(int groupCount) {
		for (int i = 0; i < groupCount; i++) {
			rocksDBS[i].close();
		}
	}

	public static void put(byte[] key, byte[] value, int groupId) throws RocksDBException {
		rocksDBS[groupId].put(key, value);
	}

	public static byte[] get(byte[] key, int groupId) throws RocksDBException {
		return rocksDBS[groupId].get(key);
	}

	public static void delete(byte[] key, int groupId) throws RocksDBException {
		rocksDBS[groupId].delete(key);
	}

	public static RocksIterator newIterator(int groupId) {
		return rocksDBS[groupId].getRocksDB().newIterator();
	}

	public static boolean makeCheckPoint(int groupId, JavaOriTypeWrapper<String> dirPath, List<String> fileList) {
		String checkpointPath = kvPath + File.separator + groupId + File.separator + "checkpoint";
		try {
			rocksDBS[groupId].makeCheckPoint(checkpointPath);
			dirPath.setValue(checkpointPath);
			File file = new File(checkpointPath);
			if (file != null && file.list() != null) {
				fileList.addAll(Arrays.asList(file.list()));
			}
			return true;
		} catch (RocksDBException e) {
			logger.error("path {}", checkpointPath, e);
			return false;
		}
	}

	public static boolean learnCheckPoint(int groupId, String checkPointPath, List<String> fileList) {
		try {
			return rocksDBS[groupId].recoverCheckpoint(checkPointPath, fileList);
		} catch (RocksDBException e) {
			logger.error(e.getMessage(), e);
			return false;
		}
	}

	public static void cleanCheckPoint(int groupId) {
		String checkpointPath = kvPath + File.separator + groupId + File.separator + "checkpoint";
		File file = new File(checkpointPath);
		if (!file.exists() || file.listFiles() == null) {
			return;
		}
		for (File listFile : file.listFiles()) {
			listFile.delete();
		}
		file.delete();
		logger.info("clean checkpoint {}", checkpointPath);
	}
}
