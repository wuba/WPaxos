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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.store.pagecache.MapedFile;
import com.wuba.wpaxos.utils.UtilAll;

/**
 * 记录存储模型最终一致的时间点
 */
public class StoreCheckpoint {
	private static final Logger log = LogManager.getLogger(StoreCheckpoint.class);
	private final RandomAccessFile randomAccessFile;
	private final FileChannel fileChannel;
	private final MappedByteBuffer mappedByteBuffer;
	private volatile long physicMsgTimestamp = 0;
	private volatile Map<Integer, Long> indexDBTimestamp = new HashMap<>();
	private ReentrantLock checkPointlock = new ReentrantLock();
	private int groupCount;

	public StoreCheckpoint(final String scpPath,int groupCount) throws IOException {
		File file = new File(scpPath);
		MapedFile.ensureDirOK(file.getParent());
		boolean fileExists = file.exists();
		this.groupCount = groupCount;
		this.randomAccessFile = new RandomAccessFile(file, "rw");
		this.fileChannel = this.randomAccessFile.getChannel();
		this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, MapedFile.OS_PAGE_SIZE);

		if (fileExists) {
			log.info("store checkpoint file exists, " + scpPath);
			this.mappedByteBuffer.position(0);
			this.physicMsgTimestamp = this.mappedByteBuffer.getLong();
			int groupIndex = 0;
			while (this.mappedByteBuffer.hasRemaining() && groupIndex++ < this.groupCount) {
				int groupId = this.mappedByteBuffer.getInt();
				long timeStamp = this.mappedByteBuffer.getLong();
				indexDBTimestamp.put(groupId, timeStamp);
			}

			log.info("store checkpoint file physicMsgTimestamp " + this.physicMsgTimestamp + ", "
					+ UtilAll.timeMillisToHumanString(this.physicMsgTimestamp));
		} else {
			log.info("store checkpoint file not exists, " + scpPath);
		}
	}

	public void shutdown() {
		this.flush();

		// unmap mappedByteBuffer
		MapedFile.clean(this.mappedByteBuffer);

		try {
			this.fileChannel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void flush() {
		try {
			checkPointlock.lock();
			this.mappedByteBuffer.position(0);
			this.mappedByteBuffer.putLong(this.physicMsgTimestamp);
			for (Map.Entry<Integer, Long> entry : indexDBTimestamp.entrySet()) {
				this.mappedByteBuffer.putInt(entry.getKey());
				this.mappedByteBuffer.putLong(entry.getValue());
			}
			this.mappedByteBuffer.force();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			checkPointlock.unlock();
		}
	}

	public long getPhysicMsgTimestamp() {
		return physicMsgTimestamp;
	}

	public void setPhysicMsgTimestamp(long physicMsgTimestamp) {
		this.physicMsgTimestamp = physicMsgTimestamp;
	}

	public long getMinTimestamp() {
		long min = physicMsgTimestamp;
		for (Map.Entry<Integer, Long> entry : indexDBTimestamp.entrySet()) {
			if (min > entry.getValue()) {
				min = entry.getValue();
			}
		}
		// 向前倒退3s，防止因为时间精度问题导致丢数据
		min -= 1000 * 3;
		if (min < 0) {
			min = 0;
		}

		return min;
	}

	public long getIndexDBTimestamp(int groupId) {
		return indexDBTimestamp.getOrDefault(groupId, 0L);
	}

	public void setIndexDBTimestamp(int groupId, long indexDBTimestamp) {
		this.indexDBTimestamp.put(groupId, indexDBTimestamp);
	}
}
