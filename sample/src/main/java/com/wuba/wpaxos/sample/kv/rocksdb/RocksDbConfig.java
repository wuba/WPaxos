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

import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;

public class RocksDbConfig {
	public static final boolean createIfMissing = true;
	public static final int maxOpenFile = -1;
	public static final long writeBufferSize = 67108864;
	public static final int maxWriteBufferNumber = 16;
	public static final int writeBufferNumberToMerge = 1;
	public static final int levelZeroFileNumCompactionTrigger = 10;
	public static final int level0SlowdownWritesTrigger = 20;
	public static final int level0StopWritesTrigger = 40;
	public static final int maxBackgroundCompactions = 10;
	public static final int maxBackgroundFlushes = 1;
	public static final double memtablePrefixBloomSizeRatio = 0.125;
	public static final int boolmFilter = 10;
	public static final CompressionType compressionType = CompressionType.NO_COMPRESSION;
	public static final CompactionStyle compactionStyle = CompactionStyle.LEVEL;
	public static final boolean useFsync = false;
	public static final long targetFileSizeBase = 12582912;
	public static final long maxFileLevelBase = 10485760;
	public static final long maxLogFileSize = 5368709120L;
	public static final int maxBackgroundJob = 10;
}
