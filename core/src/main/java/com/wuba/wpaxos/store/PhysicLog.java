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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.config.WriteOptions;
import com.wuba.wpaxos.config.WriteState;
import com.wuba.wpaxos.store.config.StoreConfig;
import com.wuba.wpaxos.store.pagecache.MapedFile;
import com.wuba.wpaxos.store.pagecache.MapedFileQueue;
import com.wuba.wpaxos.store.service.AllocateMapedFileService;
import com.wuba.wpaxos.utils.ByteConverter;
import com.wuba.wpaxos.utils.Crc32;
import com.wuba.wpaxos.utils.OtherUtils;
import com.wuba.wpaxos.utils.UtilAll;

/**
 * paxos log value storage 封装
 */
public class PhysicLog {
	private static final Logger logger = LogManager.getLogger(PhysicLog.class);

	private final int groupIdx;
	private final StoreConfig storeConfig;
	private final MapedFileQueue mapedFileQueue;
	private final AppendDataCallback appendDataCallback;
	private final AllocateMapedFileService allocateMapedFileService;
	private final DefaultLogStorage logStorage;
	// 存储路径
	private final String storePath;
	private long minInstanceID = -1;
	// Message's MAGIC CODE daa320a7
	public final static int MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8;
	// End of file empty MAGIC CODE cbd43194
	private final static int BlankMagicCode = 0xBBCCDDEE ^ 1880681586 + 8;

	private final static int APPEND_HEADER_LEN = 16;/*totalLen + msgMagicCode + storeTimestamp*/
	private final static int APPEND_HEADER_MAGICCODE_OFFSET = 4;
	private final static int APPEND_HEADER_STORETIMESTAMP_OFFSET = 8;

	public PhysicLog(DefaultLogStorage logStorage, int groupIdx, String storePath, StoreConfig storeConfig, AllocateMapedFileService allocateMapedFileService) {
		super();
		this.logStorage = logStorage;
		this.groupIdx = groupIdx;
		this.storeConfig = storeConfig;
		this.allocateMapedFileService = allocateMapedFileService;
		this.mapedFileQueue = new MapedFileQueue(storePath, storeConfig.getMapedFileSizePhysic(), this.allocateMapedFileService);
		this.appendDataCallback = new DefaultAppendDataCallback(storeConfig.getMaxMessageSize());
		this.storePath = storePath;
	}

	public boolean load() {
		boolean result = this.mapedFileQueue.load();
		logger.info("init physic log " + (result ? "OK" : "Failed"));
		return result;
	}

	public void recoverWritePosition(long offset) {
		this.mapedFileQueue.setFlushedWhere(offset);
		this.mapedFileQueue.setCommittedWhere(offset);
		this.mapedFileQueue.truncateDirtyFiles(offset);
	}

	public void recoverNormally() {
		final List<MapedFile> mapedFiles = this.mapedFileQueue.getMapedFiles();
		if (!mapedFiles.isEmpty()) {
			// Began to recover from the last third file
			int index = mapedFiles.size() - 3;
			if (index < 0) {
				index = 0;
			}

			for (int idx = index; idx < mapedFiles.size(); idx++) {
				MapedFile mapedFile = mapedFiles.get(idx);
				try {
					mapedFile.createMappedFile();
				} catch (IOException e) {
					logger.error("recover mappedfile failed, file : " + mapedFile.getFileName(), e);
				}
			}

			MapedFile mapedFile = mapedFiles.get(index);
			ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
			long processOffset = mapedFile.getFileFromOffset();
			long mapedFileOffset = 0;
			while (true) {
				int size = this.checkMessageAndReturnSize(byteBuffer);
				// Normal data
				if (size > 0) {
					mapedFileOffset += size;
				}
				// Intermediate file read error
				else if (size == -1) {
					logger.info("recover physics file end, " + mapedFile.getFileName());
					break;
				}
				//
				// Come the end of the file, switch to the next file
				// Since the return 0 representatives met last hole, this can
				// not be included in truncate offset
				//
				else if (size == 0) {
					index++;
					if (index >= mapedFiles.size()) {
						// Current branch can not happen
						logger.info("recover last 3 physics file over, last maped file " + mapedFile.getFileName());
						break;
					} else {
						mapedFile = mapedFiles.get(index);
						byteBuffer = mapedFile.sliceByteBuffer();
						processOffset = mapedFile.getFileFromOffset();
						mapedFileOffset = 0;
						logger.info("recover next physics file, " + mapedFile.getFileName());
					}
				}
			}

			processOffset += mapedFileOffset;
			this.mapedFileQueue.setFlushedWhere(processOffset);
			this.mapedFileQueue.setCommittedWhere(processOffset);
			this.mapedFileQueue.truncateDirtyFiles(processOffset);

			int mapedFileSize = mapedFiles.size();
			mapedFile = this.mapedFileQueue.getMapedFiles().get(mapedFileSize - 1);
			if (!mapedFile.isFull() && storeConfig.isTransientStorePoolEnable()) {
				mapedFile.initWritebuffer(this.logStorage.getTransientStorePool());
			}
			
			this.correctMinInstanceID();
		}
	}

	public void recoverAbnormally() {
		final List<MapedFile> mapedFiles = this.mapedFileQueue.getMapedFiles();
		if (!mapedFiles.isEmpty()) {
			// Looking beginning to recover from which file
			int index = mapedFiles.size() - 1;
			MapedFile mapedFile = null;
			for (; index >= 0; index--) {
				mapedFile = mapedFiles.get(index);

				try {
					mapedFile.createMappedFile();
				} catch (IOException e) {
					logger.error("recover mappedfile failed, file : " + mapedFile.getFileName(), e);
				}

				if (this.isMapedFileMatchedRecover(mapedFile)) {
					logger.info("recover from this maped file " + mapedFile.getFileName());
					break;
				}
			}

			if (index < 0) {
				index = 0;
				mapedFile = mapedFiles.get(index);
			}

			ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
			long processOffset = mapedFile.getFileFromOffset();
			long mapedFileOffset = 0;
			while (true) {
				int size = this.checkMessageAndReturnSize(byteBuffer);
				// Normal data
				if (size > 0) {
					mapedFileOffset += size;
				}
				// Intermediate file read error
				else if (size == -1) {
					logger.info("recover physics file end, " + mapedFile.getFileName());
					break;
				}
				// Come the end of the file, switch to the next file
				// Since the return 0 representatives met last hole, this can
				// not be included in truncate offset
				else if (size == 0) {
					index++;
					if (index >= mapedFiles.size()) {
						// The current branch under normal circumstances should
						// not happen
						logger.info("recover physics file over, last maped file " + mapedFile.getFileName());
						break;
					} else {
						mapedFile = mapedFiles.get(index);
						byteBuffer = mapedFile.sliceByteBuffer();
						processOffset = mapedFile.getFileFromOffset();
						mapedFileOffset = 0;
						logger.info("recover next physics file, " + mapedFile.getFileName());
					}
				}
			}

			int mapedFileSize = mapedFiles.size();
			mapedFile = this.mapedFileQueue.getMapedFiles().get(mapedFileSize - 1);
			if (!mapedFile.isFull() && storeConfig.isTransientStorePoolEnable()) {
				mapedFile.initWritebuffer(this.logStorage.getTransientStorePool());
			}

			processOffset += mapedFileOffset;
			this.mapedFileQueue.setFlushedWhere(processOffset);
			this.mapedFileQueue.setCommittedWhere(processOffset);
			this.mapedFileQueue.truncateDirtyFiles(processOffset);
			
			this.correctMinInstanceID();
		} else {
			this.mapedFileQueue.setFlushedWhere(0);
			this.mapedFileQueue.setCommittedWhere(0);
		}
	}

	/**
	 * check the data and returns the data size
	 *
	 * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
	 */
	public int checkMessageAndReturnSize(ByteBuffer byteBuffer) {
		try {
			int initialPos = byteBuffer.position();

			// 1 TOTALSIZE
			int totalSize = byteBuffer.getInt();

			// 2 MAGICCODE
			int magicCode = byteBuffer.getInt();

			switch (magicCode) {
				case MessageMagicCode:
					break;
				case BlankMagicCode:
					return 0;
				default:
					logger.warn("subject {}, found a illegal magic code 0x{}, totalsize {}.", this.groupIdx, Integer.toHexString(magicCode), totalSize);
					return -1;
			}

			byteBuffer.position(initialPos + totalSize);
			return totalSize;
		} catch (Exception e) {
			logger.error("checkMessageAndReturnSize error.", e);
		}

		return -1;
	}

	private boolean isMapedFileMatchedRecover(final MapedFile mapedFile) {
		ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();

		int magicCode = byteBuffer.getInt(APPEND_HEADER_MAGICCODE_OFFSET);
		if (magicCode != MessageMagicCode) {
			return false;
		}

		long storeTimestamp = byteBuffer.getLong(APPEND_HEADER_STORETIMESTAMP_OFFSET);
		if (0 == storeTimestamp) {
			return false;
		}

		if (storeTimestamp <= this.logStorage.getStoreCheckpoint().getMinTimestamp()) {
			logger.info("find check timestamp, {} {}", storeTimestamp, UtilAll.timeMillisToHumanString(storeTimestamp));
			return true;
		}

		return false;
	}

	public void correctMaxPhyOffset(long maxPhyOffset) {
		try {
			this.mapedFileQueue.setFlushedWhere(maxPhyOffset);
			this.mapedFileQueue.setCommittedWhere(maxPhyOffset);
			this.mapedFileQueue.truncateDirtyFiles(maxPhyOffset);
		} catch (Exception e) {
			logger.error("correctMaxPhyOffset error.", e);
		}
	}

	public int deleteExpiredFile(//
	                             final long expiredTime, //
	                             final int deleteFilesInterval, //
	                             final long intervalForcibly, //
	                             final boolean cleanImmediately//
	) {
		return this.mapedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
	}

	public int deleteExpiredFile(final long offset) {
		return this.mapedFileQueue.deleteExpiredPhyFileByOffset(offset,storeConfig.getMapedFileSizePhysic());
	}

	public boolean retryDeleteFirstFile(final long intervalForcibly) {
		return this.mapedFileQueue.retryDeleteFirstFile(intervalForcibly);
	}

	public long getMinOffset() {
		MapedFile mapedFile = this.mapedFileQueue.getFirstMapedFileOnLock();
		if (mapedFile != null) {
			if (mapedFile.isAvailable()) {
				return mapedFile.getFileFromOffset();
			} else {
				return this.rollNextFile(mapedFile.getFileFromOffset());
			}
		}

		return -1;
	}
	
	public long correctMinInstanceID() {
		MapedFile mapedFile = this.mapedFileQueue.getFirstMapedFileOnLock();
		if (mapedFile != null) {
			GetResult result = mapedFile.selectMapedBuffer(0);
			if (result == null) {
				return -1;
			}

			try {
				byte[] rbytes = new byte[8];
				ByteBuffer buf = result.getByteBuffer();
				buf.getInt();
				int magicCode = buf.getInt();
				
				if (magicCode == PhysicLog.BlankMagicCode) {
					return -1;
				}
				
				buf.getLong();
				buf.get(rbytes, 0, 8);
				long instanceID = ByteConverter.bytesToLongLittleEndian(rbytes);
				this.minInstanceID = instanceID;
				return instanceID;
			} catch (Exception e) {
				logger.error("get data from logstore error.");
			} finally {
				result.release();
			}
		}

		return -1;
	}

	public long getMaxOffset() {
		return this.mapedFileQueue.getMaxOffset();
	}

	public void destroy() {
		this.mapedFileQueue.destroy();
	}

	public MapedFileQueue getMapedFileQueue() {
		return mapedFileQueue;
	}

	public long rollNextFile(final long offset) {
		int mapedFileSize = this.storeConfig.getMapedFileSizePhysic();
		return (offset + mapedFileSize - offset % mapedFileSize);
	}
	
	public static int getPayLoadStartOffset() {
		// TODO
		return 0;
	}
	
	public TreeMap<Long, FileID> getFileIds(final FileID startFileID) {
		TreeMap<Long, FileID> fileIdMap = new TreeMap<>();
		
		if (startFileID == null) {
			return fileIdMap;
		}
		
		long startOffset = startFileID.getOffset();
		int size = startFileID.getSize();
		MapedFile startFile = this.mapedFileQueue.findMapedFileByOffset(startOffset, false);
		int mappedFileSize = this.storeConfig.getMapedFileSizePhysic();
		
		MapedFile mappedFile = startFile;
		long mapedFileOffset = startOffset;
		while (true) {
			if (mappedFile == null) {
				break;
			}

			int pos = (int) (mapedFileOffset % mappedFileSize);
			GetResult result = mappedFile.selectMapedBuffer(pos);
			if (result == null) {
				break;
			}

			try {
				byte[] rbytes = new byte[size - APPEND_HEADER_LEN];
				ByteBuffer buf = result.getByteBuffer();
				int dataSize = buf.getInt();
				int magicCode = buf.getInt();
				
				if (magicCode == PhysicLog.BlankMagicCode) {
					mapedFileOffset = mappedFile.getFileFromOffset() + mappedFileSize;
					mappedFile = this.mapedFileQueue.findMapedFileByOffset(mapedFileOffset, false);
					continue;
				}
				
				buf.getLong();
				buf.get(rbytes, 0, size - APPEND_HEADER_LEN);
				long instanceID = ByteConverter.bytesToLongLittleEndian(rbytes);
				
				int crc32n = Crc32.crc32(rbytes);
				
				FileID fileID = new FileID(mapedFileOffset, crc32n, dataSize);
				fileIdMap.put(instanceID, fileID);
				
				mapedFileOffset += dataSize;
			} catch (Exception e) {
				logger.error("get data from logstore error.");
				break;
			} finally {
				result.release();
			}
		}
		
		return fileIdMap;
	}

	public GetResult getData(final long offset) {
		return this.getData(offset, offset == 0);
	}

	public GetResult getData(final long offset, final boolean returnFirstOnNotFound) {
		int mappedFileSize = this.storeConfig.getMapedFileSizePhysic();
		MapedFile mappedFile = this.mapedFileQueue.findMapedFileByOffset(offset, returnFirstOnNotFound);
		if (mappedFile != null) {
			int pos = (int) (offset % mappedFileSize);
			GetResult result = mappedFile.selectMapedBuffer(pos);
			return result;
		}

		return null;
	}

	public byte[] getData(FileID fileID) {
		long offset = fileID.getOffset();
		int size = fileID.getSize();
		long crc32 = fileID.getCrc32();

		if (size < APPEND_HEADER_LEN) {
			logger.error("getdata size is illegal.");
			return null;
		}

		byte[] rbytes = new byte[size - APPEND_HEADER_LEN];

		int mappedFileSize = this.storeConfig.getMapedFileSizePhysic();
		MapedFile mappedFile = this.mapedFileQueue.findMapedFileByOffset(offset, offset == 0);
		if (mappedFile != null) {
			int pos = (int) (offset % mappedFileSize);
			GetResult result = mappedFile.selectMapedBuffer(pos, size);
			if (result != null) {
				try {
					ByteBuffer buf = result.getByteBuffer();
					buf.position(APPEND_HEADER_LEN);
					buf.get(rbytes, 0, size - APPEND_HEADER_LEN);
				} catch (Exception e) {
					logger.error("get data from logstore error.", e);
					return null;
				} finally {
					result.release();
				}
			} else {
				logger.error("data maybe deleted, groupIdx {} offset {} size {}.", this.groupIdx, offset, size);
				return null;
			}

			int crc32n = Crc32.crc32(rbytes);
			if (crc32n != crc32) {
				logger.error("getdata checksum failed.");
				return null;
			}

			return rbytes;
		} else {
			logger.debug("getdata mappedFile null.");
			return null;
		}
	}


	public byte[] getData(long offset, int size) {
		if (size < APPEND_HEADER_LEN) {
			logger.error("getdata size is illegal.");
			return null;
		}
		byte[] rbytes = new byte[size - APPEND_HEADER_LEN];
		int mappedFileSize = this.storeConfig.getMapedFileSizePhysic();
		MapedFile mappedFile = this.mapedFileQueue.findMapedFileByOffset(offset, offset == 0);
		if (mappedFile != null) {
			int pos = (int) (offset % mappedFileSize);
			GetResult result = mappedFile.selectMapedBuffer(pos, size);
			if (result != null) {
				try {
					ByteBuffer buf = result.getByteBuffer();
					buf.position(APPEND_HEADER_LEN);
					buf.get(rbytes, 0, size - APPEND_HEADER_LEN);
				} catch (Exception e) {
					logger.error("get data from logstore error.", e);
					return null;
				} finally {
					result.release();
				}
			} else {
				logger.error("data maybe deleted, groupIdx {} offset {} size {}.", this.groupIdx, offset, size);
				return null;
			}
			return rbytes;
		} else {
			logger.error("getdata mappedFile null.");
			return null;
		}
	}

	public PutDataResult appendData(WriteOptions writeOptions, long instanceID, byte[] data, WriteState writestate) {
		AppendDataResult result = null;
//    	synchronized (this) {
		MapedFile mapedFile = this.mapedFileQueue.getLastMapedFile();
		if (null == mapedFile) {
			logger.error("create maped file1 error, groupIdx: {}.", this.groupIdx);
			return new PutDataResult(PutDataStatus.CREATE_MAPEDFILE_FAILED, null);
		}

		try {
			long startTimestamp = System.currentTimeMillis();
			result = mapedFile.appendData(data, appendDataCallback, writestate);
			if (writeOptions.isSync()) {
				this.mapedFileQueue.flush();
			}
			long costs = System.currentTimeMillis() - startTimestamp;
			if (costs > 100) {
				logger.info("TRACE PhysicCommitLog appendMessage costs {}", costs);
			}
		} catch (IOException e) {
			logger.error("mapedFile appendMessage", e);
			return new PutDataResult(PutDataStatus.CREATE_MAPEDFILE_FAILED, result);
		}

		switch (result.getStatus()) {
			case PUT_OK:
				break;
			case END_OF_FILE:
				// Create a new file, re-write the message
				mapedFile = this.mapedFileQueue.getLastMapedFile();
				if (null == mapedFile) {
					// XXX: warn and notify me
					logger.error("create maped file1 error, groupIdx: {}.", this.groupIdx);
					return new PutDataResult(PutDataStatus.CREATE_MAPEDFILE_FAILED, null);
				}
				try {
					result = mapedFile.appendData(data, appendDataCallback, writestate);
				} catch (IOException e) {
					logger.error("mapedFile appendMessage", e);
					return new PutDataResult(PutDataStatus.CREATE_MAPEDFILE_FAILED, result);
				}
				break;
			case MESSAGE_SIZE_EXCEEDED:
				return new PutDataResult(PutDataStatus.MESSAGE_ILLEGAL, result);
			case UNKNOWN_ERROR:
				return new PutDataResult(PutDataStatus.UNKNOWN_ERROR, result);
			default:
				return new PutDataResult(PutDataStatus.UNKNOWN_ERROR, result);
		}

		PutDataResult putDataResult = new PutDataResult(PutDataStatus.PUT_OK, result);
		return putDataResult;
	}

	public boolean appendData(long startOffset, byte[] data) throws IOException {
		synchronized (this) {
			MapedFile mappedFile = this.mapedFileQueue.getLastMapedFile(startOffset);
			if (null == mappedFile) {
				logger.error("appendData getLastMappedFile error {}.", startOffset);
				return false;
			}

			boolean result = mappedFile.appendData(data);
			return result;
		} //spin...
	}

	public String getStorePath() {
		return storePath;
	}

	public int getGroupIdx() {
		return groupIdx;
	}

	public long getMinInstanceID() {
		return minInstanceID;
	}

	public void setMinInstanceID(long minInstanceID) {
		this.minInstanceID = minInstanceID;
	}

	static class DefaultAppendDataCallback implements AppendDataCallback {
		// File at the end of the minimum fixed length empty
		private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4 + 8;
		// Store the message content
		private final ByteBuffer msgStoreItemMemory;
		// The maximum length of the message
		private final int maxMessageSize;

		DefaultAppendDataCallback(final int size) {
			this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
			this.maxMessageSize = size;
		}

		private void resetMsgStoreItemMemory(final int length) {
			this.msgStoreItemMemory.flip();
			this.msgStoreItemMemory.limit(length);
		}

		@Override
		public AppendDataResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank, byte[] msg, WriteState writestate) {
			long wroteOffset = fileFromOffset + byteBuffer.position();

			final int msgLen = APPEND_HEADER_LEN /*totalLen + msgMagicCode + storeTimestamp*/ + msg.length;

			if (msgLen > this.maxMessageSize) {
				PhysicLog.logger.warn("message size exceeded, msg total size: {}, maxMessageSize: {}.", msgLen, this.maxMessageSize);
				return new AppendDataResult(AppendDataStatus.MESSAGE_SIZE_EXCEEDED);
			}

			long storeTimestamp = System.currentTimeMillis();
			// Determines whether there is sufficient free space
			if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
				this.resetMsgStoreItemMemory(maxBlank);
				// MSGLEN
				this.msgStoreItemMemory.putInt(maxBlank);
				// MAGICCODE
				this.msgStoreItemMemory.putInt(PhysicLog.BlankMagicCode);
				// STORETIMESTAMP
				this.msgStoreItemMemory.putLong(storeTimestamp);
				
				// The remaining space may be any value

				// Here the length of the specially set maxBlank
				byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
				return new AppendDataResult(AppendDataStatus.END_OF_FILE, wroteOffset, maxBlank, OtherUtils.getSystemMS(), 0);
			}

			this.resetMsgStoreItemMemory(msgLen);

			this.msgStoreItemMemory.putInt(msgLen);

			this.msgStoreItemMemory.putInt(PhysicLog.MessageMagicCode);

			this.msgStoreItemMemory.putLong(storeTimestamp);

			this.msgStoreItemMemory.put(msg);

			byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);

			int crc32 = Crc32.crc32(msg);

			AppendDataResult result = new AppendDataResult(AppendDataStatus.PUT_OK, wroteOffset, msgLen, OtherUtils.getSystemMS(), crc32);

			return result;
		}
	}
}
