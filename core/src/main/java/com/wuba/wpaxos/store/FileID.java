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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.utils.ByteConverter;

/**
 * paxos log instance index
 */
public class FileID {
	private static final Logger logger = LogManager.getLogger(FileID.class);
	private long offset;
	private int crc32;
	private int size;
	private static String SEPERATOR = "_";
	private static int paramNum = 3;
	
	private FileID() {}
	
	public FileID(long offset, int crc32, int size) {
		super();
		this.offset = offset;
		this.crc32 = crc32;
		this.size = size;
	}
	
	public byte[] fileIdToBytes() throws Exception {
		ByteArrayOutputStream stream = null;
		try {
			stream = new ByteArrayOutputStream();
			stream.write(ByteConverter.longToBytesLittleEndian(offset));
			stream.write(ByteConverter.intToBytesLittleEndian(this.size));
			stream.write(ByteConverter.intToBytesLittleEndian(this.crc32));
			return stream.toByteArray();
		} catch(Exception e) {
			throw new Exception("fileID to bytes failed.", e);
		} finally {
			if (stream != null) {
				try {
					stream.close();
				} catch (IOException e) {
					throw new Exception("fileID to bytes failed.", e);
				}
			}
		}
	}
	
	public static FileID parseFileIdFromBytes(byte[] buf) {
		FileID fileID = new FileID();
		
		int index = 0;
		long offset = ByteConverter.bytesToLongLittleEndian(buf, index);
		index += 8;
		
		int size = ByteConverter.bytesToIntLittleEndian(buf, index);
		index += 4;
		
		int crc32 = ByteConverter.bytesToIntLittleEndian(buf, index);
		index += 4;
		
		fileID.setOffset(offset);
		fileID.setSize(size);
		fileID.setCrc32(crc32);
		
		return fileID;
	}
	
	public String getFileIdStr() {
		StringBuilder stringBuild = new StringBuilder(8);
		stringBuild.append(offset)
					.append(SEPERATOR)
					.append(size)
					.append(SEPERATOR)
					.append(crc32);
		return stringBuild.toString();
	}
	
	public static FileID parseFileID(String fileIdStr) throws Exception {
		String[] strs = fileIdStr.split(SEPERATOR);
		if (strs.length != 3) {
			logger.error("parseFileID failed, fileIdStr length {}, expect length {}.", strs.length, paramNum);
		}
		
		try {
			FileID fileID = new FileID();
			fileID.setOffset(Long.parseLong(strs[0]));
			fileID.setSize(Integer.parseInt(strs[1]));
			fileID.setCrc32(Integer.parseInt(strs[2]));
			return fileID;
		} catch(Exception e) {
			throw new Exception("parseFileID failed.");
		}
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public int getCrc32() {
		return crc32;
	}

	public void setCrc32(int crc32) {
		this.crc32 = crc32;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	@Override
	public String toString() {
		return "FileID [offset=" + offset + ", crc32=" + crc32 + ", size="
				+ size + "]";
	}
}
