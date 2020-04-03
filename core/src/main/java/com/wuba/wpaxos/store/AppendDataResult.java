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
 * append data result
 */
public class AppendDataResult {
    // Return code
    private AppendDataStatus status;
    // Where to start writing
    private long wroteOffset;
    // Write Bytes
    private int wroteBytes;
    // Message storage timestamp
    private long storeTimestamp;
    private int crc32;

    public AppendDataResult(AppendDataStatus status) {
        this(status, 0, 0, -1L, 0);
    }

    public AppendDataResult(AppendDataStatus status, long wroteOffset, int wroteBytes,
            long storeTimestamp, int crc32) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.storeTimestamp = storeTimestamp;
        this.crc32 = crc32;
    }

    public boolean isOk() {
        return this.status == AppendDataStatus.PUT_OK;
    }

    public AppendDataStatus getStatus() {
        return status;
    }

    public void setStatus(AppendDataStatus status) {
        this.status = status;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public void setWroteOffset(long wroteOffset) {
        this.wroteOffset = wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public void setWroteBytes(int wroteBytes) {
        this.wroteBytes = wroteBytes;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public int getCrc32() {
		return crc32;
	}

	public void setCrc32(int crc32) {
		this.crc32 = crc32;
	}

	@Override
    public String toString() {
        return "AppendDataResult [status=" + status + ", wroteOffset=" + wroteOffset + ", wroteBytes="
                + wroteBytes + ", storeTimestamp=" + storeTimestamp +  "]";
    }
}
