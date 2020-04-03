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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.wuba.wpaxos.store.config.StoreConfig;
import com.wuba.wpaxos.utils.LibC;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import sun.nio.ch.DirectBuffer;

/**
 * direct bytebuffer pool for mapedfiles
 */
@SuppressWarnings("restriction")
public class TransientStorePool {
    private static final Logger log = LogManager.getLogger(TransientStorePool.class);

    private final int poolSize;
    private final int fileSize;
    private final Deque<ByteBuffer> availableBuffers;
    private final Deque<ByteBuffer> availableBuffersIndexDB;
    private final int poolSizeIndexDB;
    private final int fileSizeIndexDB;
    private final StoreConfig storeConfig;

    public TransientStorePool(StoreConfig storeConfig, int groupCount) {
        this.storeConfig = storeConfig;
        this.poolSize = storeConfig.getTransientStorePoolSize();
        this.fileSize = storeConfig.getMapedFileSizePhysic();
        this.availableBuffers = new ConcurrentLinkedDeque<ByteBuffer>();
        this.availableBuffersIndexDB = new ConcurrentLinkedDeque<ByteBuffer>();
        this.poolSizeIndexDB = storeConfig.getTransientStoreIndexDBPoolSize();
        this.fileSizeIndexDB = storeConfig.getMapedFileSizeIndexDB();
    }

    /**
     * It's a heavy init method.
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            availableBuffers.offer(byteBuffer);
        }
        
        log.info("transientStorePool init success, poolSize {}", poolSize);
        
        for (int i = 0; i < poolSizeIndexDB; i++) {
        	ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSizeIndexDB);
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSizeIndexDB));
            
            availableBuffersIndexDB.offer(byteBuffer);
        }
        
        log.info("transientStoreIndexDBPool init success, poolSize {}", poolSizeIndexDB);
    }

    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
        
        for (ByteBuffer byteBuffer : availableBuffersIndexDB) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSizeIndexDB));
        }
    }

    public void returnBuffer(ByteBuffer byteBuffer) {
    	if (byteBuffer.capacity() == fileSizeIndexDB) {
            byteBuffer.position(0);
            byteBuffer.limit(fileSizeIndexDB);
            this.availableBuffersIndexDB.offerFirst(byteBuffer);
            log.info("transientStorePool returnIndexDbBuffer success, poolSize {}", availableBuffersIndexDB.size());
    	} else {
            byteBuffer.position(0);
            byteBuffer.limit(fileSize);
            this.availableBuffers.offerFirst(byteBuffer);
            log.info("transientStorePool returnBuffer success, poolSize {}", availableBuffers.size());
    	}
    }

    public ByteBuffer borrowBuffer() {
    	log.debug("TransientStorePool borrowBuffer, remain size {}.", availableBuffers.size());
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }
    
    public ByteBuffer borrowIndexDBBuffer() {
    	log.info("TransientStorePool borrowIndexdbBuffer, remain size {}.", availableBuffersIndexDB.size());
        ByteBuffer buffer = availableBuffersIndexDB.pollFirst();
        if (availableBuffersIndexDB.size() < this.poolSizeIndexDB * 0.4) {
            log.warn("TransientStorePool indexdb only remain {} sheets.", availableBuffersIndexDB.size());
        }
        return buffer;
    }
    
    public ByteBuffer borrowBuffer(int fileSize) {
    	if (fileSize == fileSizeIndexDB) {
    		return borrowIndexDBBuffer();
    	} else {
    		return borrowBuffer();
    	}
    } 

    public int remainBufferNumbs() {
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}
