/*
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
package com.wuba.wpaxos.store.service;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.wuba.wpaxos.store.TransientStorePool;
import com.wuba.wpaxos.store.config.StoreConfig;
import com.wuba.wpaxos.store.pagecache.MapedFile;
import com.wuba.wpaxos.utils.ServiceThread;
import com.wuba.wpaxos.utils.UtilAll;

/**
 * Create MapedFile in advance
 */
public class AllocateMapedFileService extends ServiceThread {
    private static final Logger log = LogManager.getLogger(AllocateMapedFileService.class);
    private static int WaitTimeOut = 1000 * 5;
    private ConcurrentHashMap<String, AllocateRequest> requestTable = new ConcurrentHashMap<String, AllocateRequest>();
    private PriorityBlockingQueue<AllocateRequest> requestQueue = new PriorityBlockingQueue<AllocateRequest>();
    private volatile boolean hasException = false;
    private TransientStorePool transientStorePool;
    private StoreConfig storeConfig;

    public AllocateMapedFileService(TransientStorePool transientStorePool, StoreConfig storeConfig) {
        this.transientStorePool = transientStorePool;
        this.storeConfig = storeConfig;
    }
    
    public MapedFile putRequestAndReturnMapedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
    	int canSubmitRequests = 2;
    	if (this.storeConfig.isTransientStorePoolEnable() && this.storeConfig.isFastFailIfNoBufferInStorePool()) {
    		canSubmitRequests = this.transientStorePool.remainBufferNumbs() - this.requestQueue.size();
    	}
    	
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;

        if (nextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.transientStorePool.remainBufferNumbs());
                this.requestTable.remove(nextFilePath);
                return null;
            }
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("never expected here, add a request to preallocate queue failed");
            }
            canSubmitRequests--;
        }

        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
        if (nextNextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.transientStorePool.remainBufferNumbs());
                this.requestTable.remove(nextNextFilePath);
            } else {
                boolean offerOK = this.requestQueue.offer(nextNextReq);
                if (!offerOK) {
                    log.warn("never expected here, add a request to preallocate queue failed");
                }
            }
        }

        if (hasException) {
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }

        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                boolean waitOK = result.getCountDownLatch().await(WaitTimeOut, TimeUnit.MILLISECONDS);
                if (!waitOK) {
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                }
                this.requestTable.remove(nextFilePath);
                return result.getMapedFile();
            } else {
                log.error("find preallocate mmap failed, this never happen");
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }

    @Override
    public String getServiceName() {
        return AllocateMapedFileService.class.getSimpleName();
    }

    @Override
    public void shutdown() {
        this.stopped = true;
        this.thread.interrupt();

        try {
            this.thread.join(this.getJointime());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (AllocateRequest req : this.requestTable.values()) {
            if (req.mapedFile != null) {
                log.info("delete pre allocated maped file, {}", req.mapedFile.getFileName()); 
                req.mapedFile.destroy(1000);
            }
        }
    }

	@Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!stopped && this.mmapOperation());

        log.info(this.getServiceName() + " service end");
    }

    /**
     * Only interrupted by the external thread, will return false
     */
    private boolean mmapOperation() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        try {
            req = this.requestQueue.take();
            AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
            if (null == expectedRequest) {
                log.warn("this mmap request expired, maybe cause timeout {} {} {} ", req.getFilePath(), req.getFileSize());
                return true;
            }
            if (expectedRequest != req) {
                log.warn("never expected here,  maybe cause timeout {}, req:{}, expectedRequest: {}.", req.getFilePath(), req.getFileSize(), req, expectedRequest);
                return true;
            }

            if (req.getMapedFile() == null) {
                long beginTime = System.currentTimeMillis();

                MapedFile mappedFile = null;
                if (this.storeConfig.isTransientStorePoolEnable() && (req.getFileSize() >= this.storeConfig.getMapedFileSizePhysic() || req.getFileSize() == this.storeConfig.getMapedFileSizeIndexDB())) {
                    try {
                    	log.debug("create mapedfile with bytebuffer pool, size {}.", req.getFileSize());
                    	mappedFile = new MapedFile(req.getFilePath(), req.getFileSize(), this.transientStorePool, true);
                    } catch (Exception e) {
                    	log.warn("create MapedFile failed.", e);
                    }
                } else {
                    mappedFile = new MapedFile(req.getFilePath(), req.getFileSize(), true);
                }

                long eclipseTime = UtilAll.computeEclipseTimeMilliseconds(beginTime);
                if (eclipseTime > 10) {
                    int queueSize = this.requestQueue.size();
                    log.warn("create mappedFile spent time(ms) {} queue size {} path {} size {}.", eclipseTime, queueSize, req.getFilePath(), req.getFileSize());
                }

                // pre write mappedFile
                if (mappedFile.getFileSize() >= this.storeConfig.getMapedFileSizePhysic() && this.storeConfig.isWarmMapedFileEnable()) {
                    mappedFile.warmMappedFile(this.storeConfig.getFlushDiskType(), this.storeConfig.getFlushLeastPagesWhenWarmMapedFile());
                }

                req.setMapedFile(mappedFile);
                this.hasException = false;
                isSuccess = true;
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");
            this.hasException = true;
            return false;
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
            if (null != req) {
                requestQueue.offer(req);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        } finally {
            if (req != null && isSuccess)
                req.getCountDownLatch().countDown();
        }
        return true;
    }

    public void removeFile(String path) {
        AllocateRequest allocateRequest = requestTable.get(path);
        if (allocateRequest != null) {
            try {
                allocateRequest.countDownLatch.await();
            } catch (InterruptedException e) {
            }
            log.info("delete next allocated maped file, {}", path);
            allocateRequest.mapedFile.destroy(1000);
            requestTable.remove(path);
        }
    }

    static class AllocateRequest implements Comparable<AllocateRequest> {
        // Full file path
        private String filePath;
        private int fileSize;
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        private volatile MapedFile mapedFile = null;

        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public int getFileSize() {
            return fileSize;
        }

        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public MapedFile getMapedFile() {
            return mapedFile;
        }

        public void setMapedFile(MapedFile mapedFile) {
            this.mapedFile = mapedFile;
        }

        @Override
        public int compareTo(AllocateRequest other) {
            return this.fileSize < other.fileSize ? 1 : this.fileSize > other.fileSize ? -1 : 0;
        }
    }
}
