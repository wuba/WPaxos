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
package com.wuba.wpaxos.store.pagecache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import sun.nio.ch.DirectBuffer;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.wuba.wpaxos.config.WriteState;
import com.wuba.wpaxos.store.AppendDataCallback;
import com.wuba.wpaxos.store.AppendDataResult;
import com.wuba.wpaxos.store.AppendDataStatus;
import com.wuba.wpaxos.store.FileID;
import com.wuba.wpaxos.store.GetResult;
import com.wuba.wpaxos.store.TransientStorePool;
import com.wuba.wpaxos.store.config.FlushDiskType;
import com.wuba.wpaxos.store.config.StoreConfig;
import com.wuba.wpaxos.utils.LibC;
import com.wuba.wpaxos.utils.UtilAll;

/**
 * Pagecache文件访问封装
 * 
 */
@SuppressWarnings("restriction")
public class MapedFile extends ReferenceResource {

    public static final int OS_PAGE_SIZE = 1024 * 4;
    private static final Logger log = LogManager.getLogger(MapedFile.class);
    
    // 当前JVM中映射的虚拟内存总大小
    private static final AtomicLong TotalMapedVitualMemory = new AtomicLong(0);
    // 当前JVM中mmap句柄数量
    private static final AtomicInteger TotalMapedFiles = new AtomicInteger(0);
    // 映射的文件名
    private String fileName;
    // 映射的起始偏移量
    private long fileFromOffset;
    // 映射的文件大小，定长
    private int fileSize;
    // 映射的文件
    private File file;
    private RandomAccessFile randomAccessFile;
    // 映射的内存对象，position永远不变
    private MappedByteBuffer mappedByteBuffer;
    // 当前写到什么位置
    private final AtomicInteger wrotePostion = new AtomicInteger(0);
    // Flush到什么位置
    private final AtomicInteger committedPosition = new AtomicInteger(0);
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    // 映射的FileChannel对象
    private FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    protected ByteBuffer writeBuffer = null;
    protected TransientStorePool transientStorePool = null;
    // 最后一条消息存储时间
    private volatile long storeTimestamp = 0;
    private boolean firstCreateInQueue = false;
    private AtomicBoolean isMapped = new AtomicBoolean(false);
    private ReentrantLock mapLock = new ReentrantLock();
    private ReentrantLock commitLock = new ReentrantLock();
    private AtomicLong lastTouchTime = new AtomicLong();
    
    public MapedFile() {
    }

    public MapedFile(final String fileName, final int fileSize, final boolean isMapped) throws IOException {
        init(fileName, fileSize, isMapped);
    }
    
    public MapedFile(final String fileName, final int fileSize,
            final TransientStorePool transientStorePool, final boolean isMapped) throws IOException {
            init(fileName, fileSize, transientStorePool, isMapped);
        }
    
    public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool, final boolean isMapped) throws IOException {
    	init(fileName, fileSize, isMapped);
        this.writeBuffer = transientStorePool.borrowBuffer(fileSize);
        this.transientStorePool = transientStorePool;
    }

    public void init(final String fileName, final int fileSize, final boolean isMapped) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        this.lastTouchTime.set(System.currentTimeMillis());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        if (isMapped && !this.isMapped.get()) {
        	try {
        		mapLock.lock();
        		if (!this.isMapped.get()) {
        			this.randomAccessFile = new RandomAccessFile(this.file, "rw");
        			this.fileChannel = randomAccessFile.getChannel();
                    this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
                    TotalMapedVitualMemory.addAndGet(fileSize);
                    TotalMapedFiles.incrementAndGet();
                    ok = true;
                    this.isMapped.set(true);  
        		}         
            } catch (FileNotFoundException e) {
                log.error("create file channel " + this.fileName + " Failed. ", e);
                throw e;
            } catch (IOException e) {
                log.error("map file " + this.fileName + " Failed. ", e);
                throw e;
            } finally {
            	mapLock.unlock();
            	
                if (!ok && this.fileChannel != null) {
                    this.fileChannel.close();
                    this.randomAccessFile.close();
                }
            }
        }        
    }   
    
    public MapedFile createMappedFile() throws IOException {    	
    	boolean ok = false;
        ensureDirOK(this.file.getParent());

        if (!this.isMapped.get()) {
        	try {
        		mapLock.lock();
        		if (!this.isMapped.get()) {
        			this.randomAccessFile = new RandomAccessFile(this.file, "rw");
        			this.fileChannel = this.randomAccessFile.getChannel();
                    this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
                    TotalMapedVitualMemory.addAndGet(fileSize);
                    TotalMapedFiles.incrementAndGet();
                    this.lastTouchTime.set(System.currentTimeMillis());
                    ok = true;
                    this.isMapped.set(true);
                    log.info("map file {}.", this.fileName);
        		}          
            } catch (FileNotFoundException e) {
                log.error("create file channel " + this.fileName + " Failed. ", e);
                throw e;
            } catch (IOException e) {
                log.error("map file " + this.fileName + " Failed. ", e);
                throw e;
            } finally {
            	mapLock.unlock();
                if (!ok && this.fileChannel != null) {
                    this.fileChannel.close();
                    this.randomAccessFile.close();
                }
            }
        }
    	
    	return this;
    }   
    
    public void initWritebuffer(TransientStorePool transientStorePool) {
    	log.info("init write buffer : {}.", this.fileName);
    	this.writeBuffer = transientStorePool.borrowBuffer(fileSize);
    	if (writeBuffer != null) {
    		this.writeBuffer.clear();
    		ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
    		byteBuffer.position(0);
    		byteBuffer.limit(this.fileSize);
    		this.writeBuffer.put(byteBuffer);
    		this.writeBuffer.limit(fileSize);
    		this.writeBuffer.flip();
    	}
    }
    
    public void unMapFile() throws IOException {
    	if (this.isMapped.get()) {
    		try {
    			mapLock.lock();
        		if (this.isMapped.get()) {
        			this.isMapped.set(false);
        			this.mappedByteBuffer.force();
        			this.flushedPosition.set(this.wrotePostion.get());
        			
        			clean(this.mappedByteBuffer);            		
            		TotalMapedVitualMemory.addAndGet(this.fileSize * (-1));
                    TotalMapedFiles.decrementAndGet();
                    log.info("unmap file REF: {} name: {} OK", this.refCount.get(), this.fileName);
            		this.fileChannel.close();
            		this.randomAccessFile.close();
        		}      
        	} catch(IOException e) {
        		log.error("unmap file " + this.fileName + " Failed. ", e);
                throw e;
        	} finally {
        		mapLock.unlock();
        	}    	
    	}
    }    
    
    public boolean unTouchCheck() {
    	if (!this.isFull()) {
    		return false;
    	}
    	
    	if (this.flushedPosition.get() != this.fileSize) {
    		return false;
    	}
    	
    	if ((System.currentTimeMillis() - this.lastTouchTime.get()) >= StoreConfig.maxMapedfileUntouchTime) {
    		return true;
    	} 
    	
    	return false;
    }   
    
    public boolean getIsMapped() {
		return isMapped.get();
	}

	public void setIsMapped(boolean isMapped) {
		this.isMapped.set(isMapped);
	}	

	public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0) {
            return;
        }
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }    

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }    

    private static Method method(Object target, String methodName, Class<?>[] args) throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }    

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        // JDK7中将DirectByteBuffer类中的viewedBuffer方法换成了attachment方法
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if ("attachment".equals(methods[i].getName())) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null) {
            return buffer;
        } else {
            return viewed(viewedBuffer);
        }
    }

    public static int getTotalmapedfiles() {
        return TotalMapedFiles.get();
    }

    public static long getTotalMapedVitualMemory() {
        return TotalMapedVitualMemory.get();
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public String getFileName() {
        return fileName;
    }

    /**
     * 获取文件大小
     */
    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }
    
    /**
     * 向MapedBuffer追加消息<br>
     * 
     * @param msg
     *            要追加的消息
     * @param cb
     *            用来对消息进行序列化，尤其对于依赖MapedFile Offset的属性进行动态序列化
     * @return 是否成功，写入多少数据
     * @throws IOException 
     */
    public AppendDataResult appendData(final byte[] data, final AppendDataCallback cb, WriteState writeState) throws IOException {
        assert data != null;
        assert cb != null;

        this.lastTouchTime.set(System.currentTimeMillis());   
        if (!this.isMapped.get()) {
        	createMappedFile();
        }
             
        int currentPos = this.wrotePostion.get();
        // 表示有空余空间
        if (currentPos < this.fileSize) {
        	ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendDataResult result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, data, writeState);
            this.wrotePostion.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        
        // 上层应用应该保证不会走到这里
        log.error("MapedFile.appendData return null, wrotePostion: {} fileSize: {}.", currentPos, this.fileSize);
        return new AppendDataResult(AppendDataStatus.UNKNOWN_ERROR);
    }

    /**
     * 文件起始偏移量
     */
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    /**
     * 向存储层追加数据，一般在SLAVE存储结构中使用
     * 
     * @return 返回写入了多少数据
     * @throws IOException 
     */
    public boolean appendData(final byte[] data) throws IOException {
    	this.lastTouchTime.set(System.currentTimeMillis());
    	if (!this.isMapped.get()) {
        	createMappedFile();
        }
    	
        int currentPos = this.wrotePostion.get();

        // 表示有空余空间
        if ((currentPos + data.length) <= this.fileSize) {
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            byteBuffer.put(data);
            this.wrotePostion.addAndGet(data.length);
            return true;
        }

        return false;
    }
    
    /**
     * 向存储层追加数据，一般在SLAVE存储结构中使用
     * 
     * @return 返回写入了多少数据
     * @throws IOException 
     */
    public boolean appendData(final int pos, final byte[] data) throws IOException {
    	this.lastTouchTime.set(System.currentTimeMillis());
    	if (!this.isMapped.get()) {
        	createMappedFile();
        }
    	
        // 表示有空余空间
        if ((pos + data.length) <= this.fileSize) {        	
            //ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        	ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(pos);
            byteBuffer.put(data);
        	
        	if (pos < this.wrotePostion.get()) {
        		// 回写模式，需要刷新刷盘位置
        		commitLock.lock();
        		try {
        			this.randomAccessFile.seek(pos);
        			this.randomAccessFile.write(data);
        			
        			log.debug("appendMessage BACK :  pos {}.", pos);
        		} catch(Throwable th) {
        			log.error("appendMessage commit sum data error.", th);
        		} finally {
        			commitLock.unlock();
        		}
        	}
            
        	this.wrotePostion.set((pos + data.length));
        	
            return true;
        }

        return false;
    }
    
    /**
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = {}.", this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }
    
    /**
     * 消息刷盘
     * 
     * @param commitLeastPages
     *            至少刷几个page
     * @return
     */
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePostion.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0();
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = {}.", this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }
    
    protected void commit0() {
        int writePos = this.wrotePostion.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) {
        	commitLock.lock();
            try {
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            } finally {
            	commitLock.unlock();
            }
        }
    }

    public int getCommittedPosition() {
        return committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
    	if (!this.isMapped.get()) {
    		return false;
    	}    	
    	
    	int flush = this.flushedPosition.get();
        int write = this.wrotePostion.get();

        // 如果当前文件已经写满，应该立刻刷盘
        if (this.isFull()) {
            return true;
        }

        // 只有未刷盘数据满足指定page数目才刷盘
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }
    
    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePostion.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePostion.get();
    }

    public GetResult selectMapedBuffer(int pos, int size) {
    	this.lastTouchTime.set(System.currentTimeMillis());
    	if (!this.isMapped.get()) {
    		try {
				createMappedFile();
			} catch (IOException e) {
				log.error("createMappedFile failed.", e);
			}
    	}    	
    	
    	int readPosition = getReadPosition();
        // 有消息
        if ((pos + size) <= readPosition && pos >= 0) {
            // 从MapedBuffer读
            if (this.hold()) {
            	ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new GetResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: {}, fileFromOffset: {}.", pos, this.fileFromOffset);
            }
        }
        // 请求参数非法
        else {
            log.warn("selectMapedBuffer request pos invalid, request pos: {}, size: {}, fileFromOffset: {}, wrotePostion: {}, readPosition: {}.", pos, size, this.fileFromOffset, this.wrotePostion, readPosition);
        }
        
        // 非法参数或者mmap资源已经被释放
        return null;
    }
    
    public FileID selectFileID(int pos, int size) {
    	this.lastTouchTime.set(System.currentTimeMillis());
    	if (!this.isMapped.get()) {
    		try {
				createMappedFile();
			} catch (IOException e) {
				log.error("createMappedFile failed.", e);
			}
    	}    	
    	
    	int readPosition = getReadPosition();
        // 有消息
        if ((pos + size) <= readPosition && pos >= 0) {
            // 从MapedBuffer读
            if (this.hold()) {
                //ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            	ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                
                try {
                	long offset = byteBuffer.getLong();
                	int crc32 = byteBuffer.getInt();
                	int fsize = byteBuffer.getInt();
                	FileID fileID = new FileID(offset, crc32, fsize);
                	return fileID;
                } catch (Exception e) {
                	log.error("selectFileID failed, post {}, int size {}.", pos, size, e);
                } finally {
                	this.release();
                }
                
            } else {
                log.warn("matched, but hold failed, request pos: {}, fileFromOffset: {}.", pos, this.fileFromOffset);
            }
        }
        // 请求参数非法
        else {
            log.warn("selectMapedBuffer request pos invalid, request pos: {}, size: {}, fileFromOffset: {}, wrotePostion: {}.", pos, size, this.fileFromOffset, this.wrotePostion);
        }
        
        // 非法参数或者mmap资源已经被释放
        return null;
    }
    
    public GetResult selectMapedBufferIndex(int pos, int size) {
    	this.lastTouchTime.set(System.currentTimeMillis());
    	if (!this.isMapped.get()) {
    		try {
				createMappedFile();
			} catch (IOException e) {
				log.error("createMappedFile failed.", e);
			}
    	}    
    	
    	int readPosition = getReadPosition();
        // 有消息
        if ((pos + size) <= readPosition && pos >= 0) {
            // 从MapedBuffer读
            if (this.hold()) {
            	ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                
                return new GetResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: {}, fileFromOffset: {}.", pos, this.fileFromOffset);
            }
        }
        // 请求参数非法
        else {
            log.warn("selectMapedBuffer request pos invalid, request pos: {}, size: {}, fileFromOffset: {}, wrotePostion: {}.", pos, size, this.fileFromOffset, this.wrotePostion);
        }
        
        // 非法参数或者mmap资源已经被释放
        return null;
    }

    /**
     * 读逻辑分区
     * @throws IOException 
     */
    public GetResult selectMapedBuffer(int pos) {
    	this.lastTouchTime.set(System.currentTimeMillis());
    	if (!this.isMapped.get()) {
    		try {
				createMappedFile();
			} catch (IOException e) {
				log.error("createMappedFile failed.", e);
			}
    	}    	
    	
    	int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                //ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            	ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new GetResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }
        
        // 非法参数或者mmap资源已经被释放
        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        // 如果没有被shutdown，则不可以unmap文件，否则会crash
        if (this.isAvailable()) {
            log.error("this file[REF:{} ] name: {} have not shutdown, stop unmaping.", currentRef, this.fileName);
            return false;
        }

        // 如果已经cleanup，再次操作会引起crash
        if (this.isCleanupOver()) {
            log.error("this file[REF: {} ] name: {} have cleanup, do not do it again.", currentRef, this.fileName);
            // 必须返回true
            return true;
        }

        if (!this.isMapped.get()) {
    		return true;
    	}
        clean(this.mappedByteBuffer);
        TotalMapedVitualMemory.addAndGet(this.fileSize * (-1));
        TotalMapedFiles.decrementAndGet();
        log.info("unmap file[REF:{} ] name: {} OK", currentRef, this.fileName);
        return true;
    }

    /**
     * 清理资源，destroy与调用shutdown的线程必须是同一个
     * 
     * @return 是否被destory成功，上层调用需要对失败情况处理，失败后尝试重试
     */
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
            	if (this.fileChannel != null && this.fileChannel.isOpen()) {
            		this.fileChannel.close();
                    log.info("close file channel {} OK", this.fileName);
            	}                

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePostion() + " M:"
                        + this.getFlushedPosition() + ", "
                        + UtilAll.computeEclipseTimeMilliseconds(beginTime));
                
                return result;
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        }
        else {
            log.warn("destroy maped file[REF:" + this.getRefCount() + "] " + this.fileName + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePostion() {
        return wrotePostion.get();
    }

    public void setWrotePostion(int pos) {
        this.wrotePostion.set(pos);
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    /**
     * 方法不能在运行时调用，不安全。只在启动时，reload已有数据时调用
     */
    public ByteBuffer sliceByteBuffer() {
    	if (this.isMapped.get()) {
    		return this.mappedByteBuffer.slice();
    	} else {
    		return null;
    	}        
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }
    
	public long getLastTouchTime() {
		return lastTouchTime.get();
	}
	
	public void setLastTouchTime(long lastTouchTime) {
		this.lastTouchTime.set(lastTouchTime);
	}
	
    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }
    
    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePostion.get() : this.committedPosition.get();
    	//return this.wrotePostion.get();
    }
	
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MapedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare init finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}", this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(), System.currentTimeMillis() - beginTime);

        this.mlock();
    }
	
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
		final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
		final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }
	
    @Override
    public String toString() {
        return this.fileName;
    }
}
