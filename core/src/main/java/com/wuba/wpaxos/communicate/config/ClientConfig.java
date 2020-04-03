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
package com.wuba.wpaxos.communicate.config;

/**
 * client config for send msg
 */
public class ClientConfig {
	
	/*TCP*/
	private int recvBufferSizeTcp = 1024 * 1024 * 2;
	private int sendBufferSizeTcp = 1024 * 1024 * 2;
	private int maxPakageSizeTcp = 1024 * 1024 * 2;
	private int writeBufferHighWaterMark = 7340032;
	private int writeBufferLowWaterMark = 3145728;
	private int workerCountTcp = 1;
	private int connectTimeoutTcp = 3000;
	
	/*UDP*/
	private int recvBufferSizeUdp = 1024 * 1024 * 2;
	private int sendBufferSizeUdp = 1024 * 1024 * 2;
	private int maxPakageSizeUdp = 1024 * 1024 * 2;
	private int workerCountUdp = 2;
	
	private static ClientConfig instance = new ClientConfig();
	
	private ClientConfig() {}
	
	public static ClientConfig getInstance() {
		return instance;
	}
	
	public int getRecvBufferSizeTcp() {
		return recvBufferSizeTcp;
	}
	
	public void setRecvBufferSizeTcp(int recvBufferSizeTcp) {
		this.recvBufferSizeTcp = recvBufferSizeTcp;
	}
	
	public int getSendBufferSizeTcp() {
		return sendBufferSizeTcp;
	}
	
	public void setSendBufferSizeTcp(int sendBufferSizeTcp) {
		this.sendBufferSizeTcp = sendBufferSizeTcp;
	}
	
	public int getMaxPakageSizeTcp() {
		return maxPakageSizeTcp;
	}
	
	public void setMaxPakageSizeTcp(int maxPakageSizeTcp) {
		this.maxPakageSizeTcp = maxPakageSizeTcp;
	}
	
	public int getWriteBufferHighWaterMark() {
		return writeBufferHighWaterMark;
	}
	
	public void setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
		this.writeBufferHighWaterMark = writeBufferHighWaterMark;
	}
	
	public int getWriteBufferLowWaterMark() {
		return writeBufferLowWaterMark;
	}
	
	public void setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
		this.writeBufferLowWaterMark = writeBufferLowWaterMark;
	}
	
	public int getConnectTimeout() {
		return connectTimeoutTcp;
	}

	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeoutTcp = connectTimeout;
	}

	public int getWorkerCountTcp() {
		return workerCountTcp;
	}
	
	public void setWorkerCountTcp(int workerCountTcp) {
		this.workerCountTcp = workerCountTcp;
	}
	
	public int getRecvBufferSizeUdp() {
		return recvBufferSizeUdp;
	}
	
	public void setRecvBufferSizeUdp(int recvBufferSizeUdp) {
		this.recvBufferSizeUdp = recvBufferSizeUdp;
	}
	
	public int getSendBufferSizeUdp() {
		return sendBufferSizeUdp;
	}
	
	public void setSendBufferSizeUdp(int sendBufferSizeUdp) {
		this.sendBufferSizeUdp = sendBufferSizeUdp;
	}
	
	public int getMaxPakageSizeUdp() {
		return maxPakageSizeUdp;
	}
	
	public void setMaxPakageSizeUdp(int maxPakageSizeUdp) {
		this.maxPakageSizeUdp = maxPakageSizeUdp;
	}
	
	public int getWorkerCountUdp() {
		return workerCountUdp;
	}
	
	public void setWorkerCountUdp(int workerCountUdp) {
		this.workerCountUdp = workerCountUdp;
	}
}
