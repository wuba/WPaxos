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

import com.wuba.wpaxos.communicate.NetWork;

/**
 * server config for receive msg
 */
public class ServerConfig {
	private NetWork network = null;
	
	/*TCP*/
	private int recvBufferSizeTcp = 5242880;
	private int sendBufferSizeTcp = 5242880;
	private int maxPakageSizeTcp = 10485760;

	private int writeBufferHighWaterMark = 7340032;
	private int writeBufferLowWaterMark = 3145728;
	private int workerCountTcp = 8; 
	private String sListenIp;
	private int listenPort;
	
	/*UDP*/
	private int recvBufferSizeUdp = 1024 * 1024 * 32;
	private int sendBufferSizeUdp = 1024 * 1024 * 32;
	private int maxPakageSizeUdp = 1024 * 1024 * 32;
	private int workerCountUdp = 8;
	
	private static ServerConfig instance = new ServerConfig();
	
	private ServerConfig() {}
	
	public static ServerConfig getInstance() {
		return instance;
	}
	
	public NetWork getNetwork() {
		return network;
	}

	public void setNetwork(NetWork network) {
		this.network = network;
	}

	public String getsListenIp() {
		return sListenIp;
	}

	public void setsListenIp(String sListenIp) {
		this.sListenIp = sListenIp;
	}

	public int getListenPort() {
		return listenPort;
	}

	public void setListenPort(int listenPort) {
		this.listenPort = listenPort;
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

	public int getWorkerCountTcp() {
		return workerCountTcp;
	}

	public void setWorkerCountTcp(int workerCountTcp) {
		if (workerCountTcp > 1) {
			this.workerCountTcp = workerCountTcp;
		}
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

	public int getWorkerCountUdp() {
		return workerCountUdp;
	}

	public void setWorkerCountUdp(int workerCountUdp) {
		this.workerCountUdp = workerCountUdp;
	}
}
