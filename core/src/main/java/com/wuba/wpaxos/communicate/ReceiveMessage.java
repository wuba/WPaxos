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
package com.wuba.wpaxos.communicate;

import org.jboss.netty.channel.Channel;

/**
 * 接收数据封装
 */
public class ReceiveMessage {
	private byte[] receiveBuf;
	private int receiveLen;
	private boolean notifyMsg = false;
	private Channel channel;
	private long timeStamp;
	
	public ReceiveMessage(byte[] receiveBuf, int receiveLen) {
		super();
		this.receiveBuf = receiveBuf;
		this.receiveLen = receiveLen;
	}
	
	public ReceiveMessage(byte[] receiveBuf, int receiveLen, boolean notifyMsg) {
		super();
		this.receiveBuf = receiveBuf;
		this.receiveLen = receiveLen;
		this.notifyMsg = notifyMsg;
	}
	
	public static ReceiveMessage getNotifyNullMsg() {
		ReceiveMessage receiveMsg = new ReceiveMessage(null, 0, true);
		return receiveMsg;
	}
	
	public boolean isNotifyMsg() {
		return notifyMsg;
	}

	public void setNotifyMsg(boolean notifyMsg) {
		this.notifyMsg = notifyMsg;
	}

	public byte[] getReceiveBuf() {
		return receiveBuf;
	}

	public void setReceiveBuf(byte[] receiveBuf) {
		this.receiveBuf = receiveBuf;
	}

	public int getReceiveLen() {
		return receiveLen;
	}

	public void setReceiveLen(int receiveLen) {
		this.receiveLen = receiveLen;
	}

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
}
