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
package com.wuba.wpaxos.communicate.client.udp;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;

/**
 * udp client for send msg
 */
public class UDPClient {
	
	private String encode;
	
	private DatagramSocket sock = null;
	
	private InetSocketAddress addr = null;
	
	public static UDPClient getInstrance(String ip, int port, String encode) throws SocketException {
		UDPClient client = new UDPClient();
		client.encode = encode;
		client.sock = new DatagramSocket();
		client.addr = new InetSocketAddress(ip, port);
		
		return client;
	}
	
	private UDPClient() {
	}
	
	public void close() {
		sock.close();
	}

	/**
	 * send udp msg
	 * @param msg
	 */
	public void send(String msg, String encoding) throws Exception {
		byte[] buf = msg.getBytes(encoding);
		send(buf);
	}
	
	public void send(String msg, byte[] delimiter) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(msg.getBytes(encode).length + delimiter.length);
		buffer.put(msg.getBytes(encode));
		buffer.put(delimiter);
		buffer.flip();
		send(buffer.array());
		buffer.clear();
	}
	
	public void send(byte[] msg, byte[] delimiter) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(msg.length + delimiter.length);
		buffer.put(msg);
		buffer.put(delimiter);
		buffer.flip();
		send(buffer.array());
		buffer.clear();
	}
	
	public void send(String msg) throws IOException {
		byte[] buf = msg.getBytes(encode);
		send(buf);
	}
	
	public void send(byte[] buf) throws IOException {
		DatagramPacket dp = new DatagramPacket(buf, buf.length, addr);
		sock.send(dp);
	}
}