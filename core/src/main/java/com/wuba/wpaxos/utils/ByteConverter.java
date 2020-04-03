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
package com.wuba.wpaxos.utils;

public class ByteConverter {

	/**
	 * byte array to short (little endian)
	 * 
	 * @param buf
	 * @return
	 */
	public static short bytesToShortLittleEndian(byte[] buf) {
		return (short) (buf[0] & 0xff | ((buf[1] << 8) & 0xff00));
	}

	/**
	 * byte array to int (little endian)
	 * 
	 * @param buf
	 * @return
	 */
	public static int bytesToIntLittleEndian(byte[] buf) {
		return buf[0] & 0xff | ((buf[1] << 8) & 0xff00)
				| ((buf[2] << 16) & 0xff0000) | ((buf[3] << 24) & 0xff000000);
	}

	/**
	 * byte array to int (big endian)
	 * 
	 * @param buf
	 * @return
	 */
	public static long bytesToLongLittleEndian(byte[] buf) {
		return (long)buf[0] & 0xffL
				| (((long)buf[1] << 8)  & 0xff00L)
				| (((long)buf[2] << 16) & 0xff0000L)
				| (((long)buf[3] << 24) & 0xff000000L)
				| (((long)buf[4] << 32) & 0xff00000000L)
				| (((long)buf[5] << 40) & 0xff0000000000L)
				| (((long)buf[6] << 48) & 0xff000000000000L)
				| (((long)buf[7] << 56) & 0xff00000000000000L);
	}

	/**
	 * short to byte array (little endian)
	 * 
	 * @param n
	 * @return
	 */
	public static byte[] shortToBytesLittleEndian(short n) {
		byte[] buf = new byte[2];
		for(int i=0; i<buf.length; i++) {
			buf[i] = (byte) (n >> (8 * i));
		}
		return buf;
	}

	/**
	 * int to byte array (little endian)
	 * 
	 * @param n
	 * @return
	 */
	public static byte[] intToBytesLittleEndian(int n) {
		byte[] buf = new byte[4];
		for(int i=0; i<buf.length; i++) {
			buf[i] = (byte) (n >> (8 * i));
		}
		return buf;
	}

	/**
	 * long to byte array (little endian)
	 * 
	 * @param n
	 * @return
	 */
	public static byte[] longToBytesLittleEndian(long n) {
		byte[] buf = new byte[8];
		for(int i=0; i<buf.length; i++) {
			buf[i] = (byte) (n >> (8 * i));
		}
		return buf;
	}
	
	/**
	 * byte array to short (little endian)
	 * 
	 * @param buf
	 * @param offset
	 * @return
	 */
	public static short bytesToShortLittleEndian(byte[] buf, int offset) {
		return (short) (buf[offset] & 0xff | ((buf[offset + 1] << 8) & 0xff00));
	}

	/**
	 * byte array to int (little endian)
	 * 
	 * @param buf
	 * @param offset
	 * @return
	 */
	public static int bytesToIntLittleEndian(byte[] buf, int offset) {
		return buf[offset] & 0xff 
				| ((buf[offset + 1] << 8)  & 0xff00)
				| ((buf[offset + 2] << 16) & 0xff0000)
				| ((buf[offset + 3] << 24) & 0xff000000);
	}

	/**
	 * byte array to long (little endian)
	 * 
	 * @param buf
	 * @param offset
	 * @return
	 */
	public static long bytesToLongLittleEndian(byte[] buf, int offset) {
		return (long)buf[offset] & 0xffL
				| (((long)buf[offset + 1] << 8) & 0xff00L)
				| (((long)buf[offset + 2] << 16) & 0xff0000L)
				| (((long)buf[offset + 3] << 24) & 0xff000000L)
				| (((long)buf[offset + 4] << 32) & 0xff00000000L)
				| (((long)buf[offset + 5] << 40) & 0xff0000000000L)
				| (((long)buf[offset + 6] << 48) & 0xff000000000000L)
				| (((long)buf[offset + 7] << 56) & 0xff00000000000000L);
	}
	
	/**
	 * IP转换为INT
	 * @param ip
	 * @return
	 * @throws Exception
	 */
	public static int ipToInt(String ip) throws Exception {
		String[] ipAry = ip.split("\\.");
		if(ipAry.length != 4) {
			throw new Exception("ipToInt error ip:" + ip);
		}
		byte[] ipBuf = new byte[4];
		for(int i=0; i<4; i++) {
			int item = Integer.parseInt(ipAry[i]);
			if(item > 127) {
				item -= 256;
			}
			ipBuf[i] = (byte)item;
		}
		
		int s =0; 
        int s0 = ipBuf[0]&0xff;// 最低位 
        int s1 = ipBuf[1]&0xff; 
        int s2 = ipBuf[2]&0xff; 
        int s3 = ipBuf[3]&0xff; 
        s3 <<=24; 
        s2 <<=16; 
        s1 <<=8; 
        s = s0 | s1 | s2 | s3; 
        return s;
	}
	
	/**
	 * IP转换为byte[]
	 * @param ip
	 * @return
	 * @throws Exception
	 */
	public static byte[] ipToTyte(String ip) throws Exception {
		String[] ipAry = ip.split("\\.");
		if(ipAry.length != 4) {
			throw new Exception("ip2int error ip:" + ip);
		}
		byte[] ipBuf = new byte[4];
		for(int i=0; i<4; i++) {
			int item = Integer.parseInt(ipAry[i]);
			if(item > 127) {
				item -= 256;
			}
			ipBuf[i] = (byte)item;
		}
		return ipBuf;
	}
	
	/**
	 * int转ip
	 * @param ipBuf
	 * @return
	 */
	public static String byteToIp(byte[] ipBuf) {
		int[] ipBufInt = new int[4];
		for(int i=0; i<4; i++) {
			if(ipBuf[i] < 0) {
				ipBufInt[i] = ipBuf[i] + 256;
			} else {
				ipBufInt[i] = ipBuf[i];
			}
		}
		StringBuilder sbIP = new StringBuilder();
		sbIP.append(ipBufInt[0]);
		sbIP.append(".");
		sbIP.append(ipBufInt[1]);
		sbIP.append(".");
		sbIP.append(ipBufInt[2]);
		sbIP.append(".");
		sbIP.append(ipBufInt[3]);
		
		return sbIP.toString();
	}
	
    /** 
     * 注释：字节数组到int的转换！ 
     * 
     * @param b 
     * @return 
     */ 
    public static int byteToInt(byte[] b){ 
        int s =0; 
        int s0 = b[0]&0xff;// 最低位 
        int s1 = b[1]&0xff; 
        int s2 = b[2]&0xff; 
        int s3 = b[3]&0xff; 
        s3 <<=24; 
        s2 <<=16; 
        s1 <<=8; 
        s = s0 | s1 | s2 | s3; 
        return s; 
    } 
    
    /** 
     * 注释：int到字节数组的转换！ 
     * 
     * @param number 
     * @return 
     */ 
    public static byte[] intToByte(int number){ 
        int temp = number; 
        byte[] b =new byte[4]; 
        for(int i =0; i < b.length; i++){ 
            b[i]= Integer.valueOf(temp &0xff).byteValue();// 将最低位保存在最低位
            temp = temp >>8;// 向右移8位 
        } 
        return b; 
    } 
    
    public static String getIpStr(int ipInt) {
    	String ip = byteToIp(intToByte(ipInt));

    	return ip;
    }
    
}