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
package com.wuba.wpaxos.sample.simple;

import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.sample.util.NodeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleClient {

	private static Logger logger;

	public static void main(String[] args) throws Exception {
		if (args.length != 11) {
			System.out.println("arguments num is wrong ," +
					"they are[rootPath,myNode,nodeList,groupCount,userBatch,thdNum," +
					"sendCount,sleepMills,batchCount,indexType,sendSize]");
			System.exit(1);
		}
		String rootPath = args[0];
		String log4jConfig = rootPath + File.separator + "conf" + File.separator + "log4j.properties";
		ConfigurationSource src = new ConfigurationSource(new FileInputStream(log4jConfig));
		Configurator.initialize(SimpleClient.class.getClassLoader(), src);
		logger = LogManager.getLogger(SimpleClient.class);
		NodeInfo myNode = NodeUtil.parseIpPort(args[1]);
		List<NodeInfo> nodeInfoList = NodeUtil.parseIpPortList(args[2]);
		final int groupCount = Integer.parseInt(args[3]);
		boolean useBatchPropose = Boolean.parseBoolean(args[4]);
		int thdNum = Integer.parseInt(args[5]);
		final int sendCount = Integer.parseInt(args[6]);
		final int sleepMills = Integer.parseInt(args[7]);
		final int nodeCount = nodeInfoList.size();
		int nodeIdx = 0;
		for (int i = 0; i < nodeInfoList.size(); i++) {
			if (nodeInfoList.get(i).getNodeID() == myNode.getNodeID()) {
				nodeIdx = i;
			}
		}
		final int batchCount = Integer.parseInt(args[8]);
		final int indexType = Integer.parseInt(args[9]);
		final int sendSize = Integer.parseInt(args[10]);
		final GroupRand groupRand = getGroupRand(groupCount, nodeCount, nodeIdx);
		final SimpleServer simpleServer = new SimpleServer(myNode, nodeInfoList, groupCount, useBatchPropose, batchCount, rootPath, indexType);
		simpleServer.runPaxos();

		final Random random = new Random();
		final AtomicLong randCount = new AtomicLong();
		while (true) {
			CountDownLatch countDownLatch = new CountDownLatch(thdNum);
			long startStampAll = System.currentTimeMillis();

			final byte[] sendBuf = new byte[sendSize];
			for (int a = 0; a < sendBuf.length; ++a) {
				sendBuf[a] = (byte) (a % 128);
			}

			final AtomicLong allSendCount = new AtomicLong(-2763416983704698880L);
			final AtomicLong startStamp = new AtomicLong(System.currentTimeMillis());
			for (int t = 0; t < thdNum; ++t) {
				Thread th = new Thread(new Runnable() {
					@Override
					public void run() {
						while (true) {
							for (int i = 0; i < sendCount; i++) {
								try {
									int groupIdx = getGroupIdxRand(groupRand);
									simpleServer.propose(sendBuf, groupIdx);
									if (allSendCount.incrementAndGet() % 10000 == 0) {
										long endStamp = System.currentTimeMillis();
										logger.info("average qps : {} ", (10000000L / (endStamp - startStamp.get())));
										startStamp.set(System.currentTimeMillis());
									}
								} catch (Exception e) {
									logger.error(e.getMessage(), e);
								}
							}
							try {
								Thread.sleep(random.nextInt(5) + sleepMills);
							} catch (InterruptedException e) {
								logger.error(e.getMessage(), e);
							}
						}
					}

					int getGroupIdxRand(GroupRand groupRand) {
						return (int) (groupRand.getStart() + randCount.incrementAndGet() % groupRand.getRange());
					}
				});

				th.setName("Input_thread_" + t);
				th.setDaemon(true);
				th.start();
			}
			countDownLatch.await();
			long endStamp = System.currentTimeMillis();
			logger.info("average qps : {}", (sendCount * thdNum * 1000L / (endStamp - startStampAll)));
		}
	}

	static class GroupRand {
		int start;
		int end;
		int range;

		public GroupRand(int start, int end, int range) {
			this.start = start;
			this.end = end;
			this.range = range;
		}

		public int getStart() {
			return this.start;
		}

		public void setStart(int start) {
			this.start = start;
		}

		public int getEnd() {
			return this.end;
		}

		public void setEnd(int end) {
			this.end = end;
		}

		public int getRange() {
			return this.range;
		}

		public void setRange(int range) {
			this.range = range;
		}

		public String toString() {
			return "GroupRand [start=" + this.start + ", end=" + this.end
					+ ", range=" + this.range + "]";
		}
	}

	public static GroupRand getGroupRand(int groupCount, int nodeCount, int nodeIdx) {
		int index = nodeIdx;
		int mod = groupCount % nodeCount;
		int averageSize = ((mod > 0) && (index < mod)) ? groupCount / nodeCount + 1 : (groupCount <= nodeCount) ? 1 : groupCount / nodeCount;
		int startIndex = ((mod > 0) && (index < mod)) ? index * averageSize : index * averageSize + mod;
		int range = Math.min(averageSize, groupCount - startIndex);
		int endIndex = (startIndex + range - 1) % groupCount + 1;
		GroupRand groupRand = new GroupRand(startIndex, endIndex, range);
		return groupRand;
	}

}
