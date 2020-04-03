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
package com.wuba.wpaxos.sample.election;

import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.sample.echo.EchoClient;
import com.wuba.wpaxos.sample.util.NodeUtil;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ElectionMain {
	
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.out.println("arguments num is wrong , they are[rootPath,myNode,nodeList,groupCount]");
			System.exit(1);
		}
		
		String rootPath = args[0];
		String log4jConfig = rootPath + File.separator + "conf" + File.separator + "log4j.properties";
		ConfigurationSource src = new ConfigurationSource(new FileInputStream(log4jConfig));
		Configurator.initialize(EchoClient.class.getClassLoader(), src);
		NodeInfo myNode = NodeUtil.parseIpPort(args[1]);
		List<NodeInfo> nodeInfoList = NodeUtil.parseIpPortList(args[2]);
		int groupCount = Integer.parseInt(args[3]);
		
		ElectionServer electionServer = new ElectionServer(myNode, nodeInfoList, groupCount, rootPath);
		
		electionServer.runPaxos();
		System.out.println("election server start, ip [" + myNode.getIp() + "] port [" + myNode.getPort() + "]");
		
		while(true) {
			Set<Integer> masterSet = new HashSet<Integer>();
			for (int i = 0; i < groupCount; i++) {
				if (electionServer.isMaster(i)) {
					masterSet.add(i);
				}
			}
			
			System.out.println("I'm master groups of " + masterSet);
			
			Thread.sleep(10*1000);
		}
	}
}


















