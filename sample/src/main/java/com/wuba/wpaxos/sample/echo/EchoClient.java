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
package com.wuba.wpaxos.sample.echo;

import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.sample.util.NodeUtil;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;

public class EchoClient {
	
	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.out.println("arguments num is wrong , they are[rootPath,myNode,nodeList,groupCount,indexType]");
			System.exit(1);
		}
		
		String rootPath = args[0];
		String log4jConfig = rootPath + File.separator + "conf" + File.separator + "log4j.properties";
		ConfigurationSource src = new ConfigurationSource(new FileInputStream(log4jConfig));
		Configurator.initialize(EchoClient.class.getClassLoader(), src);
		NodeInfo myNode = NodeUtil.parseIpPort(args[1]);
		List<NodeInfo> nodeInfoList = NodeUtil.parseIpPortList(args[2]);
		int groupCount = Integer.parseInt(args[3]);
		int indexType = Integer.parseInt(args[4]);
		
		EchoServer echoServer = new EchoServer(myNode, nodeInfoList, groupCount, rootPath, indexType);
		
		echoServer.runPaxos();
		System.out.println("echo server start, ip [" + myNode.getIp() + "] port [" + myNode.getPort() + "]");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		int idx = 0;
		while(true) {
			System.out.println("please input : ");
			String echoReqValue = br.readLine();
			
			if("quit".equals(echoReqValue)) break;
			
			try {
				if("1".equals(echoReqValue)) {
					for(int i = 0; i < 50; i++) {
						String echoRespValue = echoServer.echo("wpaxos_test" + idx, 0);				
						System.out.println("echo response : " + echoRespValue);
						idx++;
						Thread.sleep(1000);
					}
				} else {
					echoServer.addMember(NodeUtil.parseIpPort(new String(echoReqValue)));
					System.out.println(echoServer.getAllMembers());
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		br.close();
	}
}
