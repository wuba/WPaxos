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
package com.wuba.wpaxos.sample.kv;

import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.sample.util.NodeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;

public class KVClientLeaner {
	private static Logger logger;

	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.out.println("arguments num is wrong , they are[rootPath,myNode,nodeList,groupCount,indexType]");
			System.exit(1);
		}
		ClientRole.setLeaner();
		String rootPath = args[0];
		String log4jConfig = rootPath + File.separator + "conf" + File.separator + "log4j.properties";
		ConfigurationSource src = new ConfigurationSource(new FileInputStream(log4jConfig));
		Configurator.initialize(KVClientLeaner.class.getClassLoader(), src);
		logger = LogManager.getLogger(KVClientLeaner.class);
		NodeInfo myNode = NodeUtil.parseIpPort(args[1]);
		List<NodeInfo> nodeInfoList = NodeUtil.parseIpPortList(args[2]);
		final int groupCount = Integer.parseInt(args[3]);
		final int indexType = Integer.parseInt(args[4]);
		final KVTestServer fileServer = new KVTestServer(myNode, nodeInfoList, groupCount, rootPath, indexType);
		fileServer.runPaxos();
		logger.info("paxos leaner run success!");
	}

}
