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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileUtils {
	
	public static boolean isDir(String path){
		File file = new File(path);
		return file.isDirectory();
	}

	public static void deleteDir(String dirPath) {
		File dir = new File(dirPath);
		if(!dir.isDirectory()) return ;
		
		File[] files = dir.listFiles();
		if(files != null && files.length > 0) {
			for(File file : files) {
				
				if(".".equals(file.getName()) || "..".equals(file.getName())) {
					continue ;
				}
				
				if(file.isDirectory()) {
					deleteDir(file.getAbsolutePath());
				} else {
					file.delete();
				}
			}
		}
		
		dir.delete();
	}

	public static List<String> iterDir(String dirPath) {
		List<String> filePathList = new ArrayList<String>();
		File dir = new File(dirPath);
		if(!dir.isDirectory()) {
			return filePathList;
		}
		
		iterDir(dir, filePathList);
		return filePathList;
	}
	
	private static void iterDir(File dir, List<String> filePathList) {
		File[] files = dir.listFiles();
		if(files != null && files.length > 0) {
			for(File file : files) {
				if(file.isDirectory()) {
					iterDir(file, filePathList);
				} else {
					filePathList.add(file.getName());
				}
			}
		}
	}
	
}














