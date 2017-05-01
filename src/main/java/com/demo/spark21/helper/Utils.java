package com.demo.spark21.helper;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class Utils {

	public static void main(String[] args) throws IOException {
		File file = new File("/home/jrp/workspace_1/Spark21-Example/output/wordcount_output");
		FileUtils.deleteDirectory(file);
		
		if(file.exists()) {
			System.out.println("File still exists");
		}
		
	}

}
