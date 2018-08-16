package com.beans.ko.etl.mapreduce.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MapReduceUtils {

	public static void addTmpJar(String jarPath, Configuration conf)
			throws IOException {
		System.setProperty("path.separator", ":");
		FileSystem fs = FileSystem.getLocal(conf);
		String newJarPath = new Path(jarPath).makeQualified(fs).toString();
		String tmpJars = conf.get("tmpjars");
		if (tmpJars == null || tmpJars.length() == 0) {
			conf.set("tmpjars", newJarPath);
		} else {
			conf.set("tmpjars", tmpJars + "," + newJarPath);
		}
	}
}
