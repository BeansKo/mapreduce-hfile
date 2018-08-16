package com.beans.ko.etl.mapreduce.hive;

import static com.beans.ko.etl.mapreduce.utils.ConfigUtils.*;

import java.io.File;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.ClassUtil;

import com.beans.ko.etl.mapreduce.utils.ConfigUtils;

public class HBaseToHive {
	private static Configuration config;
	static {
		/**
		 * 此代码为了防止 windows下 不设置 hadoop.home.dir 会报错 但设不设值都不影响程序执行
		 */
		System.setProperty("HADOOP_USER_NAME", getHadoopUserName());
		String osName = System.getProperty("os.name");
		if (osName.toLowerCase().startsWith("windows")) {
			String hadoop = HBaseToHive.class.getResource("/hadoop").getPath();
			if (null == hadoop || hadoop.indexOf("!") != -1) {
				String userDir = System.getProperty("user.dir");
				hadoop = userDir + File.separator + "hadoop";
			}
			try {
				hadoop = java.net.URLDecoder.decode(hadoop, "utf-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			System.setProperty("hadoop.home.dir", hadoop);
		}
		
		config = new Configuration();
		config.addResource("yarn-site.xml");
		config.addResource("hbase-site.xml");
		config.set(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM, "true");
		
		if(osName.toLowerCase().startsWith("windows")){
			boolean runWithJar = ClassUtil.findContainingJar(HBaseToHive.class) != null?true:false;
			if(!runWithJar){
				config.set(MRJobConfig.JAR, new File(getMapReduceJarPath()).getAbsolutePath());
			}
		}
	}

	public static void main(String[] args) {
		System.out.println("aa");
	}
}
