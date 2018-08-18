package com.beans.ko.etl.mapreduce.hive;

import static com.beans.ko.etl.mapreduce.utils.ConfigUtils.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.ClassUtil;

import com.beans.ko.etl.mapreduce.model.TableConfig;
import com.beans.ko.etl.mapreduce.utils.HiveUtils;

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

		// 这个配置应该是本地提交才会用到，建议放到osName=windows
		config.set(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM, "true");

		// 本地提交的设置，这里有疑问是否和配置conf.set("mapreduce.job.jar",
		// "target/mapreduce-hfile-0.0.1-SNAPSHOT.jar");
		if (osName.toLowerCase().startsWith("windows")) {
			boolean runWithJar = ClassUtil.findContainingJar(HBaseToHive.class) != null ? true
					: false;
			if (!runWithJar) {
				config.set(MRJobConfig.JAR,
						new File(getMapReduceJarPath()).getAbsolutePath());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		String partition = format.format(new Date());
		String partitionColumn = getHiveTablePartitionColumn();
		String partitionDef = String
				.format("%s=%s", partitionColumn, partition);

		String defaultFs = config
				.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
		defaultFs = defaultFs.endsWith("/") ? defaultFs.substring(0,
				defaultFs.length() - 1) : defaultFs;
		String tablePathFormat = defaultFs + getHiveTableOutputPathRemote()
				+ "/%s/";
		String partitionPathFormat = tablePathFormat + partitionDef;
		List<Path> libPathOnHdfs = uploadDependencies(
				getMapReduceLibPathLocal(), getMapReduceLibPathRemote());

		List<TableConfig> tables = getTables();
		for (TableConfig table : tables) {
			String tableName = table.getHiveTableName();
			String tablePath = String.format(tablePathFormat, tableName);
			// String partitionPath = String.format(partitionPathFormat,
			// tableName);
			createHiveTable(table, tablePath, partitionColumn);
		}

	}

	private static List<Path> uploadDependencies(String libPath,
			String libPahtOnHdfs) throws IOException {
		FileSystem fs = FileSystem.get(config);
		Path output = new Path(libPahtOnHdfs);
		if (!fs.exists(output)) {
			fs.mkdirs(output);
		}
		FileStatus[] cached = fs.listStatus(new Path(libPahtOnHdfs));
		Map<String, FileStatus> nameToHdfs = Stream.of(cached).collect(
				Collectors.toMap(status -> status.getPath().getName(),
						status -> status));

		File[] jars = new File(libPath).listFiles();
		Map<String, File> nameToFile = Stream.of(jars).collect(Collectors.toMap(file -> file.getName(), file -> file));

		Set<String> delete = new HashSet<String>(nameToHdfs.keySet());
		Set<String> add = new HashSet<String>(nameToFile.keySet());

		delete.removeAll(nameToFile.keySet());
		add.removeAll(nameToHdfs.keySet());
		for (String name : delete) {
			fs.delete(nameToHdfs.get(name).getPath(), true);
		}
		for (String name : add) {
			FSDataOutputStream out = fs.create(new Path(libPahtOnHdfs + "/" + name), true);
			FileInputStream fis = new FileInputStream(nameToFile.get(name));
			IOUtils.copyBytes(fis, out, 4096,true);
		}
		
		List<Path> paths = Stream.of(fs.listStatus(new Path(libPahtOnHdfs))).map(FileStatus::getPath).collect(Collectors.toList());
		return paths;
	}

	private static void createHiveTable(TableConfig table, String tablePath,
			String partitionColumn) {
		String createSql = getHiveCreateTalSql(table, tablePath,
				partitionColumn);
		HiveUtils.executeQuery(createSql);
		System.out.println(createSql);
	}
}
