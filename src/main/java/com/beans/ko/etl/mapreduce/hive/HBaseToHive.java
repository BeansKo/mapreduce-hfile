package com.beans.ko.etl.mapreduce.hive;

import static com.beans.ko.etl.mapreduce.utils.ConfigUtils.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ClassUtil;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

import com.beans.ko.etl.mapreduce.io.HFileInputFormat;
import com.beans.ko.etl.mapreduce.io.KeyValue;
import com.beans.ko.etl.mapreduce.model.TableConfig;
import com.beans.ko.etl.mapreduce.utils.HiveUtils;

public class HBaseToHive {
	private static final String CONFIG_HIVE_TABLE_NAME = "fullexport.hive.table.name";
	private static final String CONFIG_HIVE_TABLE_PARTITION = "fullexport.hive.table.partition";
	private static final String CONFIG_HIVE_TABLE_PARTITION_COLUMN = "fullexport.hive.table.partitionColumn";
	private static final String CONFIG_HBASE_TO_TABLE_COLUMN_MAP = "fullexport.hbase.to.hive.column.map";
	private static final Log log = LogFactory.getLog(HBaseToHive.class);
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
		List<Path> libPathOnHdfs = uploadDependencies(getMapReduceLibPathLocal(), getMapReduceLibPathRemote());

		Map<String, String> customeConfig = new HashMap<String, String>();
		customeConfig.put(CONFIG_HIVE_TABLE_PARTITION, partition);
		customeConfig.put(CONFIG_HIVE_TABLE_PARTITION_COLUMN, partitionColumn);
		
		List<TableConfig> tables = getTables();
		for (TableConfig table : tables) {
			String tableName = table.getHiveTableName();
			String tablePath = String.format(tablePathFormat, tableName);
			 String partitionPath = String.format(partitionPathFormat, tableName);
			createHiveTable(table, tablePath, partitionColumn);
			// 创建HBase快照
			String snapshotName = createSnapshot(table,partition);
			// 获取快照文件路径mapreduce的输入
			List<Path> hfiles = getSnapshotFiles(snapshotName, getColumnFamilies(table));
			
			customeConfig.put(CONFIG_HIVE_TABLE_NAME, tableName);
			customeConfig.put(OrcConf.MAPRED_OUTPUT_SCHEMA.getAttribute(), getOrcSchema(table.getColumns()));
			customeConfig.put(CONFIG_HBASE_TO_TABLE_COLUMN_MAP, getHBaseToHiveColumnMap(table.getColumns()));
			
			Job job = getJob(
					customeConfig,
					"HBaseToHive_Test_" + tableName + "_fl76",
					hfiles,
					new Path(partitionPath),
					libPathOnHdfs);
			job.waitForCompletion(true);
			
			TimeUnit.SECONDS.sleep(60);
			String tableNames = job.getConfiguration().get(CONFIG_HIVE_TABLE_NAME);
			String partitionPaths = String.format(partitionPathFormat, tableName);
			addHivePartition(partitionPaths,tableNames,partitionColumn, partition);
			System.out.println("finish");
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
	
	/**
	 * 添加新的Partition
	 */
	private static void addHivePartition(String partitionPath, String tableName, String partitionColumn, String partition) throws Exception {
		String partitionDef = partitionColumn + "='" + partition;
		String dropPartition = "alter table fl76_" + tableName.toLowerCase() + " drop partition (" + partitionDef + "')";
		String addPartition = "alter table fl76_" + tableName.toLowerCase() + " add partition (" + partitionDef + "')\n"
				+ "location '" + partitionPath + "'";
		HiveUtils.executeQuery(dropPartition);
		HiveUtils.executeQuery(addPartition);
	}

	/**
	 * 创建HBase快照
	 */
	private static String createSnapshot(TableConfig table, String partition) throws IOException {
		String snapshotName = getHBaseSnapshotPrefix() + table.getHbaseTableName().replace(":", "_") + "_" + partition;
		Configuration HBASE_CONFIG = HBaseConfiguration.create();
	    HBASE_CONFIG.setInt("hbase.htable.threads.max", 50);
	    HBASE_CONFIG.setInt("hbase.rpc.timeout", 1800000);
	    HBASE_CONFIG.setInt("hbase.client.scanner.timeout.period", 1800000);
	    Connection connection = ConnectionFactory.createConnection(HBASE_CONFIG);
	    Admin admin = connection.getAdmin();
	    if(admin.listSnapshots(snapshotName).size() > 0) {
	    	admin.deleteSnapshot(snapshotName);
	    }
	    
	    admin.snapshot(snapshotName, TableName.valueOf(table.getHbaseTableName()));
	    admin.close();
	    connection.close();
	    return snapshotName;
	}
	
	/**
	 * 获取快照文件
	 */
	private static List<Path> getSnapshotFiles(String snapshotName, Set<String> columnFamilies) throws IOException {
		Path rootPath = FSUtils.getRootDir(config);
		Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootPath);
		FileSystem snapshotFs = FileSystem.get(snapshotDir.toUri(), config);
		SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(snapshotFs, snapshotDir);

		List<Path> snapshotReferences = new ArrayList<Path>();
		SnapshotReferenceUtil.visitReferencedFiles(config, snapshotFs, snapshotDir, snapshotDesc,
			new SnapshotReferenceUtil.SnapshotVisitor() {
				@Override
				public void logFile(String server, String logfile) throws IOException {
					//snapshotReferences.add(new Path(server, logfile));
				}
				@Override
				public void storeFile(HRegionInfo region, String family, StoreFile hfile) throws IOException {
					String regionName = region.getEncodedName();
					String hfileName = hfile.getName();
					Path path = HFileLink.createPath(region.getTable(), regionName, family, hfileName);
					snapshotReferences.add(path);
				}						
			});

		List<Path> list = new ArrayList<Path>();
		FileSystem hbaseFs = FileSystem.get(rootPath.toUri(), config);
		String[] searchColumnFamilies = columnFamilies.stream().map(s -> {
			return "/" + s + "/";
		}).collect(Collectors.toSet()).toArray(new String[columnFamilies.size()]);
		for (Path path : snapshotReferences) {
			if (StringUtils.countMatches(path.toString(), "=") > 2) {
				HFileLink link = HFileLink.buildFromHFileLinkPattern(config, path);
				HFileLink archiveLink = HFileLink.buildFromHFileLinkPattern(config, link.getArchivePath());
				FileStatus status = archiveLink.getFileStatus(hbaseFs);
				if (status != null) {
					String hfilePath = status.getPath().toString();
					if (StringUtils.indexOfAny(hfilePath, searchColumnFamilies) != -1) {
						list.add(status.getPath());
					}
				}
			} else if (HFileLink.isHFileLink(path) || StoreFileInfo.isReference(path)) {
				HFileLink link = HFileLink.buildFromHFileLinkPattern(config, path);
				FileStatus status = link.getFileStatus(hbaseFs);
				if (status != null) {
					String hfilePath = status.getPath().toString();
					if (StringUtils.indexOfAny(hfilePath, searchColumnFamilies) != -1) {
						list.add(status.getPath());
					}
				}
			}
		}

		return list;
	}

	private static Job getJob(Map<String,String> customeConfig,String jobName,List<Path> input,Path output,List<Path> libPathOnHdfs) throws IOException{
		FileSystem fs = FileSystem.get(config);
		if(fs.exists(output)){fs.delete(output, true);}
		
		Job job = Job.getInstance(config,jobName);
		customeConfig.forEach((k,v) -> {job.getConfiguration().set(k, v);});
		job.setJarByClass(HBaseToHive.class);
		for(Path lib : libPathOnHdfs) {
			job.addFileToClassPath(lib);
		}
		
		job.setMapperClass(ReadHFileMapper.class);
		job.setReducerClass(WriteOrcFileReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(KeyValue.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(OrcStruct.class);

		job.setNumReduceTasks(2);
		job.setInputFormatClass(HFileInputFormat.class);
		FileInputFormat.setInputPaths(job, input.toArray(new Path[input.size()]));

		job.setOutputFormatClass(OrcOutputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		
		return job;
	}
	
	private static class ReadHFileMapper extends Mapper<NullWritable,Cell,Text,KeyValue>{
		private Map<String, String> hbaseToHiveColumnMap;
		private Text textKey = new Text();
		private KeyValue mapValue = new KeyValue();
		
		@Override
		protected void setup(
				Mapper<NullWritable, Cell, Text, KeyValue>.Context context)
				throws IOException, InterruptedException {
			String columnMap = context.getConfiguration().get(CONFIG_HBASE_TO_TABLE_COLUMN_MAP);
			this.hbaseToHiveColumnMap = new HashMap<String,String>();
			for(String field:columnMap.split(",")){
				String[] hbaseToHive = field.split("\\|");
				hbaseToHiveColumnMap.put(hbaseToHive[0], hbaseToHive[1]);
			}
		}
		
		@Override
		protected void map(NullWritable key, Cell value,
				Mapper<NullWritable, Cell, Text, KeyValue>.Context context)
				throws IOException, InterruptedException {
			String columnFamily = Bytes.toString(CellUtil.cloneFamily(value));
			String qualifier = Bytes.toString(CellUtil.cloneQualifier(value));
			String colName = this.hbaseToHiveColumnMap.get(columnFamily + ":" + qualifier);
			if(colName != null) {
				byte[] colValue = CellUtil.cloneValue(value);
				log.debug("mapper kv:"+colName+":" + Bytes.toString(colValue));
				System.out.println("mapper kv:"+colName+":" + Bytes.toString(colValue));
				String rowkey = Bytes.toString(CellUtil.cloneRow(value));
				textKey.set(rowkey);
				mapValue.put(new Text(colName), new BytesWritable(colValue));
				context.write(textKey, mapValue);
			}
		}
	}
	
	private static class WriteOrcFileReducer extends Reducer<Text, KeyValue, NullWritable, OrcStruct> {

		private TypeDescription schema;
		private String partition;
		private String partitionColumn;
		private final NullWritable nullKey = NullWritable.get();
		private OrcStruct outStruct;

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			this.partition = context.getConfiguration().get(CONFIG_HIVE_TABLE_PARTITION);
			this.partitionColumn = context.getConfiguration().get(CONFIG_HIVE_TABLE_PARTITION_COLUMN);
			String schemaStr = context.getConfiguration().get(OrcConf.MAPRED_OUTPUT_SCHEMA.getAttribute());
			schemaStr = schemaStr.substring(0, schemaStr.lastIndexOf(">")) + "," + partitionColumn + ":string>";
			this.schema = TypeDescription.fromString(schemaStr);
			this.outStruct = (OrcStruct) OrcStruct.createValue(schema);
		}

		@Override
		protected void reduce(Text key, Iterable<KeyValue> values, Context context)
				throws IOException, InterruptedException {
			Iterator<KeyValue> it = values.iterator();
			KeyValue map = null;
			while(it.hasNext()) {
				map = it.next();
				String colName = map.getKey().toString();
				if(schema.getFieldNames().contains(colName)) {
					WritableComparable<?> fieldValue = convertValue(schema.findSubtype(colName), map.getValue().copyBytes());
					System.out.println("reduce kv:"+colName+","+fieldValue);
					outStruct.setFieldValue(colName, fieldValue);
				}
			}
			outStruct.setFieldValue(this.partitionColumn, new Text(this.partition));
			context.write(nullKey, outStruct);
		}

	}

}
