package com.beans.ko.etl.mapreduce.hfile;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

import com.beans.ko.etl.mapreduce.utils.HBaseUtils;
import com.beans.ko.etl.mapreduce.utils.MapReduceUtils;

public class HBaseHFile2OrcCombiner {

	private static final String SCHEMA_STR = "struct<itemnumber:string,group:string,ic:string>";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, SCHEMA_STR);
		conf.set("hbase.zookeeper.quorum", "sxlab16,sxlab17,sxlab18");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.client.scanner.timeout.period", "720000");
		conf.set("hbase.regionserver.lease.period", "720000");
		conf.set("hbase.client.write.buffer", "500");
		conf.set(
				"io.serializations",
				"org.apache.hadoop.hbase.mapreduce.KeyValueSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		// java.io.IOException: No FileSystem for scheme: hdfs 错误
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		String osName = System.getProperty("os.name");
		// 增加tmpjar,第三方依赖
		File file = null;
		if (osName.startsWith("Windows")) {
			// 本地windows提交方式，设定"submission.cross-platform",提前打包准备好jar包
			conf.set("mapreduce.app-submission.cross-platform", "true");
			conf.set("mapreduce.job.jar",
					"target/mapreduce-hfile-0.0.1-SNAPSHOT.jar");
			file = new File("E:\\github\\mapreduce-hfile\\target\\lib");
			File[] files = file.listFiles();
			for (File fs : files) {
				MapReduceUtils.addTmpJar(fs.getAbsolutePath(), conf);
			}
		} else {
			// linux需要指定第三方jar地址
			String hbaseFile = getJarPathForClass(KeyValue.class);
			System.out.println("hbase file:" + hbaseFile);
			MapReduceUtils.addTmpJar(hbaseFile, conf);
		}

		Job job = Job.getInstance(conf, "HBaseHFile2OrcCombiner");
		job.setJarByClass(HBaseHFile2OrcCombiner.class);
		job.setMapperClass(HFileOrcComMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(com.beans.ko.etl.mapreduce.io.KeyValue.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(OrcStruct.class);

		job.setNumReduceTasks(0);
		job.getConfiguration().setLong("mapred.max.split.size", 122222);

		job.setInputFormatClass(HFileCombineInputFormat.class);
		job.setOutputFormatClass(OrcOutputFormat.class);
		job.setCombinerClass(H2OrcCombiner.class);
		
		SnapshotDescription snapshot = HBaseUtils.getLastestSnapshot(conf,
				"ecitem:IM_ItemBase");
		List<Path> pathList = HBaseUtils.getSnapshotPaths(conf,
				snapshot.getName(), "BaseInfo,ImageInfo");
		for (Path path : pathList) {
			FileInputFormat.addInputPath(job, path);
		}

		Path outputDir = new Path("/user/fl76/output/hbasefile2orcCombiner");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}

		OrcOutputFormat.setOutputPath(job, outputDir);
		job.waitForCompletion(true);
	}

	/**
	 * 操作hbasemappre的输入需要指定为ImmutableBytesWritable格式数据
	 * 
	 * @author fl76
	 *
	 */
	public static class HFileOrcComMapper
			extends
			Mapper<ImmutableBytesWritable, KeyValue, Text, com.beans.ko.etl.mapreduce.io.KeyValue> {

		@Override
		protected void map(ImmutableBytesWritable key, KeyValue value,
				Context context) throws IOException, InterruptedException {

			String rowKey = Bytes.toString(CellUtil.cloneRow(value));
			String vKey = Bytes.toString(CellUtil.cloneQualifier(value));
			byte[] colValue = CellUtil.cloneValue(value);
			com.beans.ko.etl.mapreduce.io.KeyValue v = new com.beans.ko.etl.mapreduce.io.KeyValue();
			v.setKey(new Text(vKey));
			v.setValue(new BytesWritable(colValue));

			context.write(new Text(rowKey), v);
		}
	}

	public static class H2OrcCombiner
			extends
			Reducer<Text, com.beans.ko.etl.mapreduce.io.KeyValue, NullWritable, OrcStruct> {
		private TypeDescription schema = TypeDescription.fromString(SCHEMA_STR);
		private OrcStruct orcs = (OrcStruct) OrcStruct.createValue(schema);
		private Text textValue;

		@Override
		protected void reduce(Text key,
				Iterable<com.beans.ko.etl.mapreduce.io.KeyValue> value,
				Context context) throws IOException, InterruptedException {
			for (com.beans.ko.etl.mapreduce.io.KeyValue kv : value) {
				String fieldName = kv.getKey().toString();
				String fieldvalue = kv.getValue().toString();
				textValue = new Text(fieldvalue);
				if (fieldName.equalsIgnoreCase("ItemNumber")) {
					orcs.setFieldValue(0, textValue);
				} else if (fieldName.equalsIgnoreCase("ItemGroupID")) {
					orcs.setFieldValue(1, textValue);
				} else if (fieldName.equalsIgnoreCase("IC_02")) {
					orcs.setFieldValue(2, textValue);
				}
			}
			context.write(NullWritable.get(), orcs);
		}
	}

	public static String getJarPathForClass(Class<? extends Object> classObj) {
		ClassLoader loader = classObj.getClassLoader();
		String classFile = classObj.getName().replaceAll("\\.", "/") + ".class";
		try {
			for (Enumeration<URL> itr = loader.getResources(classFile); itr
					.hasMoreElements();) {
				URL url = (URL) itr.nextElement();
				if ("jar".equals(url.getProtocol())) {
					String toReturn = url.getPath();
					if (toReturn.startsWith("file:")) {
						toReturn = toReturn.substring("file:".length());
					}

					toReturn = toReturn.replaceAll("\\+", "%2B");
					toReturn = URLDecoder.decode(toReturn, "UTF-8");
					return toReturn.replaceAll("!.*$", "");
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return null;
	}
}
