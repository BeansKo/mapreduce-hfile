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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

import com.beans.ko.etl.mapreduce.utils.HBaseUtils;


public class HBaseHFile{

	private static final String SCHEMA_STR = "struct<itemnumber:string,group:string>";
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, SCHEMA_STR);
		conf.set("hbase.zookeeper.quorum","sxlab16,sxlab17,sxlab18");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.client.scanner.timeout.period", "720000");
		conf.set("hbase.regionserver.lease.period", "720000");
		conf.set("hbase.client.write.buffer", "500");
		conf.set(
				"io.serializations",
				"org.apache.hadoop.hbase.mapreduce.KeyValueSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		//java.io.IOException: No FileSystem for scheme: hdfs 错误
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		String osName = System.getProperty("os.name");
		//增加tmpjar,第三方依赖
		File file = null;
		if(osName.startsWith("Windows")){
			//本地windows提交方式，设定"submission.cross-platform",提前打包准备好jar包
			conf.set("mapreduce.app-submission.cross-platform", "true");
			conf.set("mapreduce.job.jar", "target/mapreduce-hfile-0.0.1-SNAPSHOT.jar");
			file = new File("E:\\github\\mapreduce-hfile\\target\\lib");
			File[] files = file.listFiles();
			for(File fs:files){
				addTmpJar(fs.getAbsolutePath(), conf);
			}
		}else{
			//linux需要指定第三方jar地址
			String hbaseFile = getJarPathForClass(KeyValue.class);
			System.out.println("hbase file:"+hbaseFile);
			addTmpJar(hbaseFile,conf);
		}	

		//设置压缩
//		conf.set("mapreduce.map.output.compress", "true");
//		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
//		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.Lz4Codec");
		Job job = Job.getInstance(conf,"HBaseHFile");
		job.setJarByClass(HBaseHFile.class);
		job.setMapperClass(ReadHFileMapper.class);
//		job.setReducerClass(WriteReducer.class);
		job.setReducerClass(WriteORCReduce.class);
		
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(OrcStruct.class);
		//设置split文件大小
		job.getConfiguration().setLong("mapred.max.split.size", 122222);
		//设置mr的输入文件类，提供RecordReader的实现类，把InputSplit读到Mapper中进行处理。
		job.setInputFormatClass(HFileCombineInputFormat.class);
		job.setOutputFormatClass(OrcOutputFormat.class);
		SnapshotDescription snapshot = HBaseUtils.getLastestSnapshot(conf,"ecitem:IM_ItemBase");
		List<Path> pathList = HBaseUtils.getSnapshotPaths(conf, snapshot.getName(),"BaseInfo");
		for(Path path :pathList){
			FileInputFormat.addInputPath(job, path);
		}
		
		Path outputDir = new Path("/user/fl76/output/hbasefile");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		
//		FileOutputFormat.setOutputPath(job, outputDir);
		OrcOutputFormat.setOutputPath(job, outputDir);
		job.waitForCompletion(true);
	}
	
	/**
	 * 操作hbasemappre的输入需要指定为ImmutableBytesWritable格式数据
	 * @author fl76
	 *
	 */
	public static class ReadHFileMapper extends Mapper<ImmutableBytesWritable,KeyValue,ImmutableBytesWritable,KeyValue>{
		
		@Override
		protected void map(
				ImmutableBytesWritable key,
				KeyValue value,
				Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	public static class WriteReducer extends Reducer<ImmutableBytesWritable,KeyValue,Text,Text>{
		@Override
		protected void reduce(
				ImmutableBytesWritable key,
				Iterable<KeyValue> value,
				Context context)
				throws IOException, InterruptedException {
//			Map<String,String> map = new HashMap<String,String>();
			for(KeyValue kv:value){
				String fieldName = Bytes.toString(kv.getQualifier());
				String fieldvalue = Bytes.toString(kv.getValue());
//				map.put(fieldName, fieldvalue);
				context.write(new Text(fieldName), new Text(fieldvalue));
			}
		}
	}
	
	public static class WriteORCReduce extends Reducer<ImmutableBytesWritable,KeyValue,NullWritable,OrcStruct>{

		private TypeDescription schema = TypeDescription.fromString(SCHEMA_STR);
		private OrcStruct orcs = (OrcStruct)OrcStruct.createValue(schema);
		private Text textValue;
		@Override
		protected void reduce(
				ImmutableBytesWritable key,
				Iterable<KeyValue> value,
				Context context)
				throws IOException, InterruptedException {
			boolean flagItem = false;
			for(KeyValue kv:value){
				String fieldName = Bytes.toString(kv.getQualifier());
				if(fieldName.equalsIgnoreCase("ItemNumber")){
					String fieldvalue = Bytes.toString(kv.getValue());
					textValue = new Text(fieldvalue);
					orcs.setFieldValue(0, textValue);
					flagItem = true;
				}else if(fieldName.equalsIgnoreCase("ItemGroupID")){
					String fieldvalue = Bytes.toString(kv.getValue());
					textValue = new Text(fieldvalue);
					orcs.setFieldValue(1, textValue);
				}
			}
			if(flagItem){
				context.write(NullWritable.get(), orcs);
			}
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
