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


public class HBaseHFile2OrcNoReduce{

	private static final String SCHEMA_STR = "struct<itemnumber:string,group:string,ic:string>";
	
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
				MapReduceUtils.addTmpJar(fs.getAbsolutePath(), conf);
			}
		}else{
			//linux需要指定第三方jar地址
			String hbaseFile = getJarPathForClass(KeyValue.class);
			System.out.println("hbase file:"+hbaseFile);
			MapReduceUtils.addTmpJar(hbaseFile,conf);
		}	

		Job job = Job.getInstance(conf,"HBaseHFile2OrcNoReduce");
		job.setJarByClass(HBaseHFile2OrcNoReduce.class);
		job.setMapperClass(ReadHFileMapper.class);
//		job.setReducerClass(WriteORCReduce.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(OrcStruct.class);
		
		job.setNumReduceTasks(0);
//		job.setOutputKeyClass(NullWritable.class);
//		job.setOutputValueClass(OrcStruct.class);
		//设置split文件大小
		job.getConfiguration().setLong("mapred.max.split.size", 122222);
		//设置mr的输入文件类，提供RecordReader的实现类，把InputSplit读到Mapper中进行处理。
		job.setInputFormatClass(HFileCombineInputFormat.class);
		job.setOutputFormatClass(OrcOutputFormat.class);
		SnapshotDescription snapshot = HBaseUtils.getLastestSnapshot(conf,"ecitem:IM_ItemBase");
		List<Path> pathList = HBaseUtils.getSnapshotPaths(conf, snapshot.getName(),"BaseInfo,ImageInfo");
		for(Path path :pathList){
			FileInputFormat.addInputPath(job, path);
		}
		
		Path outputDir = new Path("/user/fl76/output/hbasefile2orc");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		
		OrcOutputFormat.setOutputPath(job, outputDir);
		job.waitForCompletion(true);
	}
	
	/**
	 * 操作hbasemappre的输入需要指定为ImmutableBytesWritable格式数据
	 * @author fl76
	 *
	 */
	public static class ReadHFileMapper extends Mapper<ImmutableBytesWritable,KeyValue,NullWritable,OrcStruct>{
		
		private TypeDescription schema = TypeDescription.fromString(SCHEMA_STR);
		private OrcStruct orcs = (OrcStruct)OrcStruct.createValue(schema);
		private Text textValue;
		@Override
		protected void map(
				ImmutableBytesWritable key,
				KeyValue value,
				Context context)
				throws IOException, InterruptedException {
			System.out.println(orcs.getSchema());
			boolean flagItem = false;
				String fieldName = Bytes.toString(CellUtil.cloneQualifier(value));
				System.out.println("Map Field:"+fieldName);
				if(fieldName.equalsIgnoreCase("ItemNumber")){
					String fieldvalue = Bytes.toString(CellUtil.cloneValue(value));
					textValue = new Text(fieldvalue);
					orcs.setFieldValue(0, textValue);
					flagItem = true;
				}else if(fieldName.equalsIgnoreCase("ItemGroupID")){
					String fieldvalue = Bytes.toString(CellUtil.cloneValue(value));
					textValue = new Text(fieldvalue);
					orcs.setFieldValue(1, textValue);
				}
				else if(fieldName.equalsIgnoreCase("IC_02")){
					String fieldvalue = Bytes.toString(CellUtil.cloneValue(value));
					textValue = new Text(fieldvalue);
					orcs.setFieldValue(2, textValue);
				}
//			if(flagItem){
				context.write(NullWritable.get(), orcs);
//			}
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
