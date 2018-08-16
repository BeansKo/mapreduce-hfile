package com.beans.ko.etl.mapreduce.hfile;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hsqldb.lib.StringUtil;

import com.beans.ko.etl.mapreduce.utils.HBaseUtils;
import com.beans.ko.etl.mapreduce.utils.MapReduceUtils;

public class HBaseHFile2Seq extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new HBaseHFile2Seq(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("hbase.zookeeper.quorum","sxlab16,sxlab17,sxlab18");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.client.scanner.timeout.period", "720000");
		conf.set("hbase.regionserver.lease.period", "720000");
		conf.set("hbase.client.write.buffer", "500");
		conf.set("io.serializations",
				"org.apache.hadoop.hbase.mapreduce.KeyValueSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		//java.io.IOException: No FileSystem for scheme: hdfs 错误
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		String osName = System.getProperty("os.name");
		
		File file = null;
		if(osName.startsWith("Windows")){
			//本地windows提交方式，设定"submission.cross-platform",提前打包准备好jar包
			conf.set("mapreduce.app-submission.cross-platform", "true");
			conf.set("mapreduce.job.jar", "target/mapreduce-hfile-0.0.1-SNAPSHOT.jar");
			file = new File("E:\\github\\mapreduce-hfile\\target\\lib");
		}else{
			//linux需要指定第三方jar地址
			file = new File("/var/lib/ECTalend/DataFeedGDV/job");
		}
		File[] files = file.listFiles();
		for(File fs:files){
			//增加tmpjar,第三方依赖
			MapReduceUtils.addTmpJar(fs.getAbsolutePath(), conf);
		}
		
		//设置压缩
		Job job = Job.getInstance(conf,"HBaseHFile2Seq");
		job.setJarByClass(HBaseHFile2Seq.class);
		job.setMapperClass(ReadHFile2SeqMapper.class);
		job.setReducerClass(ReadHFile2SeqReducer.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		//设置mr的输入文件类，提供RecordReader的实现类，把InputSplit读到Mapper中进行处理。
		job.setInputFormatClass(HFileCombineInputFormat.class);
		//设置文件输出类型
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//设置split文件大小
		job.getConfiguration().setLong("mapred.max.split.size", 22222);
		SnapshotDescription snapshot = HBaseUtils.getLastestSnapshot(conf,"ecitem:IM_ItemBase");
		List<Path> pathList = HBaseUtils.getSnapshotPaths(conf, snapshot.getName(),"BaseInfo");
		for(Path path :pathList){
			FileInputFormat.addInputPath(job, path);
		}
		Path outputDir = new Path("/user/fl76/output/hbasefile2seq");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}

		SequenceFileOutputFormat.setOutputPath(job, outputDir);
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static class ReadHFile2SeqMapper extends Mapper<ImmutableBytesWritable,KeyValue,ImmutableBytesWritable,KeyValue>{
		
		@Override
		protected void map(
				ImmutableBytesWritable key,
				KeyValue value,
				Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	public static class ReadHFile2SeqReducer extends Reducer<ImmutableBytesWritable,KeyValue,NullWritable,Text>{
		private Text outValue = new Text();
		@Override
		protected void reduce(
				ImmutableBytesWritable key,
				Iterable<KeyValue> value,
				Context context)
				throws IOException, InterruptedException {		
			String keyStr = "";
			String valueStr = "";
			for(KeyValue kv:value){
				String fieldName = Bytes.toString(CellUtil.cloneQualifier(kv));
				if(fieldName.equalsIgnoreCase("ItemNumber")){
					keyStr = Bytes.toString(CellUtil.cloneValue(kv));
				}else if(fieldName.equalsIgnoreCase("ItemGroupID")){
					valueStr = Bytes.toString(CellUtil.cloneValue(kv));
				}
			}
			if(!StringUtil.isEmpty(keyStr)){
				outValue.set(keyStr+"\t"+valueStr);
				context.write(NullWritable.get(), outValue);
			}
		}
	}
}
