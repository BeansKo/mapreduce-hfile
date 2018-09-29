package com.beans.ko.etl.mapreduce.hfile;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beans.ko.etl.mapreduce.model.KeyValueWritable;
import com.beans.ko.etl.mapreduce.model.Student;

public class MRInnerJoinTwo extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MRInnerJoinTwo(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
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

		Job job = Job.getInstance(conf,"MRInnerJoinTwo");
		
		
		job.setJarByClass(MRInnerJoinTwo.class);
		job.setMapperClass(MapInnerJoinTwo.class);
		job.setReducerClass(ReduceInnerJoinTwo.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(KeyValueWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, "/user/fl76/input/studentinfo,/user/fl76/input/studentscore");
		Path outputDir = new Path("/user/fl76/input/stu");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		
		List<Path> paths = Stream.of(fs.listStatus(new Path("/user/fl76/lib"))).map(FileStatus::getPath).collect(Collectors.toList());
		for(Path path:paths){
			job.addFileToClassPath(path);
		}
		
		FileOutputFormat.setOutputPath(job, outputDir);
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static class MapInnerJoinTwo extends Mapper<LongWritable,Text,Text,KeyValueWritable>{

		private Text outKey = new Text();
		private KeyValueWritable outValue = new KeyValueWritable();
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String path = fileSplit.getPath().toString();
			if(path.contains("studentinfo")){
				outValue.setType("info");
			}else if(path.contains("studentscore")){
				outValue.setType("score");
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			String str = value.toString();
			String[] strArr = str.split("\t");
//			System.out.println("value:"+str);
			outKey.set(strArr[0]);
			outValue.setValue(value.toString());
			context.write(outKey, outValue);
		}
	}

	public static class ReduceInnerJoinTwo extends Reducer<Text,KeyValueWritable,NullWritable,Text>{

		String infoSchema = "id,studentname,isstu,remark";
		String scoreSchema = "id,score";
		String[] infoSchemaArr = infoSchema.split(",");
		String[] scoreSchemaArr = scoreSchema.split(",");
		Map<String,String> mp = new HashMap<String,String>();
		
		@Override
		protected void reduce(Text key, Iterable<KeyValueWritable> value,Context context)
				throws IOException, InterruptedException {
			for(KeyValueWritable kv:value){
				String type = kv.getType();
				String va = kv.getValue();
				String[] strArr = va.split("\t");
				for(int i=0;i<strArr.length;i++){
					mp.put((type.equals("info")?infoSchemaArr[i]:scoreSchemaArr[i]), strArr[i]);
				}
			}
			Text outValue = new Text();
			outValue.set(mp.get("id")+"\t"+mp.get("studentname")+"\t"+mp.get("score"));
			context.write(null, outValue);
		}
	}
}
