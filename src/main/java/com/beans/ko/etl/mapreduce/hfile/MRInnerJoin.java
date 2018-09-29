package com.beans.ko.etl.mapreduce.hfile;

import java.io.File;
import java.io.IOException;
import java.util.List;
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

import com.beans.ko.etl.mapreduce.model.Student;

public class MRInnerJoin extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MRInnerJoin(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
//		conf.set("hbase.zookeeper.quorum","sxlab16,sxlab17,sxlab18");
//		conf.set("hbase.zookeeper.property.clientPort", "2181");
//		conf.set("hbase.client.scanner.timeout.period", "720000");
//		conf.set("hbase.regionserver.lease.period", "720000");
//		conf.set("hbase.client.write.buffer", "500");
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

		Job job = Job.getInstance(conf,"MRInnerJoin");
		
		
		job.setJarByClass(MRInnerJoin.class);
		job.setMapperClass(MapInnerJoin.class);
		job.setReducerClass(ReduceInnerJoin.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Student.class);
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
	
	public static class MapInnerJoin extends Mapper<LongWritable,Text,Text,Student>{

		private Text outKey = new Text();
		private Student student = new Student();
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String path = fileSplit.getPath().toString();
			if(path.contains("studentinfo")){
				student.setType("info");
			}else if(path.contains("studentscore")){
				student.setType("score");
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			String str = value.toString();
			String[] strArr = str.split("\t");
			System.out.println("value:"+str);
			outKey.set(strArr[0]);
			if(student.getType().equals("info")){
				student.setId(Integer.parseInt(strArr[0]));
				student.setStudentName(strArr[1]);
				student.setIsStu(strArr[2]);
				student.setRemark(strArr[3]);
				
			}else if(student.getType().equals("score")){
				student.setId(Integer.parseInt(strArr[0]));
				student.setScore(Integer.parseInt(strArr[1]));
			}
			context.write(outKey, student);
		}
	}

	public static class ReduceInnerJoin extends Reducer<Text,Student,NullWritable,Text>{

		@Override
		protected void reduce(Text key, Iterable<Student> value,Context context)
				throws IOException, InterruptedException {
			Student student = new Student();
			for(Student stu:value){
				if(stu.getType().equals("score")){
					student.setScore(stu.getScore());
					
				}else if(stu.getType().equals("info")){
					student.setStudentName(stu.getStudentName());
					student.setIsStu(stu.getIsStu());
					student.setRemark(stu.getRemark());
				}
				student.setId(stu.getId());
				student.setType(stu.getType());
			}
			Text outValue = new Text();
			outValue.set(student.getId()+"\t"+udf(student.getStudentName(),student.getType(),String.valueOf(student.getScore()))+"\t"+student.getScore());
			context.write(null, outValue);
		}
	}
	
	private static String udf(String str1,String str2,String str3){
		return str1+"_"+str2+"_"+str3;
	}
}
