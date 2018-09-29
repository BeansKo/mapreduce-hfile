package com.beans.ko.etl.mapreduce.hfile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;

import com.beans.ko.etl.mapreduce.model.KeyValueWritable;

public class MRInnerJoinThree extends Configured implements Tool{

	private static final String FULL_SCHEMA ="struct<itemnumber:string,itemgroupid:string,countrycode:string>";
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MRInnerJoinThree(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("orc.mapred.output.schema",FULL_SCHEMA);
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

		Job job = Job.getInstance(conf,"MRInnerJoinThree");
		
		job.setJarByClass(MRInnerJoinThree.class);
		job.setMapperClass(MapInnerJoinThree.class);
		job.setReducerClass(ReduceInnerJoinThree.class);
		
		job.setInputFormatClass(OrcInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(KeyValueWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(OrcStruct.class);
		
		job.setOutputFormatClass(OrcOutputFormat.class);

		FileInputFormat.setInputPaths(job, "/user/tiny/fullexport/IM_ItemBase/dt=2018-09-29-08-28-33,/user/tiny/fullexport/IM_ItemPrice/dt=2018-09-29-08-28-33");
		Path outputDir = new Path("/user/fl76/input/iteminner");
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
	
	public static class MapInnerJoinThree extends Mapper<NullWritable,OrcStruct,Text,KeyValueWritable>{

		private Text outKey = new Text();
		private KeyValueWritable outValue = new KeyValueWritable();
		private String[] fieldNames = null;
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String path = fileSplit.getPath().toString();
			if(path.contains("ItemBase")){
				outValue.setType("base");
			}else if(path.contains("ItemPrice")){
				outValue.setType("price");
			}
			
			String schema = context.getCurrentValue().getSchema().toString();
			schema = schema.substring(schema.indexOf("<")+1,schema.length()-1);
			fieldNames = schema.split(",");
		}
		
		@Override
		protected void map(NullWritable key, OrcStruct value,Context context)throws IOException, InterruptedException {
			if(fieldNames == null){
				return;
			}
			StringBuffer sb = new StringBuffer();
			for(String s : fieldNames){
				String fieldName = s.split(":")[0];
				if(!fieldName.equalsIgnoreCase("dt")){
					sb.append(fieldName).append("=").append(value.getFieldValue(fieldName).toString()).append(",");
				}
			}
			
			outKey.set(value.getFieldValue("itemnumber").toString());
			outValue.setValue(sb.toString());
			context.write(outKey, outValue);
		}
	}

	public static class ReduceInnerJoinThree extends Reducer<Text,KeyValueWritable,NullWritable,OrcStruct>{

		private TypeDescription schema = TypeDescription.fromString(FULL_SCHEMA);
		private OrcStruct orcStruct = (OrcStruct)OrcStruct.createValue(schema);
		@Override
		protected void reduce(Text key, Iterable<KeyValueWritable> value,Context context)
				throws IOException, InterruptedException {
			List<String> baseList = new ArrayList<String>();
			List<String> priceList = new ArrayList<String>();

			for (KeyValueWritable kv : value) {
				String type = kv.getType();
				String data = kv.getValue();
				if (type.equalsIgnoreCase("base")) {
					baseList.add(data);
				} else if (type.equalsIgnoreCase("price")) {
					priceList.add(data);
				}
			}
			
			for (String base : baseList) {
				System.out.println("base:" + base);
				for (String baseData : base.split(",")) {
					String[] baseArr = baseData.split("=");
					if(FULL_SCHEMA.contains(baseArr[0])){
						orcStruct.setFieldValue(baseArr[0], new Text(baseArr.length == 2 ? baseArr[1] : ""));
					}
				}
				
				for (String price : priceList) {
					System.out.println("price:" + price);
					for (String priceData : price.split(",")) {
						String[] priceArr = priceData.split("=");
						if(FULL_SCHEMA.contains(priceArr[0])){
							orcStruct.setFieldValue(priceArr[0], new Text(priceArr.length == 2 ? priceArr[1] : ""));
						}
					}
					context.write(NullWritable.get(), orcStruct);
				}
			}
		}
	}
}
