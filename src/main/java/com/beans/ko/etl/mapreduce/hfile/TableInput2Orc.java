package com.beans.ko.etl.mapreduce.hfile;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

import com.beans.ko.etl.mapreduce.utils.MapReduceUtils;

public class TableInput2Orc {

private static final String SCHEMA_STR = "struct<itemnumber:string,group:string,ic:string>";
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
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

	    conf.addResource("hbase-site.xml");
		Job job = Job.getInstance(conf,"TableInput2OrcNoReduce");
		job.setJarByClass(TableInput2Orc.class);
		job.setMapperClass(TableInputMapper.class);

		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(OrcStruct.class);
		
		
		job.setNumReduceTasks(0);
		String table = "ecitem:IM_ItemBase";


		Scan scan = new Scan();
		scan.setCaching(10000);
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob(table,scan,TableInputMapper.class,NullWritable.class,OrcStruct.class,job);

		job.setOutputFormatClass(OrcOutputFormat.class);

		
		Path outputDir = new Path("/user/fl76/output/tableinput2orc");
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
	public static class TableInputMapper extends TableMapper<NullWritable,OrcStruct>{

		private TypeDescription schema = TypeDescription.fromString(SCHEMA_STR);
		private OrcStruct orcs = (OrcStruct)OrcStruct.createValue(schema);
		private Text textValue;
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Context context)
				throws IOException, InterruptedException {
			if(value == null || value.isEmpty()) return;
			
			for(Cell cell:value.listCells()){
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				if(qualifier.equalsIgnoreCase("ItemNumber")){
					String fieldvalue = Bytes.toString(CellUtil.cloneValue(cell));
					textValue = new Text(fieldvalue);
					orcs.setFieldValue(0, textValue);
				} else if(qualifier.equalsIgnoreCase("ItemGroupID")){
					String fieldvalue = Bytes.toString(CellUtil.cloneValue(cell));
					textValue = new Text(fieldvalue);
					orcs.setFieldValue(1, textValue);
				}
				else if(qualifier.equalsIgnoreCase("IC_02")){
					String fieldvalue = Bytes.toString(CellUtil.cloneValue(cell));
					textValue = new Text(fieldvalue);
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
