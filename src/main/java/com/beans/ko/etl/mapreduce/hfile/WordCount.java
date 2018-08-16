package com.beans.ko.etl.mapreduce.hfile;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new WordCount(), args);
	}
	
	public static class WordCountMap extends Mapper<LongWritable,Text,Text,LongWritable>{
		LongWritable outValue = new LongWritable(1);
		Text outKey = new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			String[] splitString = value.toString().split(" ");
			for(String str:splitString){
				outKey.set(str);
				context.write(outKey, outValue);
			}
		}
	}
	
	public static class WordCountReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
		@Override
		protected void reduce(Text keyIn, Iterable<LongWritable> valueIn,Context context)
				throws IOException, InterruptedException {
			long num = 0;
			for(LongWritable value:valueIn){
				num += value.get();
			}
			context.write(keyIn, new LongWritable(num));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "fl76");
		Configuration conf = new Configuration();
		String osName = System.getProperty("os.name");
		if(osName.startsWith("Windows")){
			//本地windows提交方式，设定"submission.cross-platform",提前打包准备好jar包
			conf.set("mapreduce.app-submission.cross-platform", "true");
			conf.set("mapreduce.job.jar", "target/mapreduce-hfile-0.0.1-SNAPSHOT.jar");
		}		
		
		Job job = Job.getInstance(conf);
		job.setJobName("WordCount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMap.class);
		job.setReducerClass(WordCountReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path("/user/fl76/input/wordcount-input.txt"));
		Path outPath = new Path("/user/fl76/output/wordcount");
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
	    return job.waitForCompletion(true)?0:1;
	}
	
//	public static File createTempJar(String root) throws IOException {
//        if (!new File(root).exists()) {
//             return new File(System.getProperty("java.class.path"));
//        }
//        Manifest manifest = new Manifest();
//        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
//        final File jarFile = File.createTempFile("EJob-", ".jar", new File(System.getProperty("java.io.tmpdir")));
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            public void run() {
//                jarFile.delete();
//            }
//        });
//        JarOutputStream out = new JarOutputStream(new FileOutputStream(jarFile), manifest);
//        createTempJarInner(out, new File(root), "");
//        out.flush();
//        out.close();
//        return jarFile;
//    }
// 
//    private static void createTempJarInner(JarOutputStream out, File f,
//            String base) throws IOException {
//        if (f.isDirectory()) {
//            File[] fl = f.listFiles();
//            if (base.length() > 0) {
//                base = base + "/";
//            }
//            for (int i = 0; i < fl.length; i++) {
//                createTempJarInner(out, fl[i], base + fl[i].getName());
//            }
//        } else {
//            out.putNextEntry(new JarEntry(base));
//            FileInputStream in = new FileInputStream(f);
//            byte[] buffer = new byte[1024];
//            int n = in.read(buffer);
//            while (n != -1) {
//                out.write(buffer, 0, n);
//                n = in.read(buffer);
//            }
//            in.close();
//        }
//    }
    
//	public static void addTmpJar(String jarPath, Configuration conf)
//			throws IOException {
//		System.setProperty("path.separator", ":");
//		FileSystem fs = FileSystem.getLocal(conf);
//		String newJarPath = new Path(jarPath).makeQualified(fs).toString();
//		String tmpJars = conf.get("tmpjars");
//		if (tmpJars == null || tmpJars.length() == 0) {
//			conf.set("tmpjars", newJarPath);
//		} else {
//			conf.set("tmpjars", tmpJars + "," + newJarPath);
//		}
//	}
}
