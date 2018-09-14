package com.beans.ko.etl.mapreduce.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest.StoreFile;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

public class HBaseUtils {
	private static final Log logger = LogFactory.getLog(HBaseUtils.class);
	private static final String SNAPSHOT_PREFIX = "SNP_TS_";

	/*
	 * 获取指定表的最新的Snapshot信息
	 */
	public static SnapshotDescription getLastestSnapshot(Configuration conf,
			String tableName) throws IOException {
		tableName = wrapSnapShotName(tableName);
		HBaseAdmin hBaseAdmin = null;
		try {
			hBaseAdmin = new HBaseAdmin(conf);
			List<SnapshotDescription> snapshotList = hBaseAdmin.listSnapshots();
			long tempTime = 0L;
			SnapshotDescription lastestSnapshot = null;
			for (SnapshotDescription snapshotDescription : snapshotList) {
				if (snapshotDescription.getName().startsWith(SNAPSHOT_PREFIX + tableName)) {
					long creationTime = snapshotDescription.getCreationTime();
					if (creationTime > tempTime) {
						tempTime = creationTime;
						lastestSnapshot = snapshotDescription;
					}
				}
			}
			logger.info("tiem:" + tempTime);
			return lastestSnapshot;
		} finally {
			if (hBaseAdmin != null) {
				try {
					hBaseAdmin.close();
				} catch (IOException e) {
					throw e;
				}
			}
		}

	}

	/*
	 * 创建snapshot
	 */
	public static void createSnapshot(Configuration conf,long startTime, String... tables) throws SnapshotCreationException, IllegalArgumentException, IOException{
		HBaseAdmin hBaseAdmin = null;
		try{
			hBaseAdmin = new HBaseAdmin(conf);
			for(String table:tables){
				String snapshotName = SNAPSHOT_PREFIX+table+"_"+startTime;
				snapshotName = wrapSnapShotName(snapshotName);
				hBaseAdmin.snapshot(snapshotName.getBytes(),table.getBytes());
				logger.info("Create Snapshot:"+snapshotName);
			}
		}finally{
			if(hBaseAdmin != null){
				try {
					hBaseAdmin.close();
				} catch (IOException e) {
					throw e;
				}
			}
		}
	}


	public static List<Path> getSnapshotPaths(Configuration conf,
			String snapshotName, String columnFamilyInfo) throws IOException {
		// 指定hbase存储文件路径，否则默认是temp目录
		conf.set("hbase.rootdir", "hdfs://sxlab16/hbase");
		// 避免"java.io.IOException: No FileSystem for scheme: hdfs"的错误
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		List<Path> paths = getSnapshotFiles(conf, snapshotName);
		Path inputRoot = null;
//		Path inputArchive = null;
		FileSystem inputFs = null;
		try{
			inputRoot = new Path(conf.get("hbase.rootdir"));
			logger.info("Input Root:"+inputRoot.toUri());
//			inputArchive = new Path(inputRoot,"archive");
//			logger.info("Input archive:"+inputArchive.toUri());
			inputFs = FileSystem.get(inputRoot.toUri(), conf);
		}catch(IOException e){
			throw new RuntimeException("Could not get the input FileSystem with root="+inputRoot,e);
		}

		String[] columnFamilys = StringUtils.split(columnFamilyInfo,",");
		for(int i=0;i<columnFamilys.length;i++){
			columnFamilys[i] = columnFamilys[i]+"/";
		}
		
		List<Path> pathList = new ArrayList<Path>();
		for(Path path:paths){
			logger.info("path:"+path.toString());
			//BaseInfo/ecitem=IM_ItemBase=795f94b9884fe327470d1ea720ec03b8-1538cb1b900f476da58bfb7d40b20ddb
			//ImageInfo/ecitem=IM_ItemBase=795f94b9884fe327470d1ea720ec03b8-5b7deb7a4748433092b2ecdb8b63af6c
			if(StringUtils.indexOfAny(path.toString(), columnFamilys)== -1){
				continue;
			}
			if(StringUtils.countMatches(path.toString(), "=")>2){
				HFileLink link = HFileLink.buildFromHFileLinkPattern(conf, path);
				HFileLink archiveLink = HFileLink.buildFromHFileLinkPattern(conf, link.getArchivePath());
				FileStatus status = archiveLink.getFileStatus(inputFs);
				if(status != null){
					Path filterPath = status.getPath();
					pathList.add(filterPath);
					logger.info("add job input path:" + filterPath.toUri().toString());
				}
				
			}else{
				//old逻辑
				if(HFileLink.isHFileLink(path) || HFileLink.isBackReferencesDir(path)){
					HFileLink link = HFileLink.buildFromHFileLinkPattern(conf, path);
					FileStatus status = link.getFileStatus(inputFs);
					if(status != null){
						String hfilePath = status.getPath().toString();
						if(StringUtils.indexOfAny(hfilePath, columnFamilys) != -1){
							Path filterPath = status.getPath();
							pathList.add(filterPath);
							logger.info("add job input path:" + filterPath.toUri().toString());
						}
					}
				}
			}
		}
		return pathList;
	}

	/*
	 * 获取snapshot文件地址
	 */
	public static List<Path> getSnapshotFiles(final Configuration conf,
			String snapshotName) throws IOException {
		final FileSystem fs = FileSystem.get(conf);
		Path rootPath = FSUtils.getRootDir(conf);
		logger.info("Hbase RootPaht:" + rootPath.toString());
		// 获取当前SnapShop目录hdfs://10.16.238.79/hbase/.hbase-snapshot/SNP_EC_IM_ItemBase_1532166970838
		Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(
				snapshotName, rootPath);
		logger.info("Snapshot Dir:" + snapshotDir);
		final List<Path> files = new ArrayList<Path>();

		SnapshotReferenceUtil.visitReferencedFiles(conf, fs, snapshotDir,
				new SnapshotReferenceUtil.SnapshotVisitor() {

					@Override
					public void logFile(String server, String logfile)
							throws IOException {
						logger.info("logFile:" + logfile);
					}

					@Override
					public void storeFile(HRegionInfo regionInfo,
							String familyName, StoreFile storeFile)
							throws IOException {

						String hfileName = storeFile.getName();
						logger.info("HFileName:" + hfileName);
						String regionName = regionInfo.getEncodedName();
						logger.info("RegionName:" + regionName);
						Path path = new Path(familyName, HFileLink.createHFileLinkName(regionInfo.getTable(),
										regionName, hfileName));
						//BaseInfo/ecitem=IM_ItemBase=795f94b9884fe327470d1ea720ec03b8-1538cb1b900f476da58bfb7d40b20ddb
						//ImageInfo/ecitem=IM_ItemBase=795f94b9884fe327470d1ea720ec03b8-99e45049c28042cbb2529f11c1860672
						logger.info("StoreFile:" + path.toUri());
						files.add(path);
					}
				});

		return files;
	}

	private static String wrapSnapShotName(String snapshotName) {
		snapshotName = snapshotName.replace(":", "_");
		return snapshotName;
	}

	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","sxlab16,sxlab17,sxlab18");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.client.scanner.timeout.period", "720000");// 6min
		conf.set("hbase.regionserver.lease.period", "720000");// //6min
		conf.set("hbase.client.write.buffer", "500");
		conf.set("hbase.regionserver.global.memstore.upperLimit", "0.45");
		try {
			logger.info("----------begin---------------");
			long startTime = System.currentTimeMillis();
			HBaseUtils.createSnapshot(conf, startTime, "ecitem:IM_ItemBase");
			SnapshotDescription snapshot = HBaseUtils.getLastestSnapshot(conf,"ecitem:IM_ItemBase");
			if(snapshot == null){
				logger.error("snapshot is null");
				return;
			}
			
			logger.info(snapshot.getName());
			List<Path> pathList = getSnapshotPaths(conf, snapshot.getName(),"BaseInfo");
			Reference reference = Reference.createTopReference("0".getBytes());
			FileSystem fs = FileSystem.get(conf);
			HalfStoreFileReader reader = new HalfStoreFileReader(fs,pathList.get(0),new CacheConfig(conf),
					reference,conf);
			HFileScanner scanner = reader.getScanner(false, false);
			reader.loadFileInfo();
			scanner.seekTo();
			while(scanner.next()){
				logger.info("key:"+scanner.getKey());
				Cell cell = scanner.getKeyValue();
				logger.info("rowkey:"+Bytes.toString(CellUtil.cloneRow(cell)));
				logger.info("family:"+Bytes.toString(cell.getFamily()));
				logger.info("qualifior:"+Bytes.toString(cell.getQualifier()));
				logger.info("value:"+Bytes.toString(cell.getValue()));
			}
//			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
