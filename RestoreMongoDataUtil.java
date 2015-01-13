package com.genius.xo.mongodb.__.init;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * 从备份文件中恢复备份的数据，此备份文件的数据格式必须是使用mongoexport命令导出的文件不变，并且文件名为 .json 后缀。
 * <br>
 * 此操作会删除掉原有集合中的所有数据，以防止出现 _id 冲突的情况发生。
 * <br>
 * 另外，文件名表示此文件内所对应的collection，即：文件名就是集合名。
 * @author ShangJianguo
 */
public class RestoreMongoDataUtil {
	
	/**
	 * 恢复一个路径（datapath）下的所有数据文件
	 * @param db {@link com.mongodb.DB}的实例
	 * @param datapath 数据文件所在路径
	 * @author ShangJianguo
	 */
	public static void restorePath(DB db, String datapath){
		File dataFile = new File(datapath);
		if (!dataFile.exists()) {
			System.err.println(datapath + " not exists");
			return;
		}
		FileFilter filter = new FileFilter() {
			@Override
			public boolean accept(File file) {
				if (file.isFile() && file.getName().endsWith(".json")) {
					return true;
				}else {
					return false;
				}
			}
		};
		File[] files = dataFile.listFiles(filter);
		System.out.println("there are total " + files.length + " files. \r\n");
		for (File file : files) {
			restoreSingleFile(db, file);
		}
	}
	
	/**
	 * 恢复单个文件内的数据
	 * @param db {@link com.mongodb.DB}的实例
	 * @param file 数据文件File对象
	 * @author ShangJianguo
	 */
	public static void restoreSingleFile(DB db, File file) {
		String filename = file.getName();
		System.out.println("start retoring file: " + file.getName());
		String collName = filename.substring(0, filename.indexOf("."));
		FileInputStream fis = null;
		try {
			 fis = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.err.println("file " + filename + "not exist");
			return;
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		List<String> jsonList = new ArrayList<>();
		String dataline = "";
		try {
			while ((dataline = br.readLine()) != null) {
				if (!"".equals(dataline.trim())) {
					jsonList.add(dataline);
				}
			}
		} catch (IOException e) {
			System.err.println("read file "+filename+" err");
			e.printStackTrace();
		}finally {
			try {
				br.close();
			} catch (IOException e) {
			}
			try {
				fis.close();
			} catch (IOException e) {
			}
		}
		List<DBObject> dbObjList = new ArrayList<>();
		for (String string : jsonList) {
			DBObject obj = (DBObject) JSON.parse(string);
			dbObjList.add(obj);
		}
		DBCollection coll = db.getCollection(collName);
		coll.remove(new BasicDBObject());
		coll.insert(dbObjList);
		System.out.println("retore file: " + file.getName() + " success \r\n");
	}
	
}
