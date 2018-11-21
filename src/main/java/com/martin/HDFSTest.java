package com.martin;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;

public class HDFSTest {
    public static void main(String[] args) throws IOException {
        //  首先连接HDFS
        Configuration conf = new Configuration();
        //  设置使用何种文件系统  这里使用HDFS
        conf.set("dfs.client.use.datanode.hostname","true");
        conf.set("fs.defaultFS","hdfs://master:9000");
        System.setProperty("HADOOP_USER_NAME","root");
        FileSystem fs = FileSystem.get(conf);
        // 操作1:新建一个文件
       //  fs.create(new Path("/user/hello"),false);
        // 操作2:从HDFS上下载一个文件  史前巨错:dfs.replication写成了dfs.replaction  难怪创建没问题，一到处理数据都为空
        //fs.copyToLocalFile(false,new Path("/a.txt"),new Path("F://temp/hdfs/"),true);
      //  fs.copyFromLocalFile(new Path("F:\\temp\\Demo2.class"),new Path("/"));
        FSDataOutputStream outputStream = fs.create(new Path("/1.txt"), true);
        FileInputStream inputStream = new FileInputStream("F:\\temp\\hdfs\\ccc.txt");
        IOUtils.copy(inputStream,outputStream);
        fs.close();
    }

}
