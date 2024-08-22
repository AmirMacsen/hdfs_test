package com.amir.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSDemo {

    /**
     * 创建目录
     */
    public void mkdir() throws IOException {
        //1. 创建配置
        Configuration configuration = new Configuration();
        //2. 指定namenode
        configuration.set("fs.defaultFS","hdfs://172.16.88.131:9820");
        // 设置环境变量
//        System.setProperty("HADOOP_USER_NAME", "root");
        //3. 创建文件系统对象
        FileSystem fs = FileSystem.get(configuration);
        //4. 调用创建目录的API
        boolean mkdirs = fs.mkdirs(new Path("/api/show"));
        System.out.println("mkdirs result：" + mkdirs);
        fs.close();

    }

    /**
     * 创建目录02
     */
    public void mkdir02() throws IOException, URISyntaxException, InterruptedException {
        //1. 创建配置
        Configuration configuration = new Configuration();
        //2. 创建文件系统对象
        FileSystem fs = FileSystem.get(new URI("hdfs://172.16.88.131:9820"),
                configuration, "root");
        //4. 调用创建目录的API
        boolean mkdirs = fs.mkdirs(new Path("/api/new"));
        System.out.println("mkdirs result：" + mkdirs);
        fs.close();

    }

    /**
     * 文件上传01
     */
    public void uploadFile(){
        try {
            //1. 创建配置
            Configuration configuration = new Configuration();
            //2. 指定namenode
            System.setProperty("HADOOP_USER_NAME", "root");
            configuration.set("fs.defaultFS","hdfs://172.16.88.131:9820");
            FileSystem fs = FileSystem.get(configuration);
            // 3.读取文件流
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            String path = classLoader.getResource("loop03.txt").getPath();
            System.out.println("path: "+ path);
            FileInputStream fis = new FileInputStream(path);
            FSDataOutputStream fos = fs.create(new Path("/user/api/loop03.txt"));
            // 4.字节数组写入数据
            byte[] buf = new byte[1024];
            int len = -1;
            while ((len = fis.read(buf)) != -1) {
                fos.write(buf, 0, len);
            }
            fos.close();
            fis.close();
            fs.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 文件上传02
     */
    public void uploadFile02() throws IOException, URISyntaxException, InterruptedException {
        //1. 创建配置
        Configuration configuration = new Configuration();
        //2. 指定namenode
        System.setProperty("HADOOP_USER_NAME", "root");
        configuration.set("fs.defaultFS","hdfs://172.16.88.131:9820");
        //3. 创建文件系统对象
        FileSystem fs = FileSystem.get(configuration);
        String path = HDFSDemo.class.getClassLoader().getResource("test").getPath();
        fs.copyFromLocalFile(new Path(path), new Path("/user/api/test"));
        fs.close();
    }
    /**
     * 文件改名
     */
    public void rename() throws IOException {
        //1. 创建配置
        Configuration configuration = new Configuration();
        //2. 指定namenode
        System.setProperty("HADOOP_USER_NAME", "root");
        configuration.set("fs.defaultFS","hdfs://172.16.88.131:9820");

        try(FileSystem fs = FileSystem.get(configuration)){
            fs.rename(new Path("/user/api/test01"), new Path("/user/api/test"));
        }
    }

    /**
     * 文件下载
     */
    public void download() throws IOException {
        //1. 创建配置
        Configuration configuration = new Configuration();
        //2. 指定namenode
        System.setProperty("HADOOP_USER_NAME", "root");
        configuration.set("fs.defaultFS","hdfs://172.16.88.131:9820");


        try(FileSystem fs = FileSystem.get(configuration)){
            fs.copyToLocalFile(new Path("/user/api/test"), new Path(
                    "/Users/maningyu/workspace/javaprojects/hdfsclientapi/hdfsclientapi/src/main/resources"));
        }
    }

    /**
     * 文件或者文件夹的删除
     */
    public void delete() throws IOException {
        //1. 创建配置
        Configuration configuration = new Configuration();
        //2. 指定namenode
        System.setProperty("HADOOP_USER_NAME", "root");
        configuration.set("fs.defaultFS","hdfs://172.16.88.131:9820");
        try(FileSystem fs = FileSystem.get(configuration)){
            // 第二个参数为true表示递归删除
            fs.delete(new Path("/user/api/test"), true);
        }
    }
    /**
     * 获取文件详情
     */
    public void fileDetail() throws IOException {
        //1. 创建配置
        Configuration configuration = new Configuration();
        //2. 指定namenode
        System.setProperty("HADOOP_USER_NAME","root");
        configuration.set("fs.defaultFS","hdfs://172.16.88.131:9820");
        try(FileSystem fs = FileSystem.get(configuration)){
            FileStatus[] fileStatuses = fs.listStatus(new Path("/user/api"));
            for (FileStatus fileStatus : fileStatuses) {
                System.out.println(fileStatus.getPath());
                System.out.println(fileStatus.getPermission());
                System.out.println(fileStatus.getOwner());
                System.out.println(fileStatus.getGroup());
                System.out.println(fileStatus.getLen());
                System.out.println(fileStatus.getModificationTime());
                System.out.println(fileStatus.getReplication());
            }
        }
    }

    /**
     * 文件或者方法的判断
     */
    public void isFileOrDir() throws IOException {
        //1. 创建配置
        Configuration configuration = new Configuration();
        //2. 指定namenode
        System.setProperty("HADOOP_USER_NAME","root");
        configuration.set("fs.defaultFS","hdfs://172.16.88.131:9820");
        try(FileSystem fs = FileSystem.get(configuration)){
            System.out.println(fs.isDirectory(new Path("/user/api/test")));
            System.out.println(fs.isFile(new Path("/user/api/loop03.txt")));
        }
    }

}
