package edu.flight.flightdemo0123.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HdfsConn {
    private static final String hdfsPath = "hdfs://192.168.2.2:9000";

    private static final String hdfsName = "hadoop";


    /**
     * 获取HDFS配置信息
     *
     * @return configuration
     */
    private static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsPath);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return configuration;
    }

    /**
     * 获取HDFS文件系统对象
     *
     * @return fileSystem
     * @throws Exception 异常
     */
    public static FileSystem getFileSystem() throws Exception {
        // 客户端去操作hdfs时是有一个用户身份的，默认情况下hdfs客户端api会从jvm中获取一个参数作为自己的用户身份
        // DHADOOP_USER_NAME=hadoop
        // 也可以在构造客户端fs对象时，通过参数传递进去
        return FileSystem.get(getConfiguration());
    }
}
