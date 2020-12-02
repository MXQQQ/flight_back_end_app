package edu.flight.flightdemo0123.service;

import edu.flight.flightdemo0123.util.HdfsConn;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HdfsService {

    /**
     * 在HDFS创建文件夹
     * @param path 路径
     * @return state
     * @throws Exception 异常
     */
    public boolean mkdir(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        if (exist(path)) {
            return true;
        }
        FileSystem fs = HdfsConn.getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        boolean state = fs.mkdirs(srcPath);
        fs.close();
        return state;
    }


    /**
     * 判断HDFS文件是否存在
     * @param path 路径
     * @return 是否存在
     * @throws Exception 异常
     */
    public boolean exist(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        FileSystem fs = HdfsConn.getFileSystem();
        Path srcPath = new Path(path);
        return fs.exists(srcPath);
    }

    /**
     * 读取HDFS目录信息
     * @param path 路径
     * @return list
     * @throws Exception 异常
     */
    public List<Map<String, Object>> readPathInfo(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!exist(path)) {
            return null;
        }
        FileSystem fs = HdfsConn.getFileSystem();
        // 目标路径
        Path newPath = new Path(path);
        FileStatus[] statusList = fs.listStatus(newPath);
        List<Map<String, Object>> list = new ArrayList<>();
        if (null != statusList && statusList.length > 0) {
            for (FileStatus fileStatus : statusList) {
                Map<String, Object> map = new HashMap<>();
                map.put("filePath", fileStatus.getPath());
                map.put("fileStatus", fileStatus.toString());
                list.add(map);
            }
            return list;
        } else {
            return null;
        }
    }

    /**
     * 上传HDFS文件
     * @param path
     * @param uploadPath
     * @throws Exception
     */
    public void uploadFile(String path, String uploadPath) throws Exception {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(uploadPath)) {
            return;
        }
        FileSystem fs = HdfsConn.getFileSystem();
        // 上传路径
        Path clientPath = new Path(path);
        // 目标路径
        Path serverPath = new Path(uploadPath);

        // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
        fs.copyFromLocalFile(false, clientPath, serverPath);
        fs.close();
    }

}
