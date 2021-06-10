package com.meekou;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.jline.utils.InputStreamReader;
import org.junit.After;
import org.junit.Before;

/**
 * Unit test for simple App.
 */

class AppTest {

    private static final String HDFS_PATH = "hdfs://localhost:9000";
    private static final String HDFS_USER = "hadoop";
    private static FileSystem fileSystem;
    @BeforeAll
    public static void prepare()
    {
        try {
            Configuration configuration = new Configuration();
            configuration.set("dfs.replication", "1");
            fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, HDFS_USER);
        } catch (IOException e) {
            e.printStackTrace();
        } catch(InterruptedException e){
            e.printStackTrace();
        } catch(URISyntaxException e){
            e.printStackTrace();
        }
    }
    @Test
    public void mkDir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfs-api/test/"));
    }
    @Test
    public void mkDirWithPermission() throws Exception{
        fileSystem.mkdirs(new Path("/hdfs-api/test1"), 
        new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.READ));
    }
    @Test
    public void createAndWrite() throws Exception{
        FSDataOutputStream out = fileSystem.create(new Path("/hdfs-api/test/a.txt"), true, 4096);
        out.write("hello hadoop!".getBytes());
        out.flush();
        out.close();
    }
    @Test
    public void checkFileExist() throws Exception{
        boolean exists = fileSystem.exists(new Path("/hdfs-api/test/a.txt"));
        System.out.println(exists);
    }
    @Test
    public void checkFileContent() throws Exception{
        FSDataInputStream input = fileSystem.open(new Path("/hdfs-api/test/a.txt"));
        String context = inputStreamToString(input, "utf-8");
        System.out.println(context);
    }
    private static String inputStreamToString(InputStream inputStream, String encode){
        try {
            if(encode == null || "".equals(encode)){
                encode = "utf-8";
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, encode));
            StringBuilder builder = new StringBuilder();
            String str = "";
            while ((str = reader.readLine())!= null) {
                builder.append(str).append("\n");
                
            }
            return builder.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    @Test
    public void rename() throws Exception{
        Path oldPath = new Path("/hdfs-api/test/a.txt");
        Path newPath = new Path("/hdfs-api/test/c.txt");
        boolean result = fileSystem.rename(oldPath, newPath);
        System.out.println(result);
    }
    @Test
    public void delete() throws Exception{
        boolean result = fileSystem.delete(new Path("/hdfs-api/test/c.txt"),true);
        System.out.println(result);
    }
    @Test
    public void CopyFileFromLocal() throws Exception{
        Path src = new Path("C:/Users/jzhout1/Downloads/CopyTest/a.txt");
        Path dst = new Path("/hdfs-api/test/");
        fileSystem.copyFromLocalFile(src, dst);
    }
    @Test
    public void copyBigFileFromLocal() throws Exception{
        File file = new File("C:/Users/jzhout1/Downloads/Hadoop权威指南+中文版.pdf");
        final float fileSize = file.length();
        InputStream input = new BufferedInputStream(new FileInputStream(file));
        FSDataOutputStream output = fileSystem.create(new Path("/hdfs-api/test/" + file.getName()),
            new Progressable(){
                long fileCount = 0;
                public void progress(){
                    fileCount++;
                    System.out.println("上传速度： " + (fileCount*64*1024/fileSize)*100 + " %");
                }
            });
            IOUtils.copyBytes(input, output, 4096);

    }
    @Test
    //从HDFS上下载文件
    public void copyFileFromHDFS() throws Exception{
        Path src = new Path("/hdfs-api/test/a.txt");
        Path dst = new Path("C:/Users/jzhout1/Downloads/");
        fileSystem.copyToLocalFile(false,src, dst);
    }
    @Test
    //查看指定路径下文件或文件夹的信息
    public void listFiles() throws Exception{
        FileStatus[] status = fileSystem.listStatus(new Path("/hdfs-api"));
        for (FileStatus fileStatus : status) {
            System.out.println(fileStatus.toString());
        }
    }
    @Test
    //递推查看指定目录下所有文件信息
    public void listFilesRecursive() throws Exception{
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/hdfs-api"), true);
        while (files.hasNext()) {
            System.out.println(files.next());
        }
    }
    @Test
    //查看文件块的信息
    public void getFileBlockLocations() throws Exception{
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfs-api/test/a.txt"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation blockLocation : blocks) {
            System.out.println(blockLocation);
        }
    }
    @AfterAll
    public static void destroy(){
        fileSystem = null;
    }
}
