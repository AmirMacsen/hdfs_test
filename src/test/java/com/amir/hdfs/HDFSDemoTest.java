package com.amir.hdfs;

import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

public class HDFSDemoTest {

    @Test
    public void mkdir() throws IOException {
        HDFSDemo hdfsDemo = new HDFSDemo();
        hdfsDemo.mkdir();
    }

    @Test
    public void mkdir02() throws IOException, URISyntaxException, InterruptedException {
        HDFSDemo hdfsDemo = new HDFSDemo();
        hdfsDemo.mkdir02();
    }

    @Test
    public void uploadFile() {
        HDFSDemo hdfsDemo = new HDFSDemo();
        hdfsDemo.uploadFile();
    }

    @Test
    public void uploadFile02() throws IOException, URISyntaxException, InterruptedException {
        HDFSDemo hdfsDemo = new HDFSDemo();
        hdfsDemo.uploadFile02();
    }

    @Test
    public void rename() throws IOException {
        HDFSDemo hdfsDemo = new HDFSDemo();
        hdfsDemo.rename();
    }

    @Test
    public void download() throws IOException {
        HDFSDemo hdfsDemo = new HDFSDemo();
        hdfsDemo.download();
    }

    @Test
    public void delete() throws IOException {
        HDFSDemo hdfsDemo = new HDFSDemo();
        hdfsDemo.delete();
    }

    @Test
    public void fileDetail() throws IOException {
        HDFSDemo hdfsDemo = new HDFSDemo();
        hdfsDemo.fileDetail();
    }

    @Test
    public void isFileOrDir() throws IOException {
        HDFSDemo hdfsDemo = new HDFSDemo();
        hdfsDemo.isFileOrDir();
    }
}