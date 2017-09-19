/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.SHORT_CIRCUIT_WRITE;
import static org.junit.Assert.assertTrue;

public class TestShortCircuitWrite {
  Configuration conf;
  MiniDFSCluster cluster;
  DistributedFileSystem fs;
  int factor = 500;
  int bufferLen = 1024 * 100;
  int fileLen = factor * bufferLen;
  int blockSize = 1024*1024*8;

  @Before
  public void setup() throws IOException {
    conf = new HdfsConfiguration();
    conf.setInt(HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .storageTypes(new StorageType[]{StorageType.DISK, StorageType.SSD})
        .build();
    fs = cluster.getFileSystem();

    cluster.waitActive();
    /*try {
      Thread.sleep(5000);   // wait for datanode initialization complete
    } catch (Exception e) {
    }*/
  }

  @Test
  public void testShortCircuitWrite() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(fileLen);
    byte[] toWriteBytes = generateBytes(fileLen);
    buffer.put(toWriteBytes);
    buffer.flip();

    try {
      Path myFile = new Path("/test/dir/file");
      EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE, SHORT_CIRCUIT_WRITE);
      FSDataOutputStream out = fs.create(myFile,
          FsPermission.getFileDefault(),
          createFlags,
          bufferLen,
          (short) 1,
          blockSize,
          null);

      out.write(buffer.array(),buffer.arrayOffset()+buffer.position(),
          buffer.remaining());
      out.close();
      assertTrue(fs.exists(myFile));

      long writenFileLen = fs.getFileStatus(myFile).getLen();
      Assert.assertEquals(fileLen, writenFileLen);

      byte[] readBytes = new byte[fileLen];
      FSDataInputStream in = fs.open(myFile);
      IOUtils.readFully(in, readBytes, 0, readBytes.length);

      Assert.assertArrayEquals(toWriteBytes, readBytes);

      FSDataInputStream inCheck = fs.open(myFile, 1);
      //inCheck.close();
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testShortCircuitWriteMultipleTimes() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(bufferLen);
    byte[] toWriteBytes = generateBytes(fileLen);
    try {
      Path myFile = new Path("/test/dir/file");
      EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE, SHORT_CIRCUIT_WRITE);
      FSDataOutputStream out = fs.create(myFile,
          FsPermission.getFileDefault(),
          createFlags,
          bufferLen,
          (short) 1,
          blockSize,
          null);
      byte[] toWriteBytesEach;
      for(int i = 0; i < factor;i++) {
        buffer.clear();
        toWriteBytesEach = Arrays.copyOfRange(toWriteBytes, i*bufferLen,
            (i+1)*bufferLen);
        buffer.put(toWriteBytesEach);
        buffer.flip();
        out.write(buffer.array(),buffer.arrayOffset()+buffer.position(),buffer.remaining());
      }
      out.close();
      assertTrue(fs.exists(myFile));

      long writenFileLen = fs.getFileStatus(myFile).getLen();
      Assert.assertEquals(fileLen, writenFileLen);

      byte[] readBytes = new byte[fileLen];
      FSDataInputStream in = fs.open(myFile);
      IOUtils.readFully(in, readBytes, 0, readBytes.length);

      Assert.assertArrayEquals(toWriteBytes, readBytes);
    } finally {
      cluster.shutdown();
    }
  }

  public static byte[] generateBytes(int cnt) {
    byte[] bytes = new byte[cnt];
//    for (int i = 0; i < bytes.length; i++) {
//      bytes[i] = (byte) (i & 0xff);
//    }
    new Random().nextBytes(bytes);
    return bytes;
  }

  @Test
  public void testShortCircuitWriteWithStoragePolicy() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(fileLen);
    byte[] toWriteBytes = generateBytes(fileLen);
    buffer.put(toWriteBytes);
    buffer.flip();

    try {
      fs.mkdirs(new Path("/test"));
      fs.setStoragePolicy(new Path("/test"), "ALL_SSD");
      for (int i = 0; i < 4; i++) {
        Path myFile = new Path("/test/dir/file" + i);
        EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE, SHORT_CIRCUIT_WRITE);
        FSDataOutputStream out = fs.create(myFile,
            FsPermission.getFileDefault(),
            createFlags,
            bufferLen,
            (short) 1,
            blockSize,
            null);

        out.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
            buffer.remaining());
        out.close();
        assertTrue(fs.exists(myFile));
      }

      fs.mkdirs(new Path("/test2"));
      fs.setStoragePolicy(new Path("/test2"), "HOT");
      for (int i = 0; i < 4; i++) {
        Path myFile2 = new Path("/test2/dir/file" + i);
        EnumSet<CreateFlag> createFlags2 = EnumSet.of(CREATE, SHORT_CIRCUIT_WRITE);
        FSDataOutputStream out2 = fs.create(myFile2,
            FsPermission.getFileDefault(),
            createFlags2,
            bufferLen,
            (short) 1,
            blockSize,
            null);

        out2.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
            buffer.remaining());
        out2.close();
        assertTrue(fs.exists(myFile2));
      }
    } finally {
      cluster.shutdown();
    }
  }
}
