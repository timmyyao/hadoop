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

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.net.NetUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

/**
 * The LocalStreamer class is responsible for writing data directly to local
 * storage (short-circuit write).
 */
public class LocalStreamer {
  protected final DFSClient dfsClient;
  protected final long fileId;
  //this is for local write.
  private SocketChannel socketChannel = null;

  private BufferedOutputStream out = null;
  private FileChannel fileChannel = null;
  private String pathName = null;

  private LocatedBlock locatedBlock = null;
  private ExtendedBlock block = null;

  private DataStreamer streamer;
  private static int perQueueSize;
  private ByteBuffer currentByteBuffer;

  public LocalStreamer(DFSClient dfsClient,long fileId, DataStreamer streamer) {
    this.dfsClient = dfsClient;
    this.fileId = fileId;
    this.streamer = streamer;
  }

  private Socket createWriteSocketChannel(DatanodeInfo datanode, int port)
      throws IOException {
    String dnAddr = datanode.getIpAddr();
    InetSocketAddress isa = NetUtils.createSocketAddr(dnAddr,port);
    Socket sock = dfsClient.socketFactory.createSocket();
    int timeout = dfsClient.getDatanodeReadTimeout(2);
    NetUtils.connect(sock, isa, dfsClient.getRandomLocalInterfaceAddr(),
        dfsClient.getConf().getSocketTimeout());
    sock.setSoTimeout(timeout);
    sock.setSendBufferSize(128 * 1024);
    return sock;
  }

  private void createLocalWriteFileChannel(SocketChannel socketChannel)
      throws IOException {
    assert null != block : "You Must Apply For a block First to Create a " +
        "File Channel";
    assert  null != socketChannel :"SocketChannel for first DataNode is null";
    ByteBuffer buf = ByteBuffer.allocate(16);
    buf.putLong(-block.getBlockId());
    buf.putLong(block.getGenerationStamp());
    buf.flip();
    writeChannelFully(socketChannel,buf);
    buf.clear();
    buf.limit(4);
    readChannelFully(socketChannel,buf,4);
    buf.flip();
    int pathLen = buf.getInt();
    buf = ByteBuffer.allocate(pathLen);
    readChannelFully(socketChannel,buf,pathLen);
    buf.flip();
    CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    CharBuffer charBuffer = decoder.decode(buf.asReadOnlyBuffer());
    pathName = charBuffer.toString();
  }

  public String getBlockFilePath() throws IOException {
    return pathName;
  }

  private static void readChannelFully(ReadableByteChannel ch, ByteBuffer buf,
      int n) throws IOException {
    while (n > 0) {
      int ret = ch.read(buf);
      n-=ret;
      if (n < 0) {
        throw new IOException("Premature EOF reading from " + ch);
      }
    }
  }

  private static void writeChannelFully(WritableByteChannel ch, ByteBuffer buf)
      throws IOException {
    while (buf.hasRemaining()) {
      ch.write(buf);
    }
  }

  public LocalStreamer setDataStreamer(DataStreamer dataStreamer) {
    this.streamer = dataStreamer;
    return this;
  }

  public ExtendedBlock getBlock() {
    return block;
  }

  public void start() {
    try {
      perQueueSize = dfsClient.getConf().getShortCircuitConf()
          .getByteBufferQueueSize();
      locatedBlock = streamer.locateFollowingBlock(null, null);
      block = locatedBlock.getBlock();
      DatanodeInfo[] nodes = locatedBlock.getLocations();
      assert  nodes.length == 1 : "To use LocalWrite DataNode Replica " +
          "must be 1.";
      Socket socket1 = createWriteSocketChannel(nodes[0],8899);
      socketChannel = socket1.getChannel();
      createLocalWriteFileChannel(socketChannel);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void close() throws IOException {
    //long begin = Time.monotonicNow();
    if (out != null) {
      fileChannel.force(true);
      out.close();
    } else {
      File extFile = new File(pathName);
      if (!extFile.exists()) {
        throw new IOException("File must be created: " + pathName);
      }
      block.setNumBytes(extFile.length());
    }
    socketChannel.close();
    //DFSClient.LOG.info("close file:"+src+" cost time:"+(Time.monotonicNow()-begin)+" ms");
  }

  public synchronized void write(int b) throws IOException {
    byte[] bytes = new byte[] {(byte) b};
    write(bytes, 0, 1);
  }

  public synchronized void write(byte b[], int off, int len)
      throws IOException {
    //write(ByteBuffer.wrap(b, off, len),true);
    if (out == null) {
      File f = new File(pathName);
      f.createNewFile();
      FileOutputStream outputStream = new FileOutputStream(f);
      fileChannel = outputStream.getChannel();
      out = new BufferedOutputStream(outputStream,perQueueSize);
    }
    out.write(b,off,len);
    block.setNumBytes(block.getNumBytes() + len);
  }
}
