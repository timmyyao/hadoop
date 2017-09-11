package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.LocalWrite.DFS_CLIENT_LOCALWRITE_BYTEBUFFER_PER_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.LocalWrite.DFS_CLIENT_LOCALWRITE_BYTEBUFFER_PER_SIZE_KEY;

public class ShortCircuitWriteServer implements Runnable {
  public static final Logger LOG = DataNode.LOG;

  private DataNode dataNode;
  private Configuration config;
  private int per_buffer_size ;
  private volatile boolean shutdown = false;

  private static final String BLOCK_TMP_DIR = "scwtemp";
  private static final byte[] META_DATA = new byte[]{0, 1, 0, 0, 0, 2, 0};

  private String blockPoolID;
  private FsDatasetSpi<?> fsDataset = null;
  private List<FsVolumeImpl> volumes;
  int nDirs;
  File[] finalizedDirs = null;
  URI[] baseDirs = null;
  URI[] blockTempDirs = null;
  String[] storageIDs = null;

  private int blockIndex = 0;

  private final long blockSize;


  ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

  ShortCircuitWriteServer(DataNode dataNode, Configuration config) {
    this.dataNode = dataNode;
    this.config = config;
    assert null != config;
    per_buffer_size = config.getInt(
        DFS_CLIENT_LOCALWRITE_BYTEBUFFER_PER_SIZE_KEY,
        DFS_CLIENT_LOCALWRITE_BYTEBUFFER_PER_SIZE_DEFAULT);
    blockSize = config.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT);
  }

  public void init() {
    while (true) {
      fsDataset =  dataNode.getFSDataset();
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
      }
      if (fsDataset != null) {
        break;
      }
    }

    volumes = new ArrayList<>();
    for (FsVolumeSpi fsVolumeSpi : fsDataset.getFsVolumeReferences()) {
      if (fsVolumeSpi instanceof FsVolumeImpl) {
        volumes.add((FsVolumeImpl)fsVolumeSpi);
      } else {
        throw new RuntimeException("FsVolumeSpi is not implemented by " +
            "FsVolumeImpl, it is " + fsVolumeSpi.getClass());
      }
    }
    //String localDirs = config.get("dfs.datanode.data.dir");
    List<BPOfferService> bpos = dataNode.getAllBpOs();
    blockPoolID = bpos.get(0).getBlockPoolId();

    nDirs = volumes.size();
    finalizedDirs = new File[nDirs];
    baseDirs = new URI[nDirs];
    blockTempDirs = new URI[nDirs];
    storageIDs = new String[nDirs];

    try {
      for (int i = 0; i < volumes.size(); i++) {
        finalizedDirs[i] = volumes.get(i).getFinalizedDir(blockPoolID);
        baseDirs[i] = volumes.get(i).getBaseURI();
        blockTempDirs[i] = baseDirs[i]; // + "/" + BLOCK_TMP_DIR;
        storageIDs[i] = volumes.get(i).getStorageID();
      }
    } catch (IOException e) {
      LOG.error("[SCW] Error in short circuit write internal initialization:" + e);
      shutdown = true;
    }
  }

  public void shutdownServer() {
    shutdown = true;
  }

  private void startServer(int port) {
    try {
      ServerSocketChannel ssc = ServerSocketChannel.open();
      Selector accSel = Selector.open();
      ServerSocket socket = ssc.socket();
      socket.setReuseAddress(true);
      socket.bind(new InetSocketAddress(port));
      ssc.configureBlocking(false);
      ssc.register(accSel, SelectionKey.OP_ACCEPT);

      while (ssc.isOpen() && !shutdown) {
        if (accSel.select(1000) > 0) {
          Iterator<SelectionKey> it = accSel.selectedKeys().iterator();
          while (it.hasNext()) {
            SelectionKey key = it.next();
            it.remove();
            if (key.isAcceptable()) {
              handleAccept(key);
            }
          }
        }
      }
    } catch (IOException e) {
      LOG.error("[SCW] Failed in SCW startServer on Port " + port + " :", e);
    }
  }

  private void handleAccept(SelectionKey key) {
    try {
      ServerSocketChannel server = (ServerSocketChannel) key.channel();
      SocketChannel client = server.accept();
      if (client != null) {
        cachedThreadPool.execute(new WriteHandler(client, blockIndex++));
      }
    } catch (IOException e) {
      LOG.error("[SCW] Failed in SCW handleAccept:", e);
    }
  }
  private static boolean used = false;

  @Override
  public void run() {
    //init();
    startServer(8899);
  }

  class WriteHandler implements Runnable {
    public final int BUFFER_SIZE = per_buffer_size;

    private SocketChannel sc;
    private ByteBuffer bb;

    private File blockTempFile;
    private File blockMetaTempFile;

    private ExtendedBlock block;

    private long dataLen = 0;

    private int currBlockIndex;
    private int volIndex;
    private long blockID;
    private long blockGS;



    WriteHandler(SocketChannel sc, int index) {
      this.sc = sc;
      bb = ByteBuffer.allocate(BUFFER_SIZE);
      currBlockIndex = index;
      volIndex = currBlockIndex % nDirs;
    }

    @Override
    public void run() {
      try {
        addBlock();
      } catch (IOException e) {
        LOG.error("[SCW] Error in write replica: " + e);
      }
    }

    private void addBlock() throws IOException {
      long start = Time.monotonicNow();

      if (writeFile() == 0) {
        return ;
      }
//      long begin = Time.monotonicNow();
      writeMetaFile();

      if (dataLen == 0) {
        dataLen = blockTempFile.length();
      }

      ReplicaBeingWritten rbwReplica = new ReplicaBeingWritten(blockID, blockGS,
          volumes.get(volIndex), new File(blockTempDirs[volIndex]), 0);
      rbwReplica.setNumBytes(dataLen);

      FinalizedReplica finalizedReplica = ((FsDatasetImpl) (dataNode.data))
          .finalizeScwBlock(blockPoolID, rbwReplica, block.getLocalBlock());

      //update metrics
      dataNode.metrics.addWriteBlockOp(Time.monotonicNow() - start);
      dataNode.metrics.incrWritesFromClient(true, dataLen);

      block.setNumBytes(finalizedReplica.getBlockFile().length());  // update block length
      dataNode.closeBlock(block, null, storageIDs[volIndex],
          finalizedReplica.isOnTransientStorage());

//      LOG.info("total time write file used:"+(Time.monotonicNow()-begin));
//      LOG.info("[SCW] Write block successfully: " + block);
    }

    private void writeMetaFile() throws IOException {
      blockMetaTempFile = new File(DatanodeUtil.getMetaName(blockTempFile.getAbsolutePath(), blockGS));
      try {
        FileOutputStream osMeta = new FileOutputStream(blockMetaTempFile, false);
        osMeta.write(META_DATA);
        osMeta.close();
      } catch (FileNotFoundException ne) {
        throw new IOException(ne);
      }
    }

    private int writeFile() throws IOException {
      int ret = 1;
      int readed = 0;
      RandomAccessFile fos = null;
      FileChannel fc = null;
      if (sc.isConnected()) {
        try {
          InputStream is = sc.socket().getInputStream();
          DataInputStream dis = new DataInputStream(is);
          long tempBlockID = dis.readLong();
          long tempBlockGS = dis.readLong();
          blockID = tempBlockID > 0 ? tempBlockID : -tempBlockID;
          blockGS = tempBlockGS > 0 ? tempBlockGS : -tempBlockGS;

          block = new ExtendedBlock(blockPoolID, blockID, blockSize, blockGS);

          blockTempFile = new File(blockTempDirs[volIndex].getPath(), "blk_" + blockID);

          if (tempBlockID < 0) {
            byte[] fn;
            if (tempBlockGS >= 0) {
              fn = blockTempFile.getAbsolutePath().getBytes("UTF-8");
            } else {
              String fnPath;
              try {
                BlockLocalPathInfo localPathInfo = fsDataset.getBlockLocalPathInfo(block);
                fnPath = localPathInfo.getBlockPath();
              } catch (IOException e) {
                fnPath = "";
              }
              fn = fnPath.getBytes("UTF-8");
              ret = 0;
            }
            ByteBuffer len = ByteBuffer.allocate(4).putInt(fn.length);
            len.flip();
            while(len.hasRemaining()){
              sc.write(len);
            }
            ByteBuffer pathBuffer = ByteBuffer.wrap(fn);
            while(pathBuffer.hasRemaining()){
              sc.write(pathBuffer);
            }

            dataNode.notifyNamenodeReceivingBlock(block, storageIDs[volIndex]);

            len.flip();
            sc.read(len);
            sc.close();
            return ret;
          }

          dataNode.notifyNamenodeReceivingBlock(block, storageIDs[volIndex]);

          File file = blockTempFile;
          fos = new RandomAccessFile(file, "rw");
          fc = fos.getChannel();
          //fc = FileChannel.open(filePath, EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
          //LOG.debug("[SCW] Writing file " + file + " ...");

//          long read_time = 0L;
//          long write_time = 0L;
          while (true) {
            readed = 0;
//            long begin = Time.monotonicNow();
            while (bb.hasRemaining() && readed >= 0) {
              readed = sc.read(bb);
            }
//            read_time += Time.monotonicNow() - begin;

            bb.flip();

            //dataLen += bb.remaining();

//            begin = Time.monotonicNow();
            while (bb.hasRemaining()) {
              dataLen += fc.write(bb);
            }
//            write_time+=Time.monotonicNow()- begin;

            if (readed < 0) {
              break;
            }
            bb.flip();
          }

          fos.close();
          sc.close();
//          LOG.info("File read time is:"+read_time);
//          LOG.info("File write time is:"+write_time);
//          LOG.info("[SCW] Write file " + file + " finished with " + dataLen + " bytes!");
          return ret;
        } catch (IOException e) {
          LOG.error("[SCW] Write file " + blockID + " " + blockGS + " " + dataLen, e);
          //e.printStackTrace();
          throw e;
        }
      }
      return 0;
    }
  }
}
