/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flume.source.taildir;

import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.PollableSourceConstants;
import org.apache.flume.source.PollableSourceRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;

public class TaildirSource extends AbstractSource implements
    PollableSource, Configurable {

  private static final Logger logger = LoggerFactory.getLogger(TaildirSource.class);

  private Map<String, String> filePaths;
  private Table<String, String, String> headerTable;
  private int batchSize;
  private String positionFilePath;
  private boolean skipToEnd;
  private boolean addByteOffset;
  private boolean addLineNum;
  private String bodyPrefix;
  private String bodyPostfix;

  private SourceCounter sourceCounter;
  private ReliableTaildirEventReader reader;
  private ScheduledExecutorService idleFileChecker;
  private ScheduledExecutorService positionWriter;
  private int retryInterval = 1000;
  private int maxRetryInterval = 5000;
  private int idleTimeout;
  private int checkIdleInterval = 5000;
  private int writePosInitDelay = 5000;
  private int writePosInterval;
  private boolean cachePatternMatching;

  private List<Long> existingInodes = new CopyOnWriteArrayList<Long>();
  private List<Long> idleInodes = new CopyOnWriteArrayList<Long>();
  private Long backoffSleepIncrement;
  private Long maxBackOffSleepInterval;
  private boolean fileHeader;
  private String fileHeaderKey;
  private Long maxBatchCount;

  @Override
  public synchronized void start() {
    logger.info("{} TaildirSource source starting with directory: {}", getName(), filePaths);
    try {
      reader = new ReliableTaildirEventReader.Builder()
          .filePaths(filePaths)
          .headerTable(headerTable)
          .positionFilePath(positionFilePath)
          .skipToEnd(skipToEnd)
          .addByteOffset(addByteOffset)
          .addLineNum(addLineNum)
          .cachePatternMatching(cachePatternMatching)
          .annotateFileName(fileHeader)
          .fileNameHeader(fileHeaderKey)
          .bodyPrefix(bodyPrefix)
          .bodyPostfix(bodyPostfix)
          .build();
    } catch (IOException e) {
      throw new FlumeException("Error instantiating ReliableTaildirEventReader", e);
    }
    idleFileChecker = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("idleFileChecker").build());
    idleFileChecker.scheduleWithFixedDelay(new idleFileCheckerRunnable(),
        idleTimeout, checkIdleInterval, TimeUnit.MILLISECONDS);

    positionWriter = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("positionWriter").build());
    positionWriter.scheduleWithFixedDelay(new PositionWriterRunnable(),
        writePosInitDelay, writePosInterval, TimeUnit.MILLISECONDS);

    super.start();
    logger.debug("TaildirSource started");
    sourceCounter.start();
  }

  @Override
  public synchronized void stop() {
    try {
      super.stop();
      ExecutorService[] services = {idleFileChecker, positionWriter};
      for (ExecutorService service : services) {
        service.shutdown();
        if (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          service.shutdownNow();
        }
      }
      // write the last position
      writePosition();
      reader.close();
    } catch (InterruptedException e) {
      logger.info("Interrupted while awaiting termination", e);
    } catch (IOException e) {
      logger.info("Failed: " + e.getMessage(), e);
    }
    sourceCounter.stop();
    logger.info("Taildir source {} stopped. Metrics: {}", getName(), sourceCounter);
  }

  @Override
  public String toString() {
    return String.format("Taildir source: { positionFile: %s, skipToEnd: %s, "
        + "addByteOffset: %s, addLineNum: %s, idleTimeout: %s, writePosInterval: %s }",
        positionFilePath, skipToEnd, addByteOffset, addLineNum, idleTimeout, writePosInterval);
  }

  @Override
  public synchronized void configure(Context context) {
    String fileGroups = context.getString(FILE_GROUPS);
    Preconditions.checkState(fileGroups != null, "Missing param: " + FILE_GROUPS);

    filePaths = selectByKeys(context.getSubProperties(FILE_GROUPS_PREFIX),
                             fileGroups.split("\\s+"));
    Preconditions.checkState(!filePaths.isEmpty(),
        "Mapping for tailing files is empty or invalid: '" + FILE_GROUPS_PREFIX + "'");

    String homePath = System.getProperty("user.home").replace('\\', '/');
    positionFilePath = context.getString(POSITION_FILE, homePath + DEFAULT_POSITION_FILE);
    Path positionFile = Paths.get(positionFilePath);
    try {
      Files.createDirectories(positionFile.getParent());
    } catch (IOException e) {
      throw new FlumeException("Error creating positionFile parent directories", e);
    }
    headerTable = getTable(context, HEADERS_PREFIX);
    batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    skipToEnd = context.getBoolean(SKIP_TO_END, DEFAULT_SKIP_TO_END);
    addByteOffset = context.getBoolean(ADD_BYTE_OFFSET_BODY, DEFAULT_ADD_BYTE_OFFSET_BODY);
    addLineNum = context.getBoolean(ADD_LINE_NUMBER_BODY, DEFAULT_ADD_LINE_NUMBER_BODY);
    bodyPrefix = context.getString(BODY_PREFIX,DEFAULT_BODY_PREFIX);
    bodyPostfix = context.getString(BODY_POSTFIX,DEFAULT_BODY_POSTFIX);
    idleTimeout = context.getInteger(IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT);
    writePosInterval = context.getInteger(WRITE_POS_INTERVAL, DEFAULT_WRITE_POS_INTERVAL);
    cachePatternMatching = context.getBoolean(CACHE_PATTERN_MATCHING,
        DEFAULT_CACHE_PATTERN_MATCHING);

    backoffSleepIncrement = context.getLong(PollableSourceConstants.BACKOFF_SLEEP_INCREMENT,
        PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
    maxBackOffSleepInterval = context.getLong(PollableSourceConstants.MAX_BACKOFF_SLEEP,
        PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);
    fileHeader = context.getBoolean(FILENAME_HEADER,
            DEFAULT_FILE_HEADER);
    fileHeaderKey = context.getString(FILENAME_HEADER_KEY,
            DEFAULT_FILENAME_HEADER_KEY);
    maxBatchCount = context.getLong(MAX_BATCH_COUNT, DEFAULT_MAX_BATCH_COUNT);
    if (maxBatchCount <= 0) {
      maxBatchCount = DEFAULT_MAX_BATCH_COUNT;
      logger.warn("Invalid maxBatchCount specified, initializing source "
          + "default maxBatchCount of {}", maxBatchCount);
    }

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  private Map<String, String> selectByKeys(Map<String, String> map, String[] keys) {
    Map<String, String> result = Maps.newHashMap();
    for (String key : keys) {
      if (map.containsKey(key)) {
        result.put(key, map.get(key));
      }
    }
    return result;
  }

  private Table<String, String, String> getTable(Context context, String prefix) {
    Table<String, String, String> table = HashBasedTable.create();
    for (Entry<String, String> e : context.getSubProperties(prefix).entrySet()) {
      String[] parts = e.getKey().split("\\.", 2);
      table.put(parts[0], parts[1], e.getValue());
    }
    return table;
  }

  @VisibleForTesting
  protected SourceCounter getSourceCounter() {
    return sourceCounter;
  }

  /**
   * 由{@link PollableSourceRunner.PollingRunner#run()}-run()方法内循环调用source.process()
   * 根据process()方法返回的status决定调用process()方法的时延
   * 返回ready会立即再调用process() ;返回backoff会sleep一定时间再调用
   */
  @Override
  public Status process() {
    Status status = Status.BACKOFF;
    try {
      existingInodes.clear();
      //如果有新的文件产生,或者以前的文件有新数据,则将inode放入existingInodes中
      existingInodes.addAll(reader.updateTailFiles());
      for (long inode : existingInodes) {
        //根据inode拿到tf对象,tf负责具体的读写文件
        TailFile tf = reader.getTailFiles().get(inode);
        if (tf.needTail()) {
          //tailFileProcess->执行具体tail文件->封装event->发送到channel
          boolean hasMoreLines = tailFileProcess(tf, true);
          if (hasMoreLines) {
            status = Status.READY;
          }
        }
      }
      //此处调用close,并不是每次调用都会关闭.只有满足条件,才会关闭.
      closeTailFiles();
    } catch (Throwable t) {
      logger.error("Unable to tail files", t);
      //sourceCounter.incrementEventReadFail();
      status = Status.BACKOFF;
    }
    return status;
  }

  @Override
  public long getBackOffSleepIncrement() {
    return backoffSleepIncrement;
  }

  @Override
  public long getMaxBackOffSleepInterval() {
    return maxBackOffSleepInterval;
  }

  private boolean tailFileProcess(TailFile tf, boolean backoffWithoutNL)
      throws IOException, InterruptedException {
    long batchCount = 0;
    while (true) {
      reader.setCurrentFile(tf);
      List<Event> events = reader.readEvents(batchSize, backoffWithoutNL);
      if (events.isEmpty()) {
        return false;
      }
      sourceCounter.addToEventReceivedCount(events.size());
      sourceCounter.incrementAppendBatchReceivedCount();
      try {
        //processEventBatch里封装了put事务
        getChannelProcessor().processEventBatch(events);
        //提交tf相关的状态
        reader.commit();
      } catch (ChannelException ex) {
        logger.warn("The channel is full or unexpected failure. " +
            "The source will try again after " + retryInterval + " ms");
        sourceCounter.incrementChannelWriteFail();
        TimeUnit.MILLISECONDS.sleep(retryInterval);
        retryInterval = retryInterval << 1;
        retryInterval = Math.min(retryInterval, maxRetryInterval);
        continue;
      }
      retryInterval = 1000;
      sourceCounter.addToEventAcceptedCount(events.size());
      sourceCounter.incrementAppendBatchAcceptedCount();
      if (events.size() < batchSize) {
        logger.debug("The events taken from " + tf.getPath() + " is less than " + batchSize);
        return false;
      }
      //maxBatchCount->控制一次process()处理的批次数量,可间接控制tail多个文件时的交替时间
      if (++batchCount >= maxBatchCount) {
        logger.debug("The batches read from the same file is larger than " + maxBatchCount );
        return true;
      }
    }
  }

  private void closeTailFiles() throws IOException, InterruptedException {
    for (long inode : idleInodes) {
      TailFile tf = reader.getTailFiles().get(inode);
      if (tf.getRaf() != null) { // when file has not closed yet
        tailFileProcess(tf, false);
        tf.close();
        logger.info("Closed file: " + tf.getPath() + ", inode: " + inode + ", pos: " + tf.getPos());
      }
    }
    idleInodes.clear();
  }

  /**
   * Runnable class that checks whether there are files which should be closed.
   */
  private class idleFileCheckerRunnable implements Runnable {
    @Override
    public void run() {
      try {
        long now = System.currentTimeMillis();
        Collection<TailFile> tailFiles = reader.getTailFiles().values();
        int complateNum = 0;
        System.out.println("reader.getTailFiles() = " + reader.getTailFiles());
        //此处增加逻辑:1.判断flume当前传输是否正常 2.判断文件是否传完
        for (TailFile tf : tailFiles) {
          long timeDiff = now-tf.getLastUpdated();
          if (tf.getPos()==new File(tf.getPath()).length()){
            logger.info("当前服务器{}-文件{}在{}时间内未产生数据" ,getLocalIpByNetcard(),tf.getPath(),timeDiff);
            if (timeDiff>300000) {
              complateNum++;
            }
          }else {
            if (timeDiff<30000) {
              logger.info("当前服务器{}-文件{}-采集延时{}",getLocalIpByNetcard(),tf.getPath(),timeDiff);
            }
          }
          if (tf.getLastUpdated() + idleTimeout < now && tf.getRaf() != null) {
            idleInodes.add(tf.getInode());
          }
        }
        if (complateNum==tailFiles.size()){
          logger.info("当前服务器{}-监控文件组{}传输完成",getLocalIpByNetcard(),filePaths.toString());
        }
      } catch (Throwable t) {
        logger.error("Uncaught exception in IdleFileChecker thread", t);
        sourceCounter.incrementGenericProcessingFail();
      }
    }
  }

  /**
   * Runnable class that writes a position file which has the last read position
   * of each file.
   */
  private class PositionWriterRunnable implements Runnable {
    @Override
    public void run() {
      writePosition();
    }
  }

  private void writePosition() {
    File file = new File(positionFilePath);
    FileWriter writer = null;
    try {
      writer = new FileWriter(file);
      if (!existingInodes.isEmpty()) {
        String json = toPosInfoJson();
        writer.write(json);
      }
    } catch (Throwable t) {
      logger.error("Failed writing positionFile", t);
      sourceCounter.incrementGenericProcessingFail();
    } finally {
      try {
        if (writer != null) writer.close();
      } catch (IOException e) {
        logger.error("Error: " + e.getMessage(), e);
        sourceCounter.incrementGenericProcessingFail();
      }
    }
  }

  private String toPosInfoJson() {
    @SuppressWarnings("rawtypes")
    List<Map> posInfos = Lists.newArrayList();
    for (Long inode : existingInodes) {
      TailFile tf = reader.getTailFiles().get(inode);
      Long filesize = new File(tf.getPath()).length();
      ImmutableMap<Object, Object> map = ImmutableMap.builder()
              .put("inode", inode)
              .put("file", tf.getPath())
              .put("pos", tf.getPos())
              .put("filesize", filesize)
              .put("lineNum", tf.getLineNum())
              .put("lastUpdateTime", tf.getLastUpdated())
              .build();
      posInfos.add(map);

    }
    return new Gson().toJson(posInfos);
  }

  /**
   * 获取服务器IP
   */
  public static String getLocalIpByNetcard() {
    try {
      for (Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces(); e.hasMoreElements(); ) {
        NetworkInterface item = e.nextElement();
        for (InterfaceAddress address : item.getInterfaceAddresses()) {
          if (item.isLoopback() || !item.isUp()) {
            continue;
          }
          if (address.getAddress() instanceof Inet4Address) {
            Inet4Address inet4Address = (Inet4Address) address.getAddress();
            return inet4Address.getHostAddress();
          }
        }
      }
      return InetAddress.getLocalHost().getHostAddress();
    } catch (SocketException | UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }
}
