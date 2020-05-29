/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.source.taildir;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.stream.JsonReader;
import org.apache.commons.io.IOUtils;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.client.avro.ReliableEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReliableTaildirEventReader implements ReliableEventReader {
  private static final Logger logger = LoggerFactory.getLogger(ReliableTaildirEventReader.class);

  private final List<TaildirMatcher> taildirCache;
  private final Table<String, String, String> headerTable;

  private TailFile currentFile = null;
  private Map<Long, TailFile> tailFiles = Maps.newHashMap();
  private long updateTime;
  private boolean addByteOffset;
  private boolean addLineNum;
  private String bodyPrefix;
  private String bodyPostfix;
  private boolean cachePatternMatching;
  private boolean committed = true;
  private final boolean annotateFileName;
  private final String fileNameHeader;

  /**
   * Create a ReliableTaildirEventReader to watch the given directory.
   */
  private ReliableTaildirEventReader(Map<String, String> filePaths,
      Table<String, String, String> headerTable, String positionFilePath,
      boolean skipToEnd, boolean addByteOffset, boolean cachePatternMatching,
      boolean annotateFileName, String fileNameHeader, boolean addLineNum, String bodyPrefix, String bodyPostfix) throws IOException {
    // Sanity checks
    Preconditions.checkNotNull(filePaths);
    Preconditions.checkNotNull(positionFilePath);

    if (logger.isDebugEnabled()) {
      logger.debug("Initializing {} with directory={}, metaDir={}",
          new Object[] { ReliableTaildirEventReader.class.getSimpleName(), filePaths });
    }

    List<TaildirMatcher> taildirCache = Lists.newArrayList();
    for (Entry<String, String> e : filePaths.entrySet()) {
      taildirCache.add(new TaildirMatcher(e.getKey(), e.getValue(), cachePatternMatching));
    }
    logger.info("taildirCache: " + taildirCache.toString());
    logger.info("headerTable: " + headerTable.toString());

    this.taildirCache = taildirCache;
    this.headerTable = headerTable;
    this.addByteOffset = addByteOffset;
    this.cachePatternMatching = cachePatternMatching;
    this.annotateFileName = annotateFileName;
    this.fileNameHeader = fileNameHeader;
    this.addLineNum = addLineNum;
    this.bodyPostfix = bodyPostfix;
    this.bodyPrefix = bodyPrefix;
    this.addLineNum = addLineNum;
    updateTailFiles(skipToEnd);

    logger.info("Updating position from position file: " + positionFilePath);
    loadPositionFile(positionFilePath);
  }

  /**
   * Load a position file which has the last read position of each file.
   * If the position file exists, update tailFiles mapping.
   */
  public void loadPositionFile(String filePath) {
    Long inode, pos, filesize, lineNum, lastUpdateTime;
    String path;
    FileReader fr = null;
    JsonReader jr = null;
    try {
      fr = new FileReader(filePath);
      jr = new JsonReader(fr);
      jr.beginArray();
      while (jr.hasNext()) {
        inode = null;
        path = null;
        filesize = null;
        lineNum = null;
        lastUpdateTime = null;
        pos = null;
        jr.beginObject();
        while (jr.hasNext()) {
          switch (jr.nextName()) {
            case "inode":
              inode = jr.nextLong();
              break;
            case "file":
              path = jr.nextString();
              break;
            case "pos":
              pos = jr.nextLong();
              break;
            case "filesize":
              filesize = jr.nextLong();
              break;
            case "lineNum":
              lineNum = jr.nextLong();
              break;
            case "lastUpdateTime":
              lastUpdateTime = jr.nextLong();
              break;
          }
        }
        jr.endObject();

        for (Object v : Arrays.asList(inode, pos, path, filesize,lineNum,lastUpdateTime)) {
          Preconditions.checkNotNull(v, "Detected missing value in position file. "
              + "inode: " + inode + ", pos: " + pos + ", path: " + path + ", filesize: " + filesize + ", lineNum: " + lineNum + ", lastUpdateTime: " + lastUpdateTime);
        }
        TailFile tf = tailFiles.get(inode);
        //判断如果tf不为空并且tf与f不一致。如果名称修改了，会造成inode pos数据不更新
        //if (tf != null && tf.updatePos(path, inode, pos)) {
        //改为如果tf不为空，这里将tf与f保持一致（都传入tf，所以肯定一致）
        if (tf != null && tf.updatePos(tf.getPath(), inode, pos, lineNum)) {
          tailFiles.put(inode, tf);
        } else {
          logger.info("Missing file: " + path + ", inode: " + inode + ", pos: " + pos + ", lineNum: " + lineNum);
        }
      }
      jr.endArray();
    } catch (FileNotFoundException e) {
      logger.info("File not found: " + filePath + ", not updating position");
    } catch (IOException e) {
      logger.error("Failed loading positionFile: " + filePath, e);
    } finally {
      try {
        if (fr != null) fr.close();
        if (jr != null) jr.close();
      } catch (IOException e) {
        logger.error("Error: " + e.getMessage(), e);
      }
    }
  }

  public Map<Long, TailFile> getTailFiles() {
    return tailFiles;
  }

  public void setCurrentFile(TailFile currentFile) {
    this.currentFile = currentFile;
  }

  @Override
  public Event readEvent() throws IOException {
    List<Event> events = readEvents(1);
    if (events.isEmpty()) {
      return null;
    }
    return events.get(0);
  }

  @Override
  public List<Event> readEvents(int numEvents) throws IOException {
    return readEvents(numEvents, false);
  }

  @VisibleForTesting
  public List<Event> readEvents(TailFile tf, int numEvents) throws IOException {
    setCurrentFile(tf);
    return readEvents(numEvents, true);
  }

  public List<Event> readEvents(int numEvents, boolean backoffWithoutNL)
      throws IOException {
    if (!committed) {
      if (currentFile == null) {
        throw new IllegalStateException("current file does not exist. " + currentFile.getPath());
      }
      logger.info("Last read was never committed - resetting position");
      long lastPos = currentFile.getPos();
      currentFile.updateFilePos(lastPos);
    }
    List<Event> events = currentFile.readEvents(numEvents, backoffWithoutNL, addByteOffset, addLineNum,bodyPrefix,bodyPostfix);
    if (events.isEmpty()) {
      return events;
    }

    //将header写入event head
    Map<String, String> headers = currentFile.getHeaders();
    if (annotateFileName || (headers != null && !headers.isEmpty())) {
      for (Event event : events) {
        if (headers != null && !headers.isEmpty()) {
          event.getHeaders().putAll(headers);
        }
        if (annotateFileName) {
          event.getHeaders().put(fileNameHeader, currentFile.getPath());
        }
      }
    }
    committed = false;
    return events;
  }

  @Override
  public void close() throws IOException {
    for (TailFile tf : tailFiles.values()) {
      if (tf.getRaf() != null) tf.getRaf().close();
    }
  }

  /** Commit the last lines which were read. */
  @Override
  public void commit() throws IOException {
    if (!committed && currentFile != null) {
      long pos = currentFile.getLineReadPos();
      long lineNum = currentFile.getLineNum();
      currentFile.setPos(pos);
      currentFile.setLineNum(lineNum);
      currentFile.setLastUpdated(updateTime);
      committed = true;
    }
  }

  /**
   * Update tailFiles mapping if a new file is created or appends are detected
   * to the existing file.
   */
  public List<Long> updateTailFiles(boolean skipToEnd) throws IOException {
    updateTime = System.currentTimeMillis();
    List<Long> updatedInodes = Lists.newArrayList();

    for (TaildirMatcher taildir : taildirCache) {
      Map<String, String> headers = headerTable.row(taildir.getFileGroup());

      for (File f : taildir.getMatchingFiles()) {
        long inode;
        try {
          inode = getInode(f);
        } catch (NoSuchFileException e) {
          logger.info("File has been deleted in the meantime: " + e.getMessage());
          continue;
        }
        TailFile tf = tailFiles.get(inode);
        //此处代码有问题->如果文件被重命名,而且命名后仍然符合正则,将会被重新读取.
        //if (tf == null || !tf.getPath().equals(f.getAbsolutePath())) {
        //将!tf.getPath().equals(f.getAbsolutePath())这部分判断去掉即可,inode已经能唯一标识一个文件
        if (tf == null) {
          long startPos = skipToEnd ? f.length() : 0;
          long startLineNum = skipToEnd ? f.length() : 0;
          tf = openFile(f, headers, inode, startPos, startLineNum);
        } else {
          boolean updated = tf.getLastUpdated() < f.lastModified() || tf.getPos() != f.length();//文件重命名后,如果没有对文件修改,f.lastModified时间不会变(即重命名操作不能影响Modify时间,影响的是Change时间)
          if (updated) {
            //如果文件被重命名,那么重新打开重命名后的文件,按照之前记录的位置继续读取
            if (!tf.getPath().equals(f.getAbsolutePath())) {
              if (f.length() > tf.getPos()) {
                tf = openFile(f, headers, inode, tf.getPos(),tf.getLineNum());
              }
            }else {
              /**文件更新后,如果之前文件由于idle被关闭{@link TaildirSource#closeTailFiles()},那么重新打开文件*/
              if (tf.getRaf() == null) {
                tf = openFile(f, headers, inode, tf.getPos(), tf.getLineNum());
              }
              //如果原始文件中已经采集的部分行被删除 将导致f.length() < tf.getPos(),从头开始读取
              if (f.length() < tf.getPos()) {
                logger.info("Pos " + tf.getPos() + " is larger than file size! "
                        + "Restarting from pos 0, file: " + tf.getPath() + ", inode: " + inode);
                tf.updatePos(tf.getPath(), inode, 0, 0);
              }
            }
          }
          tf.setNeedTail(updated);
        }
        tailFiles.put(inode, tf);
        updatedInodes.add(inode);
      }
    }
    return updatedInodes;
  }

  public List<Long> updateTailFiles() throws IOException {
    return updateTailFiles(false);
  }

    /**
     * 得到文件inode,不同jdk版本采用不同方式
     */
  private long getInode(File file) throws IOException {
    if (!file.exists())
        return 0;
    String javaVersion = System.getProperty("java.version");
    if (javaVersion.startsWith("1.6"))
        return getInode_16(file);
    else
        return getInode_17(file);
  }
    /**
     * 得到文件的inode,通过底层api得到 在jdk1.7以上的情况下使用
     */
  private long getInode_17(File file) throws IOException {
    long inode = (long) Files.getAttribute(file.toPath(), "unix:ino");
    return inode;
  }

    /**
     * 得到文件的inode,通过stat命令得到 在jdk1.6的情况下使用
     */
  public  long getInode_16(File file) throws IOException {
    ProcessBuilder pb = new ProcessBuilder("sh", "-c", "stat " + file.toPath());
    Process proc = pb.start();
    InputStreamReader input = new InputStreamReader(proc.getInputStream());
    BufferedReader br = new BufferedReader(input);
    String str;
    StringBuilder sb = new StringBuilder();
    long inode = 0;
    while ((str = br.readLine()) != null) {
        int start = 0;
        start = str.toLowerCase().indexOf("inode");
        if (start != -1) {
            for (int i = start + 5; i < str.length(); i++) {
                if (str.charAt(i) >= 'a' && str.charAt(i) <= 'z')
                    break;
                if (str.charAt(i) >= '0' && str.charAt(i) <= '9')
                    sb.append(str.charAt(i));
            }
            inode = Long.parseLong(sb.toString().trim());
            break;
        }
    }
    IOUtils.closeQuietly(proc.getInputStream());
    IOUtils.closeQuietly(proc.getOutputStream());
    IOUtils.closeQuietly(proc.getErrorStream());
    proc.destroy();
    input.close();
    br.close();
    return inode;
  }

  private TailFile openFile(File file, Map<String, String> headers, long inode, long pos, long linenum) {
    try {
      logger.info("Opening file: " + file + ", inode: " + inode + ", pos: " + pos);
      return new TailFile(file, headers, inode, pos, linenum);
    } catch (IOException e) {
      throw new FlumeException("Failed opening file: " + file, e);
    }
  }

  /**
   * Special builder class for ReliableTaildirEventReader
   */
  public static class Builder {
    private Map<String, String> filePaths;
    private Table<String, String, String> headerTable;
    private String positionFilePath;
    private boolean skipToEnd;
    private boolean addByteOffset;
    private boolean addLineNum;
    private String bodyPrefix;
    private String bodyPostfix;
    private boolean cachePatternMatching;
    private Boolean annotateFileName =
            TaildirSourceConfigurationConstants.DEFAULT_FILE_HEADER;
    private String fileNameHeader =
            TaildirSourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;

    public Builder filePaths(Map<String, String> filePaths) {
      this.filePaths = filePaths;
      return this;
    }

    public Builder headerTable(Table<String, String, String> headerTable) {
      this.headerTable = headerTable;
      return this;
    }

    public Builder positionFilePath(String positionFilePath) {
      this.positionFilePath = positionFilePath;
      return this;
    }

    public Builder skipToEnd(boolean skipToEnd) {
      this.skipToEnd = skipToEnd;
      return this;
    }

    public Builder addByteOffset(boolean addByteOffset) {
      this.addByteOffset = addByteOffset;
      return this;
    }

    public Builder addLineNum(boolean addLineNum) {
      this.addLineNum = addLineNum;
      return this;
    }

    public Builder cachePatternMatching(boolean cachePatternMatching) {
      this.cachePatternMatching = cachePatternMatching;
      return this;
    }

    public Builder annotateFileName(boolean annotateFileName) {
      this.annotateFileName = annotateFileName;
      return this;
    }

    public Builder fileNameHeader(String fileNameHeader) {
      this.fileNameHeader = fileNameHeader;
      return this;
    }

    public Builder bodyPrefix(String bodyPrefix) {
      this.bodyPrefix = bodyPrefix;
      return this;
    }

    public Builder bodyPostfix(String bodyPostfix) {
      this.bodyPostfix = bodyPostfix;
      return this;
    }

    public ReliableTaildirEventReader build() throws IOException {
      return new ReliableTaildirEventReader(filePaths, headerTable, positionFilePath, skipToEnd,
                                            addByteOffset, cachePatternMatching,
                                            annotateFileName, fileNameHeader, addLineNum,bodyPrefix,bodyPostfix);
    }
  }

}
