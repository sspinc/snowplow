/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.spark

// Java
import java.io.File
import java.net.URI

// commons
import org.apache.commons.io.{FileUtils => FU}

// Scalaz
import scalaz._
import Scalaz._

// Hadoop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.filecache.DistributedCache
import org.apache.hadoop.fs.{FileSystem, Path}

/** Helpers for caching files in HDFS. */
object FileUtils {
  /**
   * Helper to source a hosted asset:
   *   - download it to hdfs if it's on http/https
   *   - do nothing if it's on s3/hdfs
   *   - fail otherwise
   * @param conf Hadoop configuration
   * @param fileURI The hosted asset's URI
   * @return The source URI readable from HDFS boxed in a Scalaz Validation
   */
  def sourceFile(conf: Configuration, fileURI: URI): Validation[String, URI] =
    fileURI.getScheme match {
      case "http" | "https" => downloadToHDFS(conf, fileURI).toUri.success
      case "s3" | "s3n" | "s3a" | "hdfs" => fileURI.success
      case s => s"Scheme $s for file $fileURI not supported".fail
    }

  /**
   * Add a file to Haoop's DistributedCache as a symbolic link.
   * @param conf Hadoop configuration
   * @param source The file to add to the DistributedCache
   * @param symlink Name of the symbolic link in the DistributedCache
   * @return The path to the symbolic link in the DistributedCache
   */
  def addToDistCache(conf: Configuration, source: URI, symlink: String): String = {
    val path = s"${source.toString()}#$symlink"
    DistributedCache.createSymlink(conf)
    DistributedCache.addCacheFile(new URI(path), conf)
    s"./$symlink"
  }

  /**
   * A helper to get the filename from a URI.
   * @param uri The URL to extract the filename from
   * @return The extracted filename
   */
  def extractFilenameFromURI(uri: URI): String = {
    val p = uri.getPath
    p.substring(p.lastIndexOf('/') + 1, p.length)
  }

  /**
   * Download a file to HDFS.
   * @param conf Hadop configuration
   * @param fileURI The URI of the asset to download
   * @return The path to the asset in HDFS
   */
  private def downloadToHDFS(conf: Configuration, fileURI: URI): Path = {
    val filename = extractFilenameFromURI(fileURI)
    val localFile = s"tmp/$filename"
    FU.copyURLToFile(fileURI.toURL, new File(localFile), 60000, 300000) // conn and read timeouts

    val fs = FileSystem.get(conf)
    val hdfsPath = new Path(s"hdfs:///cache/$filename")
    fs.copyFromLocalFile(true, true, new Path(localFile), hdfsPath) // del src and overwrite
    hdfsPath
  }
}
