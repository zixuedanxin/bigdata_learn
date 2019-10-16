/*
# Program      : AppConstants.scala
# Date Created : 07/03/2018
# Description  : This specifies the different properties mentioned in configuration files
# Parameters   :
#
# Modification history:
#
# Date         Author               Description
# ===========  ===================  ============================================
# 07/03/2018  Anand Ayyasamy               Creation
# ===========  ===================  ============================================
*/
package com.struct.stream.utils

/**
 * This specifies the different properties mentiond in configuration files
 * in hdfs , whic is being reused.
 *
 */
object AppConstants {
  //To mention the streaming time for the application.
  final val STREAMING_TIME = "streaming.duration.seconds"
  /*checkpoint Location*/
  final val CHECKPOINT_LOC = "streaming.checkpoint.Location"

  final val STREAM_SQL = "streaming.sql.query"

  final val JSON_SCHEMA = "streaming.json.schema"

  final val HDFS_PATH = "hdfs.path"


}