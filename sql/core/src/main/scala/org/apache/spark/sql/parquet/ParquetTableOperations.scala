/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.parquet

import java.io.IOException
import java.lang.{Long => JLong}
import java.text.{NumberFormat, SimpleDateFormat}
import java.util.concurrent.{Callable, TimeUnit}
import java.util.{Date, List => JList}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

import com.google.common.cache.CacheBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{BlockLocation, FileStatus, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat => NewFileOutputFormat}
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.hadoop.metadata.GlobalMetaData
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.schema.MessageType

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, InternalRow, _}
import org.apache.spark.sql.execution.{LeafNode, SparkPlan, UnaryNode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Logging, TaskContext}
import org.apache.spark.util.SerializableConfiguration

/**
 * :: DeveloperApi ::
 * Parquet table scan operator. Imports the file that backs the given
 * [[org.apache.spark.sql.parquet.ParquetRelation]] as a ``RDD[InternalRow]``.
 */
private[sql] case class ParquetTableScan(
    attributes: Seq[Attribute],
    relation: ParquetRelation,
    columnPruningPred: Seq[Expression])
  extends LeafNode {

  // The resolution of Parquet attributes is case sensitive, so we resolve the original attributes
  // by exprId. note: output cannot be transient, see
  // https://issues.apache.org/jira/browse/SPARK-1367
  val output = attributes.map(relation.attributeMap)

  // A mapping of ordinals partitionRow -> finalOutput.
  val requestedPartitionOrdinals = {
    val partitionAttributeOrdinals = AttributeMap(relation.partitioningAttributes.zipWithIndex)

    attributes.zipWithIndex.flatMap {
      case (attribute, finalOrdinal) =>
        partitionAttributeOrdinals.get(attribute).map(_ -> finalOrdinal)
    }
  }.toArray

  protected override def doExecute(): RDD[InternalRow] = {
    import org.apache.parquet.filter2.compat.FilterCompat.FilterPredicateCompat

    val sc = sqlContext.sparkContext
    val job = new Job(sc.hadoopConfiguration)
    ParquetInputFormat.setReadSupportClass(job, classOf[RowReadSupport])

    val conf: Configuration = ContextUtil.getConfiguration(job)

    relation.path.split(",").foreach { curPath =>
      val qualifiedPath = {
        val path = new Path(curPath)
        path.getFileSystem(conf).makeQualified(path)
      }
      NewFileInputFormat.addInputPath(job, qualifiedPath)
    }

    // Store both requested and original schema in `Configuration`
    conf.set(
      RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      ParquetTypesConverter.convertToString(output))
    conf.set(
      RowWriteSupport.SPARK_ROW_SCHEMA,
      ParquetTypesConverter.convertToString(relation.output))

    // Store record filtering predicate in `Configuration`
    // Note 1: the input format ignores all predicates that cannot be expressed
    // as simple column predicate filters in Parquet. Here we just record
    // the whole pruning predicate.
    ParquetFilters
      .createRecordFilter(columnPruningPred)
      .map(_.asInstanceOf[FilterPredicateCompat].getFilterPredicate)
      // Set this in configuration of ParquetInputFormat, needed for RowGroupFiltering
      .foreach(ParquetInputFormat.setFilterPredicate(conf, _))

    // Tell FilteringParquetRowInputFormat whether it's okay to cache Parquet and FS metadata
    conf.setBoolean(
      SQLConf.PARQUET_CACHE_METADATA.key,
      sqlContext.getConf(SQLConf.PARQUET_CACHE_METADATA, true))

    // Use task side metadata in parquet
    conf.setBoolean(ParquetInputFormat.TASK_SIDE_METADATA, true)

    val baseRDD =
      new org.apache.spark.rdd.NewHadoopRDD(
        sc,
        classOf[FilteringParquetRowInputFormat],
        classOf[Void],
        classOf[InternalRow],
        conf)

    if (requestedPartitionOrdinals.nonEmpty) {
      // This check is based on CatalystConverter.createRootConverter.
      val primitiveRow = output.forall(a => ParquetTypesConverter.isPrimitiveType(a.dataType))

      // Uses temporary variable to avoid the whole `ParquetTableScan` object being captured into
      // the `mapPartitionsWithInputSplit` closure below.
      val outputSize = output.size

      baseRDD.mapPartitionsWithInputSplit { case (split, iter) =>
        val partValue = "([^=]+)=([^=]+)".r
        val partValues =
          split.asInstanceOf[org.apache.parquet.hadoop.ParquetInputSplit]
            .getPath
            .toString
            .split("/")
            .flatMap {
              case partValue(key, value) => Some(key -> value)
              case _ => None
            }.toMap

        // Convert the partitioning attributes into the correct types
        val partitionRowValues =
          relation.partitioningAttributes
            .map(a => Cast(Literal(partValues(a.name)), a.dataType).eval(EmptyRow))

        if (primitiveRow) {
          new Iterator[InternalRow] {
            def hasNext: Boolean = iter.hasNext
            def next(): InternalRow = {
              // We are using CatalystPrimitiveRowConverter and it returns a SpecificMutableRow.
              val row = iter.next()._2.asInstanceOf[SpecificMutableRow]

              // Parquet will leave partitioning columns empty, so we fill them in here.
              var i = 0
              while (i < requestedPartitionOrdinals.size) {
                row(requestedPartitionOrdinals(i)._2) =
                  partitionRowValues(requestedPartitionOrdinals(i)._1)
                i += 1
              }
              row
            }
          }
        } else {
          // Create a mutable row since we need to fill in values from partition columns.
          val mutableRow = new GenericMutableRow(outputSize)
          new Iterator[InternalRow] {
            def hasNext: Boolean = iter.hasNext
            def next(): InternalRow = {
              // We are using CatalystGroupConverter and it returns a GenericRow.
              // Since GenericRow is not mutable, we just cast it to a Row.
              val row = iter.next()._2.asInstanceOf[InternalRow]

              var i = 0
              while (i < row.size) {
                mutableRow(i) = row(i)
                i += 1
              }
              // Parquet will leave partitioning columns empty, so we fill them in here.
              i = 0
              while (i < requestedPartitionOrdinals.size) {
                mutableRow(requestedPartitionOrdinals(i)._2) =
                  partitionRowValues(requestedPartitionOrdinals(i)._1)
                i += 1
              }
              mutableRow
            }
          }
        }
      }
    } else {
      baseRDD.map(_._2)
    }
  }

  /**
   * Applies a (candidate) projection.
   *
   * @param prunedAttributes The list of attributes to be used in the projection.
   * @return Pruned TableScan.
   */
  def pruneColumns(prunedAttributes: Seq[Attribute]): ParquetTableScan = {
    val success = validateProjection(prunedAttributes)
    if (success) {
      ParquetTableScan(prunedAttributes, relation, columnPruningPred)
    } else {
      sys.error("Warning: Could not validate Parquet schema projection in pruneColumns")
    }
  }

  /**
   * Evaluates a candidate projection by checking whether the candidate is a subtype
   * of the original type.
   *
   * @param projection The candidate projection.
   * @return True if the projection is valid, false otherwise.
   */
  private def validateProjection(projection: Seq[Attribute]): Boolean = {
    val original: MessageType = relation.parquetSchema
    val candidate: MessageType = ParquetTypesConverter.convertFromAttributes(projection)
    Try(original.checkContains(candidate)).isSuccess
  }
}

/**
 * :: DeveloperApi ::
 * Operator that acts as a sink for queries on RDDs and can be used to
 * store the output inside a directory of Parquet files. This operator
 * is similar to Hive's INSERT INTO TABLE operation in the sense that
 * one can choose to either overwrite or append to a directory. Note
 * that consecutive insertions to the same table must have compatible
 * (source) schemas.
 *
 * WARNING: EXPERIMENTAL! InsertIntoParquetTable with overwrite=false may
 * cause data corruption in the case that multiple users try to append to
 * the same table simultaneously. Inserting into a table that was
 * previously generated by other means (e.g., by creating an HDFS
 * directory and importing Parquet files generated by other tools) may
 * cause unpredicted behaviour and therefore results in a RuntimeException
 * (only detected via filename pattern so will not catch all cases).
 */
@DeveloperApi
private[sql] case class InsertIntoParquetTable(
    relation: ParquetRelation,
    child: SparkPlan,
    overwrite: Boolean = false)
  extends UnaryNode with SparkHadoopMapReduceUtil {

  /**
   * Inserts all rows into the Parquet file.
   */
  protected override def doExecute(): RDD[InternalRow] = {
    // TODO: currently we do not check whether the "schema"s are compatible
    // That means if one first creates a table and then INSERTs data with
    // and incompatible schema the execution will fail. It would be nice
    // to catch this early one, maybe having the planner validate the schema
    // before calling execute().

    val childRdd = child.execute()
    assert(childRdd != null)

    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)

    val writeSupport =
      if (child.output.map(_.dataType).forall(ParquetTypesConverter.isPrimitiveType)) {
        log.debug("Initializing MutableRowWriteSupport")
        classOf[org.apache.spark.sql.parquet.MutableRowWriteSupport]
      } else {
        classOf[org.apache.spark.sql.parquet.RowWriteSupport]
      }

    ParquetOutputFormat.setWriteSupportClass(job, writeSupport)

    val conf = ContextUtil.getConfiguration(job)
    // This is a hack. We always set nullable/containsNull/valueContainsNull to true
    // for the schema of a parquet data.
    val schema = StructType.fromAttributes(relation.output).asNullable
    RowWriteSupport.setSchema(schema.toAttributes, conf)

    val fspath = new Path(relation.path)
    val fs = fspath.getFileSystem(conf)

    if (overwrite) {
      try {
        fs.delete(fspath, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${fspath.toString} prior"
              + s" to InsertIntoParquetTable:\n${e.toString}")
      }
    }
    saveAsHadoopFile(childRdd, relation.path.toString, conf)

    // We return the child RDD to allow chaining (alternatively, one could return nothing).
    childRdd
  }

  override def output: Seq[Attribute] = child.output

  /**
   * Stores the given Row RDD as a Hadoop file.
   *
   * Note: We cannot use ``saveAsNewAPIHadoopFile`` from [[org.apache.spark.rdd.PairRDDFunctions]]
   * together with [[org.apache.spark.util.MutablePair]] because ``PairRDDFunctions`` uses
   * ``Tuple2`` and not ``Product2``. Also, we want to allow appending files to an existing
   * directory and need to determine which was the largest written file index before starting to
   * write.
   *
   * @param rdd The [[org.apache.spark.rdd.RDD]] to writer
   * @param path The directory to write to.
   * @param conf A [[org.apache.hadoop.conf.Configuration]].
   */
  private def saveAsHadoopFile(
      rdd: RDD[InternalRow],
      path: String,
      conf: Configuration) {
    val job = new Job(conf)
    val keyType = classOf[Void]
    job.setOutputKeyClass(keyType)
    job.setOutputValueClass(classOf[InternalRow])
    NewFileOutputFormat.setOutputPath(job, new Path(path))
    val wrappedConf = new SerializableConfiguration(job.getConfiguration)
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = sqlContext.sparkContext.newRddId()

    val taskIdOffset =
      if (overwrite) {
        1
      } else {
        FileSystemHelper
          .findMaxTaskId(NewFileOutputFormat.getOutputPath(job).toString, job.getConfiguration) + 1
      }

    def writeShard(context: TaskContext, iter: Iterator[InternalRow]): Int = {
      /* "reduce task" <split #> <attempt # = spark task #> */
      val attemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = false, context.partitionId,
        context.attemptNumber)
      val hadoopContext = newTaskAttemptContext(wrappedConf.value, attemptId)
      val format = new AppendingParquetOutputFormat(taskIdOffset)
      val committer = format.getOutputCommitter(hadoopContext)
      committer.setupTask(hadoopContext)
      val writer = format.getRecordWriter(hadoopContext)
      try {
        while (iter.hasNext) {
          val row = iter.next()
          writer.write(null, row)
        }
      } finally {
        writer.close(hadoopContext)
      }
      SparkHadoopMapRedUtil.commitTask(committer, hadoopContext, context)
      1
    }
    val jobFormat = new AppendingParquetOutputFormat(taskIdOffset)
    /* apparently we need a TaskAttemptID to construct an OutputCommitter;
     * however we're only going to use this local OutputCommitter for
     * setupJob/commitJob, so we just use a dummy "map" task.
     */
    val jobAttemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = true, 0, 0)
    val jobTaskContext = newTaskAttemptContext(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    sqlContext.sparkContext.runJob(rdd, writeShard _)
    jobCommitter.commitJob(jobTaskContext)
  }
}

/**
 * TODO: this will be able to append to directories it created itself, not necessarily
 * to imported ones.
 */
private[parquet] class AppendingParquetOutputFormat(offset: Int)
  extends org.apache.parquet.hadoop.ParquetOutputFormat[InternalRow] {
  // override to accept existing directories as valid output directory
  override def checkOutputSpecs(job: JobContext): Unit = {}
  var committer: OutputCommitter = null

  // override to choose output filename so not overwrite existing ones
  override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
    val numfmt = NumberFormat.getInstance()
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)

    val taskId: TaskID = getTaskAttemptID(context).getTaskID
    val partition: Int = taskId.getId
    val filename = "part-r-" + numfmt.format(partition + offset) + ".parquet"
    val committer: FileOutputCommitter =
      getOutputCommitter(context).asInstanceOf[FileOutputCommitter]
    new Path(committer.getWorkPath, filename)
  }

  // The TaskAttemptContext is a class in hadoop-1 but is an interface in hadoop-2.
  // The signatures of the method TaskAttemptContext.getTaskAttemptID for the both versions
  // are the same, so the method calls are source-compatible but NOT binary-compatible because
  // the opcode of method call for class is INVOKEVIRTUAL and for interface is INVOKEINTERFACE.
  private def getTaskAttemptID(context: TaskAttemptContext): TaskAttemptID = {
    context.getClass.getMethod("getTaskAttemptID").invoke(context).asInstanceOf[TaskAttemptID]
  }

  // override to create output committer from configuration
  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    if (committer == null) {
      val output = getOutputPath(context)
      val cls = context.getConfiguration.getClass("spark.sql.parquet.output.committer.class",
        classOf[ParquetOutputCommitter], classOf[ParquetOutputCommitter])
      val ctor = cls.getDeclaredConstructor(classOf[Path], classOf[TaskAttemptContext])
      committer = ctor.newInstance(output, context).asInstanceOf[ParquetOutputCommitter]
    }
    committer
  }

  // FileOutputFormat.getOutputPath takes JobConf in hadoop-1 but JobContext in hadoop-2
  private def getOutputPath(context: TaskAttemptContext): Path = {
    context.getConfiguration().get("mapred.output.dir") match {
      case null => null
      case name => new Path(name)
    }
  }
}

/**
 * We extend ParquetInputFormat in order to have more control over which
 * RecordFilter we want to use.
 */
private[parquet] class FilteringParquetRowInputFormat
  extends org.apache.parquet.hadoop.ParquetInputFormat[InternalRow] with Logging {

  private var fileStatuses = Map.empty[Path, FileStatus]

  override def createRecordReader(
      inputSplit: InputSplit,
      taskAttemptContext: TaskAttemptContext): RecordReader[Void, InternalRow] = {

    import org.apache.parquet.filter2.compat.FilterCompat.NoOpFilter

    val readSupport: ReadSupport[InternalRow] = new RowReadSupport()

    val filter = ParquetInputFormat.getFilter(ContextUtil.getConfiguration(taskAttemptContext))
    if (!filter.isInstanceOf[NoOpFilter]) {
      new ParquetRecordReader[InternalRow](
        readSupport,
        filter)
    } else {
      new ParquetRecordReader[InternalRow](readSupport)
    }
  }

}

private[parquet] object FilteringParquetRowInputFormat {
  private val footerCache = CacheBuilder.newBuilder()
    .maximumSize(20000)
    .build[FileStatus, Footer]()

  private val blockLocationCache = CacheBuilder.newBuilder()
    .maximumSize(20000)
    .expireAfterWrite(15, TimeUnit.MINUTES)  // Expire locations since HDFS files might move
    .build[FileStatus, Array[BlockLocation]]()
}

private[parquet] object FileSystemHelper {
  def listFiles(pathStr: String, conf: Configuration): Seq[Path] = {
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"ParquetTableOperations: Path $origPath is incorrectly formatted")
    }
    val path = origPath.makeQualified(fs)
    if (!fs.exists(path) || !fs.getFileStatus(path).isDir) {
      throw new IllegalArgumentException(
        s"ParquetTableOperations: path $path does not exist or is not a directory")
    }
    fs.globStatus(path)
      .flatMap { status => if (status.isDir) fs.listStatus(status.getPath) else List(status) }
      .map(_.getPath)
  }

    /**
     * Finds the maximum taskid in the output file names at the given path.
     */
  def findMaxTaskId(pathStr: String, conf: Configuration): Int = {
    val files = FileSystemHelper.listFiles(pathStr, conf)
    // filename pattern is part-r-<int>.parquet
    val nameP = new scala.util.matching.Regex("""part-.-(\d{1,}).*""", "taskid")
    val hiddenFileP = new scala.util.matching.Regex("_.*")
    files.map(_.getName).map {
      case nameP(taskid) => taskid.toInt
      case hiddenFileP() => 0
      case other: String =>
        sys.error("ERROR: attempting to append to set of Parquet files and found file" +
          s"that does not match name pattern: $other")
      case _ => 0
    }.reduceOption(_ max _).getOrElse(0)
  }
}
