package com.github

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.NonPartitionedExtractor
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

/**
 * An example to demo how Glue read and write hudi dataset, and also sync metadata to Glue Catalog.
 *
 * @author Laurence Geng (https://laurence.blog.csdn.net/)
 */
object GlueHudiReadWriteExample {

  case class User(id: Long, name: String, age: Int, updatedTime: Long)

  val userTableName = "user"
  val userRecordKeyField = "id"
  val userPrecombineField = "updatedTime"

  var bucketName:String = _
  var userTablePath:String = _

  var spark: SparkSession = _

  def main(sysArgs: Array[String]): Unit = {

    init(sysArgs)

    val sparkImplicits = spark.implicits
    import sparkImplicits._

    // Step 1: build a dataframe with 2 user records, then write as
    // hudi format, but won't create table in glue catalog
    val users1 = Seq(
      User(1, "Tom", 24, System.currentTimeMillis()),
      User(2, "Bill", 32, System.currentTimeMillis())
    )
    val dataframe1 = users1.toDF
    saveUserAsHudiWithoutHiveTableSync(dataframe1)

    // Step 2: read just saved hudi dataset, and print each records
    val dataframe2 = readUserFromHudi()
    val users2 = dataframe2.as[User].collect().toSeq
    println("printing user records in dataframe2...")
    users2.foreach(println(_))

    // Step 3: append 2 new user records, one is updating Bill's age from 32 to 33,
    // the other is a new user whose name is 'Rose'. This time, we will enable
    // hudi hive syncing function, and a table named `user` will be created on
    // default database, this action is done by hudi automatically based on
    // the metadata of hudi user dataset.
    val users3 = users2 ++ Seq(
      User(2, "Bill", 33, System.currentTimeMillis()),
      User(3, "Rose", 45, System.currentTimeMillis())
    )
    val dataframe3 = users3.toDF
    saveUserAsHudiWithHiveTableSync(dataframe3)

    // Step 4: since a table is created automatically, now, we can query user table
    // immediately, and print returned user records, printed messages should show:
    // Bill's is updated, Rose's record is inserted, this demoed UPSERT feature of hudi!
    val dataframe4 = spark.sql("select * from user")
    val users4 = dataframe4.as[User].collect().toSeq
    println("printing user records in dataframe4...")
    users4.foreach(println(_))

    commit()
  }

  /**
   * 1. Parse job params
   * 2. Create SparkSession instance with given configs
   * 3. Init glue job
   *
   * @param sysArgs all params passing from main method
   */
  def init(sysArgs: Array[String]): Unit = {
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "bucketName").toArray)
    bucketName = args("bucketName")
    println(s"bucketName=$bucketName")
    userTablePath = s"s3://$bucketName/$userTableName"
    println(s"userTablePath=$userTablePath")
    val conf = new SparkConf()
    // This is required for hudi
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = new SparkContext(conf)
    val glueContext = new GlueContext(sparkContext)
    spark = glueContext.getSparkSession
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
  }

  /**
   * Commit glue job.
   */
  def commit(): Unit = {
    Job.commit()
  }

  /**
   * Read user records from Hudi, and return a dataframe.
   *
   * @return The dataframe of user records
   */
  def readUserFromHudi(): DataFrame = {
    spark
      .read
      .format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(userTablePath)
  }

  /**
   * Save a user dataframe as hudi dataset, but WON'T SYNC its metadata to glue catalog,
   * In other words, no table will be created after saving.
   *
   * @param dataframe The dataframe to be saved
   */
  def saveUserAsHudiWithoutHiveTableSync(dataframe: DataFrame) = {

    val hudiOptions = Map[String, String](
      HoodieWriteConfig.TABLE_NAME -> userTableName,
      DataSourceWriteOptions.OPERATION_OPT_KEY -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      DataSourceWriteOptions.TABLE_TYPE_OPT_KEY -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> userRecordKeyField,
      DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> userPrecombineField,
      DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY -> classOf[NonpartitionedKeyGenerator].getName
    )

    dataframe
      .write
      .format("hudi")
      .options(hudiOptions)
      .mode(SaveMode.Append)
      .save(userTablePath)
  }

  /**
   * Save a user dataframe as hudi dataset, but also SYNC its metadata to glue catalog,
   * In other words, after saving, a table named `default.user` will be created automatically by hudi hive sync
   * tool on Glue Catalog!
   *
   * @param dataframe The dataframe to be saved
   */
  def saveUserAsHudiWithHiveTableSync(dataframe: DataFrame) = {

    val hudiOptions = Map[String, String](
      HoodieWriteConfig.TABLE_NAME -> userTableName,
      DataSourceWriteOptions.OPERATION_OPT_KEY -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      DataSourceWriteOptions.TABLE_TYPE_OPT_KEY -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> userRecordKeyField,
      DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> userPrecombineField,
      DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY -> classOf[NonpartitionedKeyGenerator].getName,
      DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> classOf[NonPartitionedExtractor].getName,
      // Register hudi dataset as hive table (sync meta data)
      DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY -> "true",
      DataSourceWriteOptions.HIVE_USE_JDBC_OPT_KEY -> "false", // For glue, it is required to disable sync via hive jdbc!
      DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY -> "default",
      DataSourceWriteOptions.HIVE_TABLE_OPT_KEY -> userTableName
    )

    dataframe
      .write
      .format("hudi")
      .options(hudiOptions)
      .mode(SaveMode.Append)
      .save(userTablePath)
  }


}
