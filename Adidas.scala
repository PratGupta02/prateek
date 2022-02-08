package com.adidas

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level


object Adidas {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val json_file_path = "src\\input_data\\input.json"
  val output_file_path = "src\\input_data\\output"
  println(s"JSON Input file path: $json_file_path")
  val spark = SparkSession.builder.master("local").appName("Adidas").getOrCreate()


  def flattenDataframe(df: DataFrame): DataFrame = {
    //getting all the fields from schema
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    //length shows the number of fields inside dataframe
    for (i <- 0 until fields.length) {
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name

      fieldtype match {
        case arrayType: ArrayType =>
          val fieldName1 = fieldName
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName1)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName1) as $fieldName1")
          val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
          return flattenDataframe(explodedDf)

        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
          val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_").replace("$", "_").replace("__", "_").replace(" ", "").replace("-", ""))))
          val explodedf = df.select(renamedcols: _*)
          return flattenDataframe(explodedf)
        case _ =>
      }
    }
    df
  }


  def standardize_publish_date(df: DataFrame): DataFrame = {

    val stndrd_publsh_dt_df = df.withColumn("FormattedPublishDate",
      when(to_date(col("publish_date"), "MMMM dd, yyyy").isNotNull,
        to_date(col("publish_date"), "MMMM dd, yyyy"))
        .when(to_date(col("publish_date"), "yyyy").isNotNull,
          to_date(col("publish_date"), "yyyy"))
        .when(to_date(col("publish_date"), "MMMM, yyyy").isNotNull,
          to_date(col("publish_date"), "MMMM, yyyy"))
        .when(to_date(col("publish_date"), "MMMM yyyy").isNotNull,
          to_date(col("publish_date"), "MMMM yyyy"))
        .otherwise("Unknown Format"))

    extract_year_month_date(stndrd_publsh_dt_df)
  }

  def extract_year_month_date(stndrd_publsh_dt_df: DataFrame): DataFrame = {
    val stndrd_publsh_dt_df_new_cols = stndrd_publsh_dt_df.withColumn("stnd_publish_year", date_format(col("FormattedPublishDate"), "Y"))
      .withColumn("stnd_publish_month", date_format(col("FormattedPublishDate"), "M"))
      .withColumn("stnd_publish_month_nm", date_format(col("FormattedPublishDate"), "MMM"))
      .withColumn("stnd_publish_date", date_format(col("FormattedPublishDate"), "d"))

    stndrd_publsh_dt_df_new_cols
  }

  def flattenJsonData(json_df: DataFrame, solution_num: Int): (DataFrame, Long) = {

    var flattenedDF = spark.emptyDataFrame
    var standarize_publish_date_df = spark.emptyDataFrame
    val relevant_clmn_list = List("authors", "genres", "key", "title", "publish_date", "number_of_pages")

    if (solution_num == 1) {
      val limited_cols_df = json_df.select(relevant_clmn_list.map(x => col(x)): _*)
      standarize_publish_date_df = standardize_publish_date(limited_cols_df)
      flattenedDF = standarize_publish_date_df
        .withColumn("genres", explode_outer(col("genres")))
        .withColumn("authors_e", explode_outer(col("authors")))
        .withColumn("authors_key", col("authors_e.key"))
        .drop("authors", "authors_e")

    }
    else if (solution_num == 2) {
      var shorter_df = standardize_publish_date(json_df)
      val extra_col_list = List("excerpts", "identifiers", "links", "oclc_", "subject", "isbn_", "url", "uri", "table_of_contents", "dewey_", "work", "ia_")
      for (x_col <- extra_col_list) {
        shorter_df = shorter_df.drop(shorter_df.columns.filter(clmn => clmn.contains(x_col)): _*)
      }

      println("\n\nShorter File JSON Schema:")
      shorter_df.printSchema()
      println(s"\n\nTotal Number of Shorter DF Keys:${shorter_df.columns.size}")
      flattenedDF = flattenDataframe(shorter_df).cache
      val sel_clmn_list = List("authors_key", "genres", "key", "title", "publish_date", "number_of_pages", "stnd_publish_year", "stnd_publish_month_nm", "stnd_publish_month", "stnd_publish_date")
      flattenedDF = flattenedDF.select(sel_clmn_list.map(x => col(x)): _*).distinct
    }

    val flat_df_cnt = flattenedDF.count
    println(s"\n\nFlattenedDF Count:${flat_df_cnt}")
    //flattenedDF.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", "~").csv(output_file_path)
    (flattenedDF, flat_df_cnt)
  }

  def dataProfile(flattenedDF: DataFrame): Unit = {
    flattenedDF.createOrReplaceTempView("flattenedDF_vw")
    spark.sql(
      """select count(distinct(length(cast(regexp_extract(publish_date,'([0-9]+)',1) AS INT)))) as diff_date_formats_publsh_dt,
        | sum(case when length(cast(regexp_extract(genres,'([^a-zA-Z\s_0-9]+)',1) AS INT)) > 0 then 1 else 0 end) special_char_in_genres,
        | sum(case when stnd_publish_year > year(current_date()) or stnd_publish_year < 1200  then 1 else 0 end) as out_of_range_publish_year,
        | sum(case when number_of_pages = 0 or number_of_pages is null then 1 else 0 end) as zero_null_number_of_pages,
        | sum(case when publish_date = '' or publish_date is null then 1 else 0 end) as empty_null_publish_date,
        | sum(case when title = '' or title is null then 1 else 0 end ) as empty_null_title,
        | sum(case when genres = '' or genres is null then 1 else 0 end) as empty_null_genres,
        | sum(case when authors_key = '' or authors_key is null then 1 else 0 end) as empty_null_authors_key
        | from flattenedDF_vw
        |""".stripMargin).show(1)
  }

  def filterDF(flattenedDF: DataFrame): (DataFrame, Long) = {
    val filteredDF = flattenedDF.filter(
      """not (stnd_publish_year > 1950
        |and number_of_pages >20
        |and (title is null or title = '')
        |)""".stripMargin).cache

    val filtered_df_cnt = filteredDF.count
    println(s"\n\nFilteredDF Count:$filtered_df_cnt")
    (filteredDF, filtered_df_cnt)
  }

  def trimColumns(df: DataFrame): DataFrame = {

    val trim_col_list = List("authors_key", "title", "genres", "key")
    val trimed_df = trim_col_list.foldLeft(df) {
      (tempdf, clmn) =>
        tempdf.withColumn(clmn, trim(col(clmn)))

    }
    trimed_df
  }

  def cleanDF(df: DataFrame): (DataFrame, Long) = {
    val cleaned_df = df.filter(
      """ publish_date != '' and publish_date is not null
        | and authors_key != '' and authors_key is not null
        | and title != '' and title is not null
        | and stnd_publish_year between 1200 and year(current_date)
        | """.stripMargin)

    val cleaned_df_cnt = cleaned_df.count
    cleaned_df.show(5)
    println(s"\n\ncleaned_df Count:${cleaned_df_cnt}")
    (cleaned_df, cleaned_df_cnt)
  }

  def show_authors_first_last_book(): Unit = {

    println(s"\n\nFirst and Last Published Book of each Author:")

    spark.sql(
      """ select authors_key,max(first_published_book) as first_published_book,
        | max(last_published_book) as last_published_book
        |   from
        |  (
        | select  authors_key,
        | case when rnum_first_book = 1
        |      then concat_ws(':',stnd_publish_year, title)
        |       end as first_published_book,
        |  case when rnum_last_book = 1
        |      then concat_ws(':',stnd_publish_year, title)
        |      end as last_published_book
        | from (
        | select authors_key ,stnd_publish_year,title,
        | ROW_NUMBER() over(partition by authors_key order by stnd_publish_year,stnd_publish_month,stnd_publish_date) as rnum_first_book,
        | ROW_NUMBER() over(partition by authors_key order by stnd_publish_year desc,stnd_publish_month desc,stnd_publish_date desc) as rnum_last_book
        | FROM  temp_book_view)
        | where (rnum_first_book =1 or rnum_last_book =1)
        | ) group by authors_key
        |  """.stripMargin).show(20)
  }


  def getTop5CoAuthors(): Unit = {
    println(s"\n\nTop 5 Co-Authors:")

    val co_authored_books = spark.sql(
      """
       select key , count(distinct authors_key ) as cnt
       from temp_book_view
       group by key
       having cnt >1 """).cache
    println(s"\n\nCo-Authored Books DF Count:${co_authored_books.count}")

    co_authored_books.createOrReplaceTempView("co_authored_books")

    spark.sql(
      """
        select authors_key as top_5_co_authors, count(distinct t.key) as book_cnt
        | FROM  temp_book_view t
        | INNER JOIN
        | co_authored_books c
        | ON t.key = c.key
        | group by authors_key
        | order by count(distinct t.key) desc
        |  limit 5 """.stripMargin).show(5, false)

  }

  def getTop5Genres(): Unit = {

    println(s"\n\nTop 5 Genres of Published Books:")
    spark.sql(
      """ select count(*) as cnt, genres as top_5_genres
        | FROM  temp_book_view
        | where genres is not null
        | group by genres
        | order by count(*) desc
        |  limit 5 """.stripMargin).show(5, false)

  }


  def authorCountyearWise(): Unit = {
    println(s"\n\nYear wise Author's count who published at least one book:")
    spark.sql(
      """
        | select stnd_publish_year,count(distinct authors_key) as author_cnt
        | FROM  temp_book_view
        | group by stnd_publish_year
        | order by stnd_publish_year desc
        |  """.stripMargin).show(20, false)

  }

  def numAuthornumBooksPublishedPerMonth(): Unit = {
    println(s"\n\nMonth wise Author's count who published at least one book:")
    spark.sql(
      """
        | select stnd_publish_year , stnd_publish_month, stnd_publish_month_nm,
        | count(distinct authors_key) as author_count_per_year,
        | count(distinct key) as books_count_per_year
        | FROM  temp_book_view
        | WHERE stnd_publish_year between 1950 and 1970
        | group by stnd_publish_year, stnd_publish_month,stnd_publish_month_nm
        | order by stnd_publish_year,stnd_publish_month
        |  """.stripMargin).show(100, false)
  }




  def main(args: Array[String]): Unit = {
    val solution_num = if (args.size == 0) 1 else args(0).toInt
    try
    {
      val json_df = spark.read.json(json_file_path).cache()
      println("\n\nComplete File JSON Schema:")
      println(s"\n\nTotal Number of Keys:${json_df.columns.size}")

      json_df.printSchema
      val raw_df_cnt = json_df.count
      println(s"Raw Data Count: $raw_df_cnt")

      val (flattenedDF, flat_df_cnt) = flattenJsonData(json_df, solution_num)
      dataProfile(flattenedDF)
      val (filtered_df, filtered_df_cnt) = filterDF(flattenedDF)
      println(s"\n\nRows Filtered :${flat_df_cnt - filtered_df_cnt} ")


      val trimed_df = trimColumns(filtered_df)
      val standardize_genres_df = trimed_df.withColumn("genres", regexp_replace(col("genres"), "[^a-zA-Z\\s_0-9]+", ""))
      val (cleaned_df, cleaned_df_cnt) = cleanDF(standardize_genres_df)
      println(s"\n\nRows Filtered :${filtered_df_cnt - cleaned_df_cnt} ")

      cleaned_df.createOrReplaceTempView("temp_book_view")

      show_authors_first_last_book()
      getTop5CoAuthors()
      getTop5Genres()
      authorCountyearWise()
      numAuthornumBooksPublishedPerMonth()
    }
    finally
    {
      spark.close()
    }
  }

}
