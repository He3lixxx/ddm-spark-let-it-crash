package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class ValueColumnPair(value: String, column: String)
case class InclusionList(column: String, includedInColumns: Set[String])

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

// Given grouped inclusion lists, aggregate to intersected inclusion dependencies
class IntersectionAggregation() extends UserDefinedAggregateFunction {
  def inputSchema: StructType = StructType(Array(StructField("includedInColumns", ArrayType(StringType))))

  def bufferSchema = StructType(Array(
    StructField("intersected_elements", ArrayType(StringType)),
    StructField("first_element", BooleanType)
  ))

  def dataType: DataType = ArrayType(StringType)

  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = Array()
    buffer(1) = true
  }

  def update(buffer: MutableAggregationBuffer, input: Row) = {
    if (buffer.getBoolean(1)) {
      buffer(0) = input(0)
      buffer(1) = false
    } else {
      val current_set = buffer.getList[String](0).asScala.toSet
      val input_set = input.getList[String](0).asScala.toSet
      val result_set = current_set intersect input_set

      buffer(0) = result_set.toList
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    this.update(buffer1, buffer2)
  }

  def evaluate(buffer: Row) = {
    buffer.getList[String](0)
  }
}

object Sindy {
  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    val reader = spark
      .read
      .option("header", "true")
      .option("delimiter", ";")

    import spark.implicits._

    val values_to_columns_df = inputs.flatMap( file => {
      val df = reader.csv(file)
      df.columns.map( column => {
        df.select(column).map( row => ValueColumnPair(row.getString(0), column) )
      })
    })

    val unioned_values_to_columns = values_to_columns_df.reduce(_ union _)

    val columns_per_distinct_value = unioned_values_to_columns
      .distinct()
      .groupBy("value")
      .agg(collect_set("column"))

    // attribute sets as in the Sindy paper
    val distinct_attribute_sets = columns_per_distinct_value.select("collect_set(column)").distinct()

    val inclusion_lists = distinct_attribute_sets.flatMap( row => {
      val list: Seq[String] = row(0).asInstanceOf[Seq[String]]
      val set = list.toSet

      set.map( element => {
        val reduced_set = set - element
        InclusionList(element, reduced_set)
      })
    })

    val intersection_aggregator = new IntersectionAggregation()

    val inclusion_dependencies = inclusion_lists
      .groupBy("column")
      .agg(intersection_aggregator(inclusion_lists.col("includedInColumns")).as("includedIn"))

    val sorted_inds = inclusion_dependencies
      .filter( row => !row.getList[String](1).isEmpty )
      .sort("column")

    sorted_inds
      .collect
      .foreach( row => {
        println(row.getString(0) + " < " + String.join(", ", row.getList[String](1)))
      })
  }
}

