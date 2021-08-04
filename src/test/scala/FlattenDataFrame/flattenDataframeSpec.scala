package FlattenDataFrame


import FlattenDataFrame.FlattenDataframe.flattenDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class flattenDataframeSpec extends AnyFlatSpec with Matchers with GivenWhenThen{
  implicit val  spark : SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Sample App")
    .getOrCreate()

    "flattenDataFrame" should "return a flattened DataFrame" in {

    Given ("The schema given")
      import spark.implicits._
      val df = spark.read.option("multiLine", true).json("C:/FlattenDataFrameF/data/Sample.json")


      When ("flattenDataFrame is invoked")
      val schema = flattenDataFrame(df).schema


    Then ("the new schema is")
      val expectedResult = new StructType()
        .add("GraphImages_typename","string")
        .add("GraphImages_comments_data_created_at","long")
        .add("GraphImages_comments_data_id","string")
        .add("GraphImages_comments_data_text", "string")
        .add("GraphImages_comments_data_owner_id", "string")
        .add("GraphImages_comments_data_owner_profile_pic_url", "string")
        .add("GraphImages_comments_data_owner_username", "string")
      //.add("1_tel", "string")
      //.add("1_e-mail", "string")
      //.add("2_tel", "string")
      //.add("2_e-mail", "string")

    expectedResult should contain theSameElementsAs(schema)
  }

}
