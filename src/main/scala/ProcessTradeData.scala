/*****************************
Description : Task is to create a new match engine for FX orders and match order between orderBook and orderFile.
Source - Flat File(CSV)
Used - Spark Framework along with Scala
Target - Flat File(CSV)

******************************/

import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path,FileUtil}
import org.apache.spark.sql.SaveMode
import spark.implicits._

Object ProcessTradeData {
   val spark=GetSparkSession.getSparkSession("ProcessTradeData")
     
   val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
   hadoopfs.setVerifyChecksum(false)

//Below method reads CSV file with schema and load data into a dataframe
def readCSVFile(filePath:String,Schema:StructType):DataFrame={
   val hadoopfs=new Path("$filePath")
   if(hadoopfs.exists(hadoopfs))
   {
     spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header","false").schema(Schema).load(filePath)
	}
    else{
	   spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schema)
	}

}

/*matchOrders - defines to capture match and unmatch orders from orderBook and orderbook,orderFile */

def matchOrders(orderBookDF:DataFrame,orderFileDF:DataFrame):(DataFrame,DataFrame,DataFrame)={

    val orderBookBuy=orderBookDF.where("""order_type="BUY" """).withColumn("row_num",row_number() over (Window.partitionBy("quantity").orderBy("order_time")))
	
	val orderBookSell=orderBookDF.where("""order_type="SELL" """).withColumn("row_num",row_number() over (Window.partitionBy("quantity").orderBy("order_time")))
	
	val orderFileBuy=orderFileDF.where("""order_type="BUY" """).withColumn("row_num",row_number() over (Window.partitionBy("quantity").orderBy("order_time")))
	
	val orderFileSell=orderFileDF.where("""order_type="SELL" """).withColumn("row_num",row_number() over (Window.partitionBy("quantity").orderBy("order_time")))

    val orderBookBuyMatchDF=orderBookBuy.as("bookbuy").join(orderFileSell.as("filesell"),bookbuy("quantity")===filesell("quantity") AND bookbuy("row_num")===filesell("row_num"),"inner")
	
	.withColumn("matchId",when bookbuy("order_time") > filesell("order_time"),bookbuy("order_id").otherwise(filesell("order_id")))
	
	.withColumn("first_order_id",when bookbuy("order_time") > filesell("order_time"),filesell("order_id").otherwise(bookbuy("order_id")))
	
	.withColumn("timeMatched",greatest(bookbuy("order_time"),filesell("order_time")))
	
	.withColumn("matchedQuantity",bookbuy("quantity"))
	
	.withColumn("orderPrice",bookbuy("price"))
	
	.select("matchId","first_order_id","timeMatched","matchedQuantity","orderPrice")

    val orderBookSellmatchDF=orderBookSell.as("booksell").join(orderFileBuy.as("filebuy"),booksell("quantity")===filebuy("quantity") AND booksell("row_num")===filebuy("row_num"),"inner")
	
	.withColumn("matchId",when booksell("order_time") > filebuy("order_time"),booksell("order_id").otherwise(filebuy("order_id")))
	
	.withColumn("first_order_id",when booksell("order_time") > filebuy("order_time"),filebuy("order_id").otherwise(booksell("order_id")))
	
	.withColumn("timeMatched",greatest(booksell("order_time"),filebuy("order_time")))
	
	.withColumn("matchedQuantity",booksell("quantity"))
	
	.withColumn("orderPrice",booksell("price"))
	
	.select("matchId","first_order_id","timeMatched","matchedQuantity","orderPrice")



  val ordersMatchedDF=orderBookBuyMatchDF.union(orderBookSellmatchDF)
  
  val closedOrdersDF=ordersMatchedDF.withColumn("closedOrderId",explode(array("matchId","first_order_id"))).select("closedOrderId")
  
  val unclosedOrdersBookDF=orderBookDF.join(closedOrdersDF,orderBookDF("order_id")===closedOrdersDF("order_id"),"left").where(closedOrdersDF("order_id") is NULL)
  
  val unclosedOrdersFileDF=orderFileDF.join(closedOrdersDF,orderFileDF("order_id")===closedOrdersDF("order_id"),"left").where(closedOrdersDF("order_id") is NULL)
  
  (ordersMatchedDF,unclosedOrdersBookDF,unclosedOrdersFileDF)
  
  }

/*matchOrderWithInTheOrderFile : defines match orders from orderFile and returns the matched,unmatched orders from orderFile */
  def matchOrderWithInTheOrderFile(orderFile:DataFrame):(DataFrame,DataFrame)={
  
  val buyOrdersDF=orderFileDF.where("order_type='BUY'").withColumn("row_num",row_number() over(Window.partitionBy("quantity"),.orderBy("order_time")))
  
  val sellOrdersDF=orderFileDF.where("order_type='SELL'").withColumn("row_num",rwo_number() over(Window.partitionBy("quantity").orderBy("order_time")))

  val closedOdersDF=buyOrdersDF.jon("sellOrdersDF",buyOrdersDF("quantity")===sellOrdersDF("quantity") AND buyOrdersDF("row_number") === sellOrdersDF("row_number"))
                       .withColumn("matchOrderId",when(buyOrdersDF("order_time") > sellOrdersDF("order_time"),buyOrdersDF("order_id").otherwise(sellOrdersDF("order_id"))))
					   
                       .withColumn("firstOrderId",when(buyOrdersDF("order_time") > sellOrdersDF("order_time"),sellOrdersDF("order_id").otherwise(buyOrdersDF("order_id"))))
					   
					   .withColumn("matchedOrderTime",greatest(buyOrdersDF("order_time"),sellOrdersDF("order_time"))
					   
					   .withColumn("quantityMatched",sellOrdersDF("quantity"))
					   
					   .withColumn("order_price",when(buyOrdersDF("order_time")>sellOrdersDF("order_time"),sellOrdersDF("price")).otherwise(buyOrdersDF("price")))
					   
					   .select("matchOrderId","firstOrderId","matchedOrderTime","quantityMatched","order_price")
	

    val closedOrdersDF=closedOdersDF.withColumn("closedOrderId",explode(array("matchId","first_order_id"))).select("closedOrderId")	
	
    val unclosedOrdersBookDF=orderFileDF.join(closedOrdersDF,orderBookDF("order_id")===closedOrdersDF("order_id"),"left").where(closedOrdersDF("order_id") is NULL)
  
   (closedOrdersDF,unclosedOrdersBookDF)
  }

def main(args:Array[String])={
    try{
	val ordersFilePath=args(0)
	val ordersFileName=args(1)
	val ordersBookPath=args(2)
	val orderBookFileName=args(3)
	
	val schema=StructType(Array(
	     StructField("order_id","StringType"),
		 StructField("user_name","StringType"),
		 StructField("order_time","LongType"),
		 StructField("order_type"StringType"),
		 StructField("quantity","IntergerType"),
		 StructField("price","DecimalType")))
		 
	val ordersDF=readCsvFile("$ordersFilePath/$ordersFileName",ordersSchema).persist()
	
	val orderBookDF=readCsvFile("$ordersBookPath/$orderBookFileName",ordersSchema).persist()
	
	val (closedOrders,unclosedOrders)=matchOrders(orderBookDF,ordersDF)
	
	closedOrders.coalesce(1).write.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").mode(SaveMode.Append).save(s"$OrdersFilePath/output_${OrdersFileName}")
	
	unclosedOrders.coalesce(1).write.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").mode(SaveMode.Append).save(s"$ordersBookPath/output_${orderBookFileName}")
	
	println("end of the process")
		 
	}

   catch {
      case e:Throwable=>e.printStackTrace()
    }
    finally{
      spark.stop()
    }
  }
}

