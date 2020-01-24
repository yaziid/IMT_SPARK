import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Column;
import static org.apache.spark.sql.functions.*;
import java.lang.Object;
import scala.Tuple2;

public class Yazid {
	 public static void main(String[] args) {
		 
		 JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("JavaRandomForestClassificationExample")
		            .setMaster("local[2]").set("spark.executor.memory","2g"));
		 
		 SparkSession spark = SparkSession

				    .builder()

				    .appName("Java Spark SQL Example")

				    .getOrCreate();
		 
		 StructType schema = new StructType()

				    .add("transactionId", "integer")

				    .add("resourceId", "integer")

				    .add("releaseId", "integer")

				    .add("quantity", "integer");
		 
		 Dataset<Row> transaction = spark.read().format("csv").option("delimiter", ";").option("header", "true").load("file:///C://WinUtils//transaction.csv");
		 Dataset<Row> resource = spark.read().format("csv").option("delimiter", ";").option("header", "true").load("file:///C://WinUtils//resource.csv");
		 
		 //Question 1
		 Dataset<Row> dfResult1 = transaction.select(count((new Column("resourceId"))));

		//Question 2
		 Dataset<Row> dfResult2 = transaction.select(new Column("releaseId")).orderBy(new Column("resourceId").desc()).limit(1);
		 
		 
		 //Question 3
		 Dataset<Row> dfResult3 = transaction.select("*").orderBy(new Column("quantity").desc()).limit(10);
		 
		 //Question 4
		 Dataset<Row> transaction_resource = transaction.join(resource, transaction.col("resourceId").equalTo(resource.col("resourceId")), "inner").where(new Column("artist").equalTo("Yazid")).select(sum(new Column("quantity")).multiply(0.15));
		 
		 		 //Question 5 T2
		 Dataset<Row> transaction_resource1 = transaction.join(resource, transaction.col("resourceId").equalTo(resource.col("resourceId")), "inner").where(new Column("artist").equalTo("Yazid"))
				 .select(sum(when(new Column("quantity").$less(500), new Column("quantity").multiply(0.10)
								 ).when(new Column("quantity").$greater$eq(500), new Column("quantity").multiply(0.20))));
		 
		 		 //Question 6 T3
		 Dataset<Row> transaction_resource2 = transaction.join(resource, transaction.col("resourceId").equalTo(resource.col("resourceId")), "inner").
				 where(new Column("artist").equalTo("Yazid")).
				 select(sum(when(new Column("quantity").$less(500), new Column("quantity").multiply(0.1)).
						 when(new Column("quantity").$greater$eq(500), new Column("quantity").$minus(500).multiply(0.2).$plus(500*0.1))));
		 
		 transaction_resource.show();

	     
		sc.close();
	   }
}
