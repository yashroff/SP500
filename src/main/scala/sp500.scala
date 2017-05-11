import breeze.linalg.Vector
import Numeric.Implicits._
import scala.io.Source
import scala.collection.mutable.{ListBuffer }
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.runtime.RichDouble._
import weka.estimators.KernelEstimator
import org.apache.spark.rdd.RDD

def main(args: Array[String]) = {
	val conf = new SparkConf().setAppName("Sp500").setMaster("local")
	val sc = new SparkContext(conf)
	//val sparkSession = SparkSession.builder.getOrCreate()
	//import sparkSession.implicits._
	val Spjob = new SP500(sc)
	var results = spjob.run()
	sc.stop()
}

class SP500  (sc: SparkContext) {

	def run() {
		
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		var df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/home/ec2-user/1SP500.csv")

		//cast VALUE column to a double:
		df = df.filter(_(1) != ".")
		df.col("VALUE").cast("Double")
		val df_1 = df.withColumnRenamed("VALUE","oldVALUE")
		var df_2 = df_1.withColumn("VALUE",df_1.col("oldVALUE").cast("DOUBLE")).drop("oldVALUE")
		df = df_2.drop("DATE")

		//convert dataframe to an RDD
		var rows = df.map{x:org.apache.spark.sql.Row => x.getAs[Double](0)}.rdd
		
		//find rows whose negatives are also present
		val list: List[(Double)] = rows.collect().toList
		var neglist =  List[Double]()
		for (j <- list) {
			neglist = j * -1 :: neglist
		}
		
		//calculate density using one of 3 algorithms we've considered
		val values=rows.collect().sortWith(_<_)	//convert RDD to an array[Double]
		val density =  KernelDensityEstimatorProb (rows, values)
		val densities = density(values)
		
		//find matching rows
		var matches = rows.filter {neglist.contains (_) }
		matches.collect()

		var ind = 0
		var negind = 0
		var sumVal = 0.0
		var sumnegVal = 0.0
		for (j <- matches){
			ind = values.indexWhere (_ == j)
			negind = values.indexWhere (_ == -j)
			sumVal = densities.take(ind).sum
			sumnegVal = densities.take(negind).sum
			println(j,ind,negind,sumVal,sumnegVal, sumVal-sumnegVal)
		}	
	}
	def NormalProb (rows: RDD[Double])  {

	   //since there's no kCDF function in spark, we can try the cumulative probability from the normal distribution
		val mu = rows.mean()
		val stDev = rows.sampleStdev()

		val normalDist = new org.apache.commons.math3.distribution.NormalDistribution(mu,stDev)
		normalDist.cumulativeProbability(-2.2,2.2)
		//result is  0.90: this is not right, that should be for (-3,3)
		//hence wecannot use normal, need Kernel CDF
		
		//** try density(x)
	}

	def KernelDensityProb (rows: RDD[Double], values: Array[Double]) = (densities: Array[Double]) =>  {
	//def KernelDensityProb (rows: RDD[Double], values: Array[Double]) = (densities: Array[Double]) =>  {

		//calculate KernelDensity probabilities for each input date
		val kd = new KernelDensity()
		kd.setSample(rows.map(_.toDouble))
		kd.setBandwidth(rows.sampleStdev())
		//convert RDD to an array[Double]
		//val densities = kd.estimate(values)
		 def densityFunc(x:Array[Double])= kd.estimate(values)
	}
	def KernelDensityEstimatorProb (rows: RDD[Double], values:Array[Double]) = (densities: Array[Double]) =>  {
		val kde = new KernelEstimator(.0001)
		rows.collect().foreach(a => kde.addValue(a, 1))

		//calculate KernelDensityEstimator probabilities for each input date
		//val densities = kd.estimate(values)
		def densityFunc() = (x:Array[Double]) => x.map(kde.getProbability(_))
	}

}