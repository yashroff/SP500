import Numeric.Implicits._
import scala.io.Source
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import weka.estimators.KernelEstimator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.round

object SP500 {
	def main(args: Array[String])  {
		val conf = new SparkConf().setAppName("Sp500").setMaster("local")
		val sc = new SparkContext(conf)
		val Spjob = new SP500(sc)
		var results = Spjob.run()
		println("RESULT")
		results.foreach(println)
		sc.stop()

	}
}


class SP500  (sc: SparkContext) {

var densitiesArray = Array.empty[Double] 
var valuesArray = Array.empty[Double]
var valuesRDD = sc.emptyRDD[Double]
	
   /* RUN method: entry point of SP500 class.
    * 1. load data file from local directory
	* 2. clean the file
	* 3. calculate densities for each value in the inputFile, using one of 3 algorithms:
	*    KernelDensity, KernelDensityEstimatorProb or NormalProb
	*    At present this must be manually switched via code change: UNDER CONSTRUCTION
	* 4. calculate density ranges for values and their negatives and return the result.
	*/
	def run() : List[Double] = {
		
		loadFileAndDataCleanse("/home/ec2-user/1SP500.csv")	//perform ETL and load the file
		//calcKernelDensityEstimatorDensities()
		//calKernelDensities()
		calcNormalDensities()
		val res = calcDensityRanges ()
		return res		
	
	}
	
	// loadFileAndDataCleanse method:
	// remove rows with null values, cast values to double, remove unneeded DATE column, round values to 3 digits
	def loadFileAndDataCleanse( fname : String)  = {
		val sparkSession = SparkSession.builder.getOrCreate()
		import sparkSession.implicits._
		
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		var d = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(fname)
	    var d1 = d.filter(_(1) != ".")		 
		var d2 = d1.withColumnRenamed("VALUE","oldVALUE")
		var d3 = d2.withColumn("VALUE",d2.col("oldVALUE").cast("DOUBLE")).drop("oldVALUE").drop("DATE").withColumn("VALUE", round($"VALUE", 3))
		 //convert dataframe to an RDD and an Array for different  processing needs
		valuesRDD = d3.map{x:org.apache.spark.sql.Row => x.getAs[Double](0)}.rdd
		valuesArray=valuesRDD.collect().sortWith(_<_)	

	}

	// calcNormalDensities method: 
	// for each value in the input dataset, calculate its density using NormalDistribution cumulativeProbability and store in densitiesArray
	def calcNormalDensities ()  {
		val normalDist = new org.apache.commons.math3.distribution.NormalDistribution(valuesRDD.mean(),valuesRDD.sampleStdev())
		densitiesArray = valuesArray.map(normalDist.cumulativeProbability(_))
	
	}

	// calKernelDensities method:
	//for each value in the input dataset, calculate its density using KernelDensity and store in densitiesArray
	def calKernelDensities () {
		val kd = new KernelDensity()
		kd.setSample(valuesRDD.map(_.toDouble))
		kd.setBandwidth(valuesRDD.sampleStdev())
		densitiesArray = kd.estimate(valuesArray)
			
	}
	
	// calcKernelDensityEstimatorDensities method:
	//for each value in the input dataset, calculate its density using KernelDensityEstimator class and store in densitiesArray
	def calcKernelDensityEstimatorDensities ()  {
		val kde = new KernelEstimator(.0001)
		valuesArray.foreach(a => kde.addValue(a, 1))
		densitiesArray = valuesArray.map(kde.getProbability(_))
			
	}

	/* calcDensityRanges method:
	* search valuesArray to find values whose negative symmetric values also exist in the dataset
	* get the difference between the densities of the value and its negative value
	*  if it falls between 85% and 95% add it to the list of returnValues
	* output: list of values where the difference in densitiies between (value, -value) is between 85% and 95%
	*/
	def calcDensityRanges () :List[Double]  = {
	
		var diff = 0.0
		var retlist = List[(Double)] ()
		var ctr = 0
		var matched = Array.ofDim[Double](valuesRDD.count.toInt(),6)

		for (j <- valuesArray) { 
			if (valuesArray.indexWhere(_ == -j) != -1 ) {
				//record columns: positive value, index of pos value, density of pos value, neg value, index of neg value, density of neg value
				matched (ctr)(0) = j	// pos value
				matched (ctr)(1) = valuesArray.indexWhere(_ == j) //index of pos value
				matched (ctr)(2) = densitiesArray(valuesArray.indexWhere(_ == j)) //density of pos value
				matched (ctr)(3) = valuesArray(valuesArray.indexWhere(_ == -j)) //neg value
				matched (ctr)(4) = valuesArray.indexWhere(_ == -j) //index of neg value
				matched (ctr)(5) = densitiesArray(valuesArray.indexWhere(_ == -j)) //density of neg value

				diff = matched(ctr)(5) - matched(ctr)(2)
				//println(ctr, matched(ctr)(0),matched(ctr)(1),matched(ctr)(2),matched(ctr)(3),matched(ctr)(4),matched(ctr)(5) , "DIFF:",diff)
				ctr+=1

				if (0.85 < diff && diff < 0.95) retlist = j :: retlist
			}
		}
		return retlist

}
}
