import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf,SparkContext}

@SerialVersionUID(123L)
case class M_Matrix ( i: Int, j: Int, v: Double )
  extends Serializable {}

@SerialVersionUID(123L)
case class N_Matrix ( j: Int, k: Int, w: Double )
  extends Serializable {}
  

object Multiply {
  def main(args: Array[ String ]): Unit ={
  
    val conf = new SparkConf().setAppName("Multiply")
    val sc = new SparkContext(conf)
    
    val MatrixM = sc.textFile(args(0)).map( line => { 	val a = line.split(",")
      													M_Matrix(a(0).toInt,a(1).toInt,a(2).toDouble) } )

    val MatrixN = sc.textFile(args(1)).map( line => { 	val a = line.split(",")
      													N_Matrix(a(0).toInt, a(1).toInt, a(2).toDouble) } )
 
    
    val res = MatrixM.map( MatrixM => (MatrixM.j, (MatrixM.i, MatrixM.v)))
		      .join(MatrixN.map(MatrixN => (MatrixN.j, (MatrixN.k, MatrixN.w))))
		      .map{ case(j, ((i,v),(k,w))) => ((i,k), v * w)}		    
		      .reduceByKey(_ + _)
		      .sortByKey()
		      .map({ case ((i,k), sum) => (i, k, sum) })
    		  
    res.saveAsTextFile(args(2))
    
    sc.stop()
  }
}
