package github.joestein.apophis
import scala.collection.mutable.{HashMap}
import github.joestein.skeletor._

trait Apophis {
	var month: String = ""
	var day: String = ""	
	var hour: String = ""	
	var minute: String = ""	
	var second: String = ""		
	
	//holds the values for our aggregate rows, the CF delinates the type of aggregate
	var aggregateKeys: HashMap[ColumnFamily, String] = HashMap.empty[ColumnFamily, String] 
	
	//holds the names of our aggregate columns
	var aggregateColumnNames: HashMap[String, String] = HashMap.empty[String, String] 
}