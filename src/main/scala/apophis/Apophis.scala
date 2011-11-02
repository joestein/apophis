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
	
	var rows:Rows = new Rows()
	
	//we wan to do this for all of the indexed key permutations
	def r(columnName: String): Unit = {
		aggregateKeys.foreach{tuple:(ColumnFamily, String) => {
			val (columnFamily,row) = tuple
			if (row !=null && row.size > 0)
				rows add (columnFamily -> row has columnName inc) //increment the counter
			}}
	}	
}