package github.joestein.apophis

import github.joestein.skeletor.{Cassandra, Keyspace, Rows, ColumnFamily}
import github.joestein.skeletor.Conversions._

object ApophisSampleLog {
	def apply(line: String) = new ApophisSampleLog(line)
}

class ApophisSampleLog (line: String) extends Apophis {
	//10/12/2011 11:22:33	GET	/sample?animal=duck&sound=quack&home=pond
	
	var animal: String = ""
	var sound: String = ""
	var home: String = ""
	
	val FIELD_DELIMITER = "\t";
	
	//parse the time we got
	def parseTime(time: String) = {
		//10/12/2011 11:22:33
		var ymdhms = time.split(" ")
		var ymd = ymdhms(0).split("/")
		var hms = ymdhms(1).split(":")

		month = ymd(2) + ymd(0)
		day = month + ymd(1)
		hour = day + hms(0)
		minute = hour + hms(1)
		second = minute + hms(2)
	}
	
	def setParams(q: String) = {
		val k = q.split("=")
		if (k.size>1) { //not a query param
			val p = k(1)
			k(0) match {
				case "animal" => animal = p			
				case "sound" => sound = p			
				case "home" => home = p													
				case _ => //we could deal with unknowns in a map as their own aggregate
			}
		}		
	}
	///sample?animal=duck&sound=quack&home=pond
	def parseQueryParams(qp: String) = {
		var qps = qp.split("\\?")
		
		var query = qps(1).split("&")
		
		query.foreach(q => setParams(q))
	}
	
	try {
		
		val lineParts = line.split(FIELD_DELIMITER)	
		parseTime(lineParts(0))
		parseQueryParams(lineParts(2))
		
	} catch {
		case e: Exception => e.printStackTrace() 
	}
	
	
	def ccAnimal(c: (String) => Unit) = {
		c(aggregateColumnNames("Animal") + animal)
	}
	
	def ccAnimalSound(c: (String) => Unit) = {
		c(aggregateColumnNames("AnimalSound") + animal + "#" + sound)
	}
		
	def ccAnimalHome(c: (String) => Unit) = {
		c(aggregateColumnNames("AnimalHome") + animal + "#" + home)
	}
			
	def ccAnimalSoundHome(c: (String) => Unit) = {
		c(aggregateColumnNames("AnimalSoundHome") + animal + "#" + sound + "#" + home)
	}
	
	def ccTotal(c: (String) => Unit) = {
		c(aggregateColumnNames("total"))
	}				
	
	def metrics(KEYSPACE: Keyspace) = {	

		//composite columns
		aggregateColumnNames("Animal") = "animal="
		aggregateColumnNames("AnimalSound") = "animal#sound="
		aggregateColumnNames("AnimalHome") = "animal#home="
		aggregateColumnNames("AnimalSoundHome") = "animal#sound#home="
		aggregateColumnNames("total") = "total="
		
		//rows we are going to write too
		aggregateKeys(KEYSPACE \ "ByDay") = day
		aggregateKeys(KEYSPACE \ "ByHour") = hour
		aggregateKeys(KEYSPACE \ "ByMinute") = minute	
		
		var rows:Rows = new Rows()

		//we wan to do this for all of the indexed key permutations
		def r(columnName: String): Unit = {
			aggregateKeys.foreach{tuple:(ColumnFamily, String) => {
				val (columnFamily,row) = tuple
				//rows add (columnFamily -> row has columnName inc) //increment the counter
				if (row !=null && row.size > 0)
				rows add (columnFamily -> row has columnName inc) //increment the counter
				//mutator.insertCounter(row, columnFamily.name, HFactory.createCounterColumn(columnName, 1))
				}}
		}		
		
		ccAnimal(r) 
		ccAnimalSound(r)
		ccAnimalSoundHome(r)
		ccTotal(r)
		
		rows									
	}

}