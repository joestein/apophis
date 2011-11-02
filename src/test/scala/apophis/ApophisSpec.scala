package github.joestein.apophis

import org.specs._
import github.joestein.skeletor.{Cassandra, Rows}
import java.util.UUID
import me.prettyprint.hector.api.query.{MultigetSliceQuery}
import github.joestein.skeletor.Conversions._

class ApophisSpec extends Specification with Cassandra {

	val FixtureTestApophis = "FixtureTestApophis"
	val FixtureTestApophisByDay = "FixtureTestApophis" \ "ByDay"  //our keyspace for day data
	val FixtureTestApophisByHour = "FixtureTestApophis" \ "ByHour"  //our keyspace for hour data
	val FixtureTestApophisByMinute = "FixtureTestApophis" \ "ByMinute"  //our keyspace for Minute data	
		
	//sample logs for testing
	val sample1: String = "10/12/2011 11:22:33	GET	/sample?animal=duck&sound=quack&home=pond"
	val sample2: String = "10/12/2011 11:22:33	GET	/sample?animal=dog"	
	val sample3: String = "see spot run"		
	
	doBeforeSpec {
		Cassandra connect ("apophis-spec","localhost:9160")
		
	}
	
	doAfterSpec {
		Cassandra shutdown;
	}
	
	"Aposhis " should  {
		
		def validateSampleDate(sample: ApophisSampleLog) = {
			sample.month mustEqual "201110"
			sample.day mustEqual "20111012"			
			sample.hour mustEqual "2011101211"
			sample.minute mustEqual "201110121122"
			sample.second mustEqual "20111012112233"			
		}
		
		"parse a log file for expected input" in {
			val sample = ApophisSampleLog(sample1)	
			validateSampleDate(sample)		
			sample.animal mustEqual "duck"				
			sample.sound mustEqual "quack"				
			sample.home mustEqual "pond"								
		} 
		
		"parse a log file gracefully with semi expected input" in {
			val sample = ApophisSampleLog(sample2)
			validateSampleDate(sample)
			sample.animal mustEqual "dog"							
		}
		
		"aggregate some logs and confirm the time series are equal to each other" in  {
			//clean up the schema existing results
						
			val sample = ApophisSampleLog(sample1)
			Cassandra << sample.metrics(FixtureTestApophis)

			def processRowByDay(r:String, c:String, v:String):Unit = {
				(r == "20111012") must beTrue
				(c == "animal#sound") must beTrue
				(v == 1) must beTrue
			}

			def sets(mgsq: MultigetSliceQuery[String, String, String]):Unit = {
				mgsq.setKeys("20111012") //we want to pull out the row key we just put into Cassandra
				mgsq.setColumnNames("animal#sound") //and just this column
			}

			FixtureTestApophisByDay >> (sets, processRowByDay) //get data out of Cassandra and process it			

			Cassandra delete (FixtureTestApophisByDay -> "20111012" has "" of "")
			Cassandra delete (FixtureTestApophisByHour -> "2011101211" has "" of "")
			Cassandra delete (FixtureTestApophisByMinute -> "201110121112" has "" of "")

			true must beTrue
		}
	}
}