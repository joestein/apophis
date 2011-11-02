package github.joestein.apophis

import org.specs._
import github.joestein.skeletor.{Cassandra, Rows}
import java.util.UUID
import me.prettyprint.hector.api.query.{MultigetSliceCounterQuery}
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
	val sample4: String = "10/12/2011 11:22:33	GET	/sample?animal=cat&sound=purr&home=house"
	val sample5: String = "10/12/2011 11:22:33	GET	/sample?animal=lion&sound=purr&home=zoo"
	val sample6: String = "10/12/2011 11:22:33	GET	/sample?animal=dog&sound=woof&home=street"
		
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
									
			val sample = ApophisSampleLog(sample1)
			Cassandra << sample.metrics(FixtureTestApophis)

			var foundRows = false
			def processRowByDay(r:String, c:String, v:Long):Unit = {
				(r == "20111012") must beTrue
				(c == "animal#sound=duck#quack") must beTrue
				(v >= 1) must beTrue
				
				foundRows = true
			}

			def sets(mgsq: MultigetSliceCounterQuery[String, String]):Unit = {
				mgsq.setKeys("20111012") //we want to pull out the row key we just put into Cassandra
				mgsq.setColumnNames("animal#sound=duck#quack") //and just this column
			}

			FixtureTestApophisByDay ># (sets, processRowByDay) //get data out of Cassandra and process it	
					
			foundRows must beTrue
		}
		
		def setupAnimalSoundHomeTestData() = {
			val test1 = ApophisSampleLog(sample4)
			Cassandra << test1.metrics(FixtureTestApophis)			
			
			val test2 = ApophisSampleLog(sample5)
			Cassandra << test2.metrics(FixtureTestApophis)
			
			val test3 = ApophisSampleLog(sample6)
			Cassandra << test3.metrics(FixtureTestApophis)			
		}
		
		"read composite columns for all animals and their sounds only" in {	
			
			setupAnimalSoundHomeTestData()

			var foundDuckQuack = false			
			var foundDogWoof = false
			var foundCatPurr = false
			var foundLionPurr = false
			
			def processRowByDay(r:String, c:String, v:Long):Unit = {
				println("processRowByDay1="+r+"["+c+"]="+v)
				
				(r == "20111012") must beTrue //sanity for this insane world
				
				if (c == "animal#sound=duck#quack") 
					foundDuckQuack = true
				else if (c == "animal#sound=dog#woof") 
					foundDogWoof = true
				else if (c == "animal#sound=cat#purr") 
					foundCatPurr = true	
				else if (c == "animal#sound=lion#purr") 
					foundLionPurr = true
				else {
					println("UGH="+r+"["+c+"]="+v)
					false must beTrue //invalid to have any column BUT this come up
				}
			}

			def sets(mgsq: MultigetSliceCounterQuery[String, String]):Unit = {
				mgsq.setKeys("20111012") //we want to pull out the row key we just put into Cassandra
				mgsq.setRange("animal#sound=","animal#sound=~", false, 10000) //and just these columns
			}			

			FixtureTestApophisByDay ># (sets, processRowByDay) //get data out of Cassandra and process it			

			foundDuckQuack must beTrue
			foundDogWoof must beTrue
			foundCatPurr must beTrue			
			foundLionPurr must beTrue				
		}
		
		"read composite columns for animals that they pur only" in {

			setupAnimalSoundHomeTestData()		
			
			var foundCatPurr = false
			var foundLionPurr = false
			
			def processRowByDay(r:String, c:String, v:Long):Unit = {
				println("processRowByDay2="+r+"["+c+"]="+v)
				
				(r == "20111012") must beTrue //sanity for this insane world
				
				if (c == "sound#animal=purr#cat") 
					foundCatPurr = true	
				else if (c == "sound#animal=purr#lion") 
					foundLionPurr = true
				else
					false must beTrue //invalid to have any column BUT these come up
			}

			def sets(mgsq: MultigetSliceCounterQuery[String, String]):Unit = {
				mgsq.setKeys("20111012") //we want to pull out the row key we just put into Cassandra
				mgsq.setRange("sound#animal=purr#","sound#animal=purr#~", false, 10000) //and just these columns
			}			

			FixtureTestApophisByDay ># (sets, processRowByDay) //get data out of Cassandra and process it			

			foundCatPurr must beTrue			
			foundLionPurr must beTrue			
		}
	}
}