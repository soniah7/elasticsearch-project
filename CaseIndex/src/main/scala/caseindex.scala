import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scalaj.http._
import scala.xml._
import java.io.File
import org.json4s._
import org.json4s.jackson.JsonMethods._


object CaseIndex {


	def main(args: Array[String]) {

		implicit val formats = DefaultFormats	// brings in default formats

		// create index with corresponding mapping
		val index = Http("http://localhost:9200/legal_idx").method("PUT").asString
		val mapping = Http("http://localhost:9200/legal_idx/cases/_mapping").method("PUT").postData("""{"cases":{"properties":{"file_name":{"type":"text"},"article_name":{"type":"text"},"url":{"type":"text"},"catchphrase":{"type":"text"},"sentence":{"type":"text"},"person":{"type":"text"},"location":{"type":"text"},"organization":{"type":"text"}}}}""").method("PUT").header("Content-Type", "application/json").asString

		// open files in directory
		val input = getListOfFiles(args(0))

		// traverse through input of each file
		input.foreach(f =>{

			// load xml file for further processing
			val xmlFile = XML.loadFile(f)

			// parse XML file to extract name, catchphrases and sentenses
			val file_name = f.getName()
			val article_name = (xmlFile \ "name").text
			val url = (xmlFile \ "AustLII").text
			// extract all catchphrases and split by space
			val catchphrase = xmlFile \\ "catchphrase"
			val allCatchphrasesInFile = new StringBuilder("");
            catchphrase.foreach(c =>{
				allCatchphrasesInFile ++= c.text
				allCatchphrasesInFile ++= " "
			})
			// extract all sentences
			val sentence = xmlFile \\ "sentence"
			val allSentencesInFile = new StringBuilder("");
            sentence.foreach(s =>{
				allSentencesInFile ++= s.text
			})

			// forward all sentences in a file to NLP
			val NLP_result = Http("""http://localhost:9000/?properties=%7B'annotators':'ner','outputFormat':'json'%7D""").postData(allSentencesInFile.toString).method("POST").header("Content-Type", "application/json").asString.body //??????????


			// collect result returned from NLP as JObject
			val NLP_object = parse(NLP_result).asInstanceOf[JObject]
			// query JSON field by "tokens" type
            val tokens = NLP_object \\ "tokens"
			// query JSON field by "word" & "ner" type
			// data with same index in word and NER forms a pair of (word, NER)
            var words = tokens \\ "word" \ "word"
            var ner = tokens \\ "ner" \ "ner"

			// initialize people, locations and organizations as list-like strings (i.e. "[a,b,c,d,...]")
            var people = "["
			var locations  = "["
            var organizations = "["

			// initialize counter to deal with comma seperation in the strings declared above
			var counter_people = 0
			var counter_locations = 0
			var counter_organizations = 0

			// transfer the variable "text" from type of JValue to List
			val wordsArr = words.extract[JArray].arr

			// iterate through all the words and check NER type of "PERSON", "LOCATION" AND "ORGANIZATION"
            var i = 0
            for(i <- 0 until wordsArr.length){

                if(ner(i).equals(JString("PERSON"))){
					if (counter_people == 1) {
						people ++= ","
					}
                    people += words(i).extract[String]
					counter_people = 1

                } else if(ner(i).equals(JString("LOCATION"))){
					if (counter_locations == 1) {
						locations ++= ","
					}
                    locations += words(i).extract[String]
					counter_locations = 1

                } else if(ner(i).equals(JString("ORGANIZATION"))){
					if (counter_organizations == 1) {
						organizations ++= ","
					}
                    organizations += words(i).extract[String]
					counter_organizations = 1
                }

            }

			// finalize people, locations and organizations strings
			people ++= "]"
			locations ++= "]"
			organizations ++= "]"

			// use extracted entity informatation together with name, catchphrases and sentenses to enrich created data through HTTP request to NLP
            val enriched_result = Http("http://localhost:9200/legal_idx/cases/"+file_name+"?pretty").postData("""{"file_name":"${file_name}","article_name":"${article_name}","url":"${url}","catchphrase":"${allCatchphrasesInFile}","sentence":"${allSentencesInFile}","person":"${people}","location":"${locations}"
,"organization":"${organizations}"}""").method("PUT").header("Content-Type", "application/json").asString

		})

	}

	// code of function below could be retrived from https://www.oreilly.com/library/view/scala-cookbook/9781449340292/ch12s09.html
	def getListOfFiles(dir: String):List[File] = {
		val d = new File(dir)
		if (d.exists && d.isDirectory) {
		    d.listFiles.filter(_.isFile).toList
		} else {
		    List[File]()
		}
	}


}
ß
