import at.tomtasche.contextio._
import scala.collection.JavaConversions._
import net.liftweb.json._
import au.com.bytecode.opencsv.CSVWriter
import java.io.{BufferedWriter, FileWriter, File}

case class Addr (email:String, name:Option[String])

case class Addrs (from:Addr, to:List[Addr])

case class Msg(
  emailMessageId:String,
  date:Int,
  subject:String,
  addresses:Addrs
)

case class BodySeg(bodySection:String, content:String)


case class Message (emailMessageId:String, date:Int, from:Addr, subject:String, text:String) {
  override def toString = "Message(" + emailMessageId + ", " + subject + ", ...)"
}

object CIOConsts {
  val since = "223"
  def count(count:Int) = Map("limit" -> count.toString, "since" -> since)
  val headers = Array[String]("emailMessageId", "date", "from.email", "subject", "text")
}

class ContextIOGet(key:String, secret:String) {
  implicit val formats = DefaultFormats
  private val cio = new ContextIO(key, secret)
  def getBody(account:String, msg:Msg):String = {
    val header = Map("emailmessageid" -> msg.emailMessageId,
      "datesent" -> msg.date.toString, "from" -> msg.addresses.from.email)
    val body = cio.messageText(account, header).rawResponse.getBody
    val data = parse(body) \ "data"
    //once data is got, string together each body segment's content strings.
    data.extract[List[BodySeg]] map (_ content) reduce (_ + _)
  }
  def getData(count:Int, account:String):List[Message] = {
    val body = cio.allMessages(account, CIOConsts.count(count)).rawResponse.getBody
    val data = parse(body) \ "data"
    data.extract[List[Msg]] map {
      msg => Message(msg.emailMessageId, msg.date, msg.addresses.from, msg.subject,
        getBody(account, msg))
    }
  }
}

class CorpusWriter(output:String) {
  val writer = new CSVWriter(new FileWriter(new File(output)))
  writer.writeNext(CIOConsts.headers)
  def write(msg:Message):Unit = msg match {
    case Message(emailMessageId, date, from, subject, text) => {
      val line = Array[String](emailMessageId, date.toString, from.email, subject, text)
      writer.writeNext(line)
    }
  }
  def close():Unit = writer.close()
}

object Download extends App {
  args match {
    case Array(apiKey, apiSecret, limit, account, target) => {
      val ciog = new ContextIOGet(apiKey, apiSecret)
      val corp = new CorpusWriter(target)
      ciog.getData(limit.toInt, account) foreach(msg => {println(msg); corp.write(msg)})
      corp.close()
    }
    case _ => println("nothing")
  }
}
