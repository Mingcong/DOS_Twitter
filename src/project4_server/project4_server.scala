import akka.actor.{ActorSystem, Actor, Props,ActorRef}
import scala.collection.mutable.ArrayBuffer
import java.security.MessageDigest
import akka.util.Timeout
import scala.concurrent.Await
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.Date
import java.util.Formatter
import java.util.Calendar
import java.text.SimpleDateFormat
import com.typesafe.config.ConfigFactory

object project4_server {
  
sealed trait Message
case object InitJob extends Message
case object IsReady extends Message
case object SendTweet extends Message
case object ViewTweet extends Message
case object buildRelation extends Message
case class getTweet(t: Tweet)
case class processWorkload(user_id: Long, ref_id: String, time_stamp: Date, workerArray: ArrayBuffer[ActorRef])
case class processTweet(user_id: Long, time_stamp: Date, ref_id: String, workerArray: ArrayBuffer[ActorRef])
case class getFollowers(user_id: Long, time_stamp: Date, ref_id: String, followers: ArrayBuffer[Long], workerArray: ArrayBuffer[ActorRef])
case class updateHomeTimeline(user_id: Long, time_stamp: Date, ref_id: String)
case class buildFollowers(user_id: Long, followers: ArrayBuffer[Long])
case class viewHomeTimeline(user_id: Long)
case class getHomeTimeline(user_id: Long)
case class viewUserTimeline(user_id: Long)
case class getUserTimeline(user_id: Long)

val numPerWorker: Int = 1000
var tweetStorage: Map[String, Tweet] = Map()

  class Tweet(_user_id: Long, _text: String, _time_stamp: Date) {
    var user_id: Long = _user_id
    var text: String = _text
    var time_stamp: Date = _time_stamp
    var ref_id: String = getHash(user_id.toString + text + dateToString(time_stamp))
  }

  case class TimeElement(ref_id: String, time_stamp: Date)

  def insertIntoArray(userTimeline: ArrayBuffer[TimeElement], ref_id: String, time_stamp: Date) {
    if (0 == userTimeline.size) {
      userTimeline.append(TimeElement(ref_id, time_stamp))
    } else {
      var index = 0
      while (index < userTimeline.size && time_stamp.compareTo(userTimeline(index).time_stamp) < 0)
        index += 1
      userTimeline.insert(index, TimeElement(ref_id, time_stamp))
    }
  }
  
  
  def getCurrentTime(): Date = {
    Calendar.getInstance().getTime()
  }
  
  def dateToString(current: Date): String = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val s: String = formatter.format(current)
    return s
  }
   
  def getHash(s: String): String = {
    val sha = MessageDigest.getInstance("SHA-256")
    sha.digest(s.getBytes)
      .foldLeft("")((s: String, b: Byte) => s +
        Character.forDigit((b & 0xf0) >> 4, 16) +
        Character.forDigit(b & 0x0f, 16))
  }

  
  class scheduleActor(numWorkers: Int, workerArray: ArrayBuffer[ActorRef]) extends Actor {
    var count: Int = 0
    
    val i: Long = 0
    def receive = {
      case getTweet(t) => {
        //save message
        tweetStorage += t.ref_id -> t
        workerArray(count) ! processWorkload(t.user_id ,t.ref_id, t.time_stamp , workerArray)
        count = count +1
        if(count == numWorkers){
          count = 0
        }
      }
      
      case viewHomeTimeline(i) => {
        workerArray((i/numPerWorker).toInt) ! getHomeTimeline(i)
      }
      case viewUserTimeline(i) => {
        workerArray((i/numPerWorker).toInt) ! getUserTimeline(i)
      }
      
      case buildRelation => {
        for(i<-0 until (numWorkers*numPerWorker-1)) {
          var followers = ArrayBuffer[Long]() 
          followers.append(i+1)
          workerArray((i/numPerWorker).toInt) ! buildFollowers(i, followers)
        }
        var followers = ArrayBuffer[Long](0,1,2,3,4,5,6,7,8,9) 

        workerArray((10/numPerWorker).toInt) ! buildFollowers(10, followers)
      }
      
    }
    
  }
  
  class workerActor() extends Actor {
    var userTimeline = new Array[ArrayBuffer[TimeElement]](numPerWorker)
    var homeTimeline = new Array[ArrayBuffer[TimeElement]](numPerWorker)
    
    var followers = new Array[ArrayBuffer[Long]](numPerWorker)
    
    var ready: Boolean = false
    
    def receive = {
      case processWorkload(user_id, ref_id, time_stamp, workerArray) => {
        workerArray((user_id/numPerWorker).toInt) ! processTweet(user_id, time_stamp, ref_id, workerArray)
      }
      
      case processTweet(user_id, time_stamp, ref_id, workerArray) => {
        insertIntoArray(userTimeline((user_id-numPerWorker*self.path.name.toInt).toInt), ref_id, time_stamp)
 //       userTimeline((user_id-numPerWorker*self.path.name.toInt).toInt).append(ref_id)
//        var userT = userTimeline((user_id-numPerWorker*self.path.name.toInt).toInt)
//        println(user_id + " send tweet: " + tweetStorage(ref_id).text  )
        sender ! getFollowers(user_id, time_stamp, ref_id, followers((user_id-numPerWorker*self.path.name.toInt).toInt), workerArray)
      }
      
      case getFollowers(user_id, time_stamp, ref_id, followers, workerArray) => {
        for(node<-followers){
          workerArray((node/numPerWorker).toInt) ! updateHomeTimeline(node, time_stamp, ref_id)
        }
      }
      
      case updateHomeTimeline(user_id, time_stamp, ref_id) => {
        insertIntoArray(homeTimeline((user_id-numPerWorker*self.path.name.toInt).toInt), ref_id, time_stamp)
//        homeTimeline((user_id-numPerWorker*self.path.name.toInt).toInt).append(ref_id)
//        println(user_id + " following: " + tweetStorage(ref_id).text  )

      }
      
      case buildFollowers(user_id,_followers) => {
          followers((user_id-numPerWorker*self.path.name.toInt).toInt) = _followers 
      }
      
      case getHomeTimeline(i) => {
        var line = homeTimeline((i-numPerWorker*self.path.name.toInt).toInt)
        println(i + " homeTimeline")
        for(ele<-line){
          println(tweetStorage(ele.ref_id ).user_id   + " at " + dateToString(tweetStorage(ele.ref_id ).time_stamp)  + " : "+ tweetStorage(ele.ref_id ).text )
        }
      }
 
      case getUserTimeline(i) => {
        var line = userTimeline((i-numPerWorker*self.path.name.toInt).toInt)
        println(i + " userTimeline")
        for(ele<-line){
          println(tweetStorage(ele.ref_id ).user_id   + " at " + dateToString(tweetStorage(ele.ref_id ).time_stamp)  + " : "+ tweetStorage(ele.ref_id ).text )
        }
      }
            
      case InitJob => {
        for(i <- 0 until numPerWorker){
          userTimeline(i) = new ArrayBuffer()
          homeTimeline(i) = new ArrayBuffer()
          followers(i) = new ArrayBuffer()
        }
        ready = true
      }
      
     case IsReady => {
       sender ! ready  
     }
        
    }
    
  }
  
  class clientActor(boss:ActorRef) extends Actor {
    def receive = {
      case SendTweet => {
        val t1 = new Tweet(10, "what are you doing?", getCurrentTime)
        boss ! getTweet(t1)

        val t3 = new Tweet(6, "I am hacking code.", getCurrentTime)
        boss ! getTweet(t3)
        
        val t2 = new Tweet(5, "what are you doing?", getCurrentTime)
        boss ! getTweet(t2)
        
        val t4 = new Tweet(6, "I want to sleep.", getCurrentTime)
        boss ! getTweet(t4)
      }
      
      
      case ViewTweet => {
        boss ! viewHomeTimeline(6)
        boss ! viewUserTimeline(6)
      }
          
    }

    
  }
  
  def main(args: Array[String]) {
    val numWorkers = if (args.length > 0) args(0) toInt else 100  // the number of workers in server
    var workerArray = ArrayBuffer[ActorRef]()
    val system = ActorSystem("TwitterSystem")
    
    var counter: Int =0
    while(counter < numWorkers){
      val worker = system.actorOf(Props(classOf[workerActor]), counter.toString)
      workerArray.append(worker)
      counter += 1
    }
    val boss = system.actorOf(Props(classOf[scheduleActor],numWorkers,workerArray), "boss")
    
    val client = system.actorOf(Props(classOf[clientActor],boss), "client")
    for (node <- workerArray) {
      node ! InitJob
      implicit val timeout = Timeout(20 seconds)
      var ready: Boolean = false
      while (!ready) {
        val future = node ? IsReady
        ready = Await.result(future.mapTo[Boolean], timeout.duration)
      }
    }
    
    boss ! buildRelation
    client ! SendTweet
    system.scheduler.scheduleOnce(1000 milliseconds) {
      client ! ViewTweet
    }
    
    system.scheduler.scheduleOnce(1000 milliseconds) {
    	system.shutdown
    }
  }

}