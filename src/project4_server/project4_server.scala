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
import Array._
import scala.util.Random
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer
import common._

object project4_server {

  sealed trait Message
  case object BuildFinshed extends Message
  case object InitJob extends Message
  case object IsReady extends Message
  case object IsBossReady extends Message
  case object IsClientReady extends Message
  case object SendTweet extends Message
  case object ViewTweet extends Message
  case object BuildRelation extends Message
  case object SentReadytoCient extends Message

  case class processWorkload(user_id: Long, ref_id: String, time_stamp: Date, workerArray: ArrayBuffer[ActorRef])
  case class processTweet(user_id: Long, time_stamp: Date, ref_id: String, workerArray: ArrayBuffer[ActorRef])
  case class getFollowers(user_id: Long, time_stamp: Date, ref_id: String, followers: ArrayBuffer[Long], workerArray: ArrayBuffer[ActorRef])
  case class updateHomeTimeline(user_id: Long, time_stamp: Date, ref_id: String)
  case class buildFollowers(user_id: Long)
 
//  case class getHomeTimeline(user_id: Long, user_actor: ActorRef)
//
//  case class getUserTimeline(user_id: Long, user_actor: ActorRef)

  val numPerWorker: Int = 5000
  val numWorkers: Int = 100
  val prob: ArrayBuffer[Double] = ArrayBuffer(0.06, 0.811, 0.874, 0.966, 0.9825, 0.9999, 0.99999, 1.000)
  var ClientActors: ArrayBuffer[ActorRef] = ArrayBuffer()
  var workloadPerWorker: ArrayBuffer[Int] = ArrayBuffer()
  var requestPerWorker: ArrayBuffer[Int] = ArrayBuffer()
    //    prob(0) = 0.06
    //    prob(1) = 0.751
    //    prob(2) = 0.063
    //    prob(3) = 0.092

  var tweetStorage: Map[String, Tweet] = Map()
  def dateToString(current: Date): String = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val s: String = formatter.format(current)
    return s
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
  
  def genRandFollowerCount(): Int = {    
    var randProb = Random.nextDouble()
    var index = prob.size
    breakable {
      for (i <- 0 until prob.size) {
        if (prob(i) >= randProb) {
          index = i
          break
        }
      }
    }
    if(0 == index) {
      return 0
    }
    else if (1 == index) {
      return genRandNumber(0, 50)
    }else if(2 == index) {
      return  genRandNumber(51, 100)
    }else if(3 == index) {
      return genRandNumber(101, 500)
    }else if(4 == index) {
      return genRandNumber(501, 1000)
    }else if(5 == index) {
      return genRandNumber(1001, 5000)
    }else if(6 == index) {
      return genRandNumber(5001, 10000)
    }else if(7 == index) {
//      return genRandNumber(10001, 100000)
      return 10001
    }else
//      return 100001
      return 10001      
  }
  

  def genRandNumber(first: Int, last: Int): Int = {
    first + Random.nextInt(last-first)
  }

  def genRandExceptCur(first: Long, last: Long, current: Long, randomNumber: Int): ArrayBuffer[Long] = {
    var followers: ArrayBuffer[Long] = new ArrayBuffer()
    followers.append(current)

    var randomFollower: Long = 0
    var counter: Int = 0
    while (counter < randomNumber) {
//      randomFollower = genRandNumber(first.toInt, last.toInt).toLong
    	randomFollower = Random.nextInt(last.toInt).toLong
      while (followers.contains(randomFollower))
//        randomFollower = genRandNumber(first.toInt, last.toInt).toLong
        randomFollower = Random.nextInt(last.toInt).toLong
      followers.append(randomFollower)
      counter += 1
    }
    followers.remove(0)
    followers

  }
  
  
  class scheduleActor(numWorkers: Int, workerArray: ArrayBuffer[ActorRef]) extends Actor {
    var count: Int = 0
    var ready: Boolean = false
    var ClientReady: Boolean = false
    val i: Long = 0
    var countFinished : Long = 0
    var countClient : Int = 0
    def receive = {
      
      case msg:String => {
        println(sender + " " + sender.path.name + " " + msg)
      }
      
      case GetNumofServerWorkers => {
        println(sender + "        "+ self + "        " + numWorkers)
        sender ! numOfServerWorkers(numWorkers)
        ClientActors.append(sender)
      }
            
      case getTweet(t) => {
//        println(sender.path.name + " " + t.text )
        //save message
        tweetStorage += t.ref_id -> t
        workerArray(count%numWorkers) ! processWorkload(t.user_id ,t.ref_id, t.time_stamp , workerArray)
        count = count +1
//        println("receive: " + count)
//        if(count == numWorkers){
//          count = 0
//        }
      }
      
//      case viewHomeTimeline(i) => {
//        workerArray((i%numWorkers).toInt) ! getHomeTimeline(i, sender)
//      }

      case BuildRelation => {
        for(i<-0 until numWorkers*numPerWorker) {
          workerArray((i%numWorkers).toInt) ! buildFollowers(i)  
        }
        println("build sent!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" )
        
      }
      
      case BuildFinshed => {
        countFinished = countFinished + 1
        println(countFinished)
        if(countFinished >= numWorkers*numPerWorker)
          ready = true
      }
      
      case IsBossReady => {
        sender ! ready
      }
      
//      case IsServerBossReady => {
//        sender ! ServerReady
//      }
      

      case IsClientReady => {
        sender ! ClientReady
      }

      case ClientBossReady => {
        countClient = countClient + 1
        if (countClient >= ClientActors.size) {
          ClientReady = true
          println("client ready")
        }

      }
//      case SentReadytoCient => {
//        sender ! ServerActorWantYouWork 
//      }
    }
    
  }
  
  class workerActor() extends Actor {
    var userTimeline = new Array[ArrayBuffer[TimeElement]](numPerWorker)
    var homeTimeline = new Array[ArrayBuffer[TimeElement]](numPerWorker)    
    var followers = new Array[ArrayBuffer[Long]](numPerWorker)
    
    var ready: Boolean = false
    
    def receive = {
      case processWorkload(user_id, ref_id, time_stamp, workerArray) => {
        var workerId = (user_id%numWorkers).toInt
        workerArray(workerId) ! processTweet(user_id, time_stamp, ref_id, workerArray)
      }
      
      case processTweet(user_id, time_stamp, ref_id, workerArray) => {
        insertIntoArray(userTimeline((user_id/numWorkers).toInt), ref_id, time_stamp)
//        println("process_in: actor" + self + " " + user_id + " send tweet: " + tweetStorage(ref_id).text  )
        sender ! getFollowers(user_id, time_stamp, ref_id, followers((user_id/numWorkers).toInt), workerArray)
      }
      
      case getFollowers(user_id, time_stamp, ref_id, followers, workerArray) => {
        for(node<-followers){
          workerArray((node%numWorkers).toInt) ! updateHomeTimeline(node, time_stamp, ref_id)
        }
        workloadPerWorker(self.path.name.toInt) = workloadPerWorker(self.path.name.toInt) +1
      }
      
      case updateHomeTimeline(user_id, time_stamp, ref_id) => {
        insertIntoArray(homeTimeline((user_id/numWorkers).toInt), ref_id, time_stamp)
//        println(user_id + " following: " + tweetStorage(ref_id).text  )

      }
      
      case buildFollowers(user_id) => {
        var randomFollowers = genRandFollowerCount()
        if (randomFollowers >= numWorkers * numPerWorker)
          randomFollowers = numWorkers * numPerWorker - 1
   
        followers((user_id/numWorkers).toInt) = genRandExceptCur(0, numWorkers * numPerWorker, user_id, randomFollowers) 
        sender ! BuildFinshed
      }
      
//      case getHomeTimeline(i, user_actor) => {
//        var line = homeTimeline((i/numWorkers).toInt)
//        var userLine = line.dropRight(line.size-10)
//        println(i + " homeTimeline")
//        for(ele<-userLine){
//          println(tweetStorage(ele.ref_id ).user_id   + " at " + dateToString(tweetStorage(ele.ref_id ).time_stamp)  + " : "+ tweetStorage(ele.ref_id ).text )
//        }
//      }

      case viewHomeTimeline(i) => {
        var timeLine = ArrayBuffer[String]()
        var line = homeTimeline(i)
        var userLine = line.dropRight(line.size - 25)
        //println(sender.path.name + " homeTimeline " + line.size)
        for (ele <- userLine) {
          //var message = tweetStorage(ele.ref_id).user_id + " at " + dateToString(tweetStorage(ele.ref_id).time_stamp) + " : " + tweetStorage(ele.ref_id).text
          var message = tweetStorage(ele.ref_id).text + " sent by:" + tweetStorage(ele.ref_id).user_id + " at " + dateToString(tweetStorage(ele.ref_id).time_stamp)
          //println(message)
          timeLine.append(message)
        }
        sender ! displayHomeTimeLine(timeLine)
        requestPerWorker(self.path.name.toInt) = requestPerWorker(self.path.name.toInt) +1
      }

      case viewUserTimeline(i) => {
        var timeLine = ArrayBuffer[String]()
        var line = userTimeline(i)
        var userLine = line.dropRight(line.size - 25)
        //println(sender.path.name + " userTimeline " + line.size )
        for (ele <- userLine) {
          var message = tweetStorage(ele.ref_id).user_id + " at " + dateToString(tweetStorage(ele.ref_id).time_stamp) + " : " + tweetStorage(ele.ref_id).text
          //println(message)
          timeLine.append(message)
        }
        sender ! displayUserTimeLine(timeLine)
        requestPerWorker(self.path.name.toInt) = requestPerWorker(self.path.name.toInt) +1
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

     case numFollowers(user_id) => {
       sender ! followers_num(followers(user_id).size)
       //println(user_id + " numOFfollowers  "   + self + " " + followers(user_id).size + "sender:" + sender)
     }
        
    }
    
  }
  

  
  def main(args: Array[String]) {
//    val numWorkers = if (args.length > 0) args(0) toInt else 100  // the number of workers in server
    var workerArray = ArrayBuffer[ActorRef]()

    val system = ActorSystem("TwitterSystem")
    val numUsers = numWorkers * numPerWorker
    var counter: Int =0
    while(counter < numWorkers){
      val worker = system.actorOf(Props(classOf[workerActor]), counter.toString)
      workerArray.append(worker)
      counter += 1
      workloadPerWorker.append(0)
      requestPerWorker.append(0)
    }
    val boss = system.actorOf(Props(classOf[scheduleActor],numWorkers,workerArray), "boss")
    
    for (node <- workerArray) 
      node ! InitJob
    for (node <- workerArray) {
      implicit val timeout = Timeout(20 seconds)
      var ready: Boolean = false
      while (!ready) {
        val future = node ? IsReady
        ready = Await.result(future.mapTo[Boolean], timeout.duration)
      }
    }
    
    boss ! BuildRelation
    implicit val timeout = Timeout(600 seconds)
    var ready: Boolean = false
    while (!ready) {
      val future = boss ? IsBossReady
      ready = Await.result(future.mapTo[Boolean], timeout.duration)
    }
    println("Build Finished")

    for(client_actor<-ClientActors) {
      client_actor ! ServerReady
    }
    
    var ClientReady: Boolean = false
    while (!ClientReady) {
      val future = boss ? IsClientReady
      ClientReady = Await.result(future.mapTo[Boolean], timeout.duration)
    }
//    boss ! SentReadytoCient
    println("Server Ready")
    for(client_actor<-ClientActors) {
      client_actor ! ServerActorWantYouWork
    }
    var t = 180
    system.scheduler.scheduleOnce(t seconds) {
      var SendWorkload : Double = 0.0
      var RequesWorkload : Double = 0.0
      for(workload<-workloadPerWorker) {
        SendWorkload = SendWorkload + workload
      }
      for(workload<-requestPerWorker) {
        RequesWorkload = RequesWorkload + workload
      }
      println("RequesWorkload = " + RequesWorkload)
      println("SendWorkload = " + SendWorkload)
      println("throughput = " + (SendWorkload+RequesWorkload)/t)
    }
    	
  }

}
