import akka.actor.{ActorSystem, Actor, Props,ActorRef}
import scala.collection.mutable.ArrayBuffer


sealed trait Message

case class Tweet()

object project4_server {
  
  class scheduleActor(numWorkers: Int) extends Actor {
    def receive = {
      case Tweet
      
    }
    
  }
  
  class workerActor() extends Actor {
    def receive = {
      
    }
    
  }
  
  class clientActor() extends Actor {
    def receive = {
      
    }
    
  }
  
  def main(args: Array[String]) {
    val numWorkers = if (args.length > 0) args(0) toInt else 100  // the number of workers in server
    var workerArray = ArrayBuffer[ActorRef]()
    val system = ActorSystem("TwitterSystem")
    val boss = system.actorOf(Props(classOf[scheduleActor],numWorkers), "boss")
    var counter: Int =0
    while(counter < numWorkers){
      val worker = system.actorOf(Props(classOf[workerActor]), counter.toString)
      workerArray.append(worker)
      counter += 1
    }
    val client = system.actorOf(Props(classOf[clientActor]), "client")
    
  }

}