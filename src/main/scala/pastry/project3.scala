package pastry

import akka.actor.ActorSystem
import akka.actor.Props

object project3 {

  var numNodes: Int = 1000; //default number of nodes = 1000
  var numReq: Int = 5 //default number of requests = 5
  
  def main(args: Array[String]) {
    if (args.length == 2) {
      numNodes = args(0).toInt
      numReq = args(1).toInt
    } else {
      println("Usage : project3.scala <numNodes> <numRequests>")
      println("Executing default scenario with number of nodes = 1000 and number of requests = 5")
    }
  
    var system = ActorSystem("pastry")
    var master = system.actorOf(Props(new masterActor(numNodes, numReq, system)), name = "master")
    println("Starting Pastry Algorithm ...\n")
    println("Initializing " + numNodes + " nodes ...\n")
    master ! "initialize"
  
  } //Main method ends here
}//class ends here