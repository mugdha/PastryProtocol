package pastry

import akka.actor.Actor
import akka.actor.ActorSystem
import scala.util.Random
import akka.actor.Props
import akka.actor.ActorRef
import scala.collection.mutable.HashSet
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.PoisonPill

case class Joined(nodeId: String)
case class Finished(nHops: Int)
case class init(nid1: String)

class masterActor(numNodes: Int, numReq: Int, mySys: ActorSystem) extends Actor {

  var b: Int = 3;
  var l: Int = 16;
  var lenUUID: Int = 1 << b //length of random string can be increased to support more actors
  var logBaseB = (Math.log(numNodes) / Math.log(lenUUID)).toInt
  var i: Int = 1
  var idHash = new HashSet[String]
  var peerList: List[ActorRef] = List()
  var prevNID: String = "first"
  var count: Int = 0; 
  var terminateCount : Int = 0
  var numN : Double = numNodes.toDouble
  var numR : Double = numReq.toDouble
  var totalHops: Double = 0
  var nodeid1:String = "Hi"
  
  def receive = {
  
    /*
     * First node joins
     */
    case "initialize" =>
      nodeid1= getRandomID(i)
      idHash.+=(nodeid1)
        var peer = mySys.actorOf(Props(new pastryActor(nodeid1, numReq, numNodes, b, l, lenUUID, logBaseB)), name = nodeid1)
        peerList = peerList :+ peer
        i += 1
        peer ! FirstJoin(nodeid1, b, l, lenUUID)
        //val roundScheduler = context.system.scheduler.schedule(100 milliseconds, 10 seconds, peer, PoisonPill)
      //self ! init(nodeid1)
    
    /*
     *  All other nodes join one by one
     */  
    case init(nid1: String) =>
      if ((i > 1) && (i <= numNodes)) {
        //create nodes
        var nodeid: String = getRandomID(i)
        idHash.+=(nodeid)
        var peer = mySys.actorOf(Props(new pastryActor(nodeid, numReq, numNodes, b, l, lenUUID, logBaseB)), name = nodeid)
        peerList = peerList :+ peer
        i += 1
        //add nodes
        var peer1 = context.actorFor("akka://pastry/user/" + nid1 )
        peer ! Join(peer1)
      } 
    
    /*
     * This message comes from a node when it has finished joining. After all nodes have joined, we wait 1 second
     * and then proceed to the routing step
     */
    case Joined(nodeId: String) =>
      count+=1
      if(count == numNodes) {
        /*
         * Start routing
         */
        Thread.sleep(1000)
        println(numNodes + " nodes have joined ...\n")
        println("Initializing Routing ...\n")
        for (peer <- peerList) {
          var msg: String = "hello"
          //peer ! "printTables"
          peer ! startRouting(msg) 
        }
      } else {
        self ! init(nodeid1)
      }
      
      /*
       * This message comes from a node, when it has finished routing numReq requests.
       * We wait for a second and print out the results
       */
      case Finished(nHops: Int) =>
     terminateCount += 1
     totalHops += nHops.toDouble
     if (terminateCount >= (numNodes * numReq)) {
       Thread.sleep(1000)
       println("All nodes have finished routing ...\n")
       println("================================================================================================================================")
       println("TOTAL ROUTES = " + (numN * numR))
       println("TOTAL HOPS = " + totalHops)
       println("AVERAGE HOPS PER ROUTE = " + totalHops / (numN * numR))
       println("================================================================================================================================")

       mySys.shutdown
      }
     
    case _ => println("Entering default case of masterActor")
  
  } //receive method ends here
  
  /*
   * Get a random 8-digit node id by taking the Octal String of Integers
   */
  def getRandomID(id:Int) : String = {
    var sb = new StringBuffer()
    var strZ = new StringBuffer()
    sb.append(Integer.toOctalString(i))
    
    for(i <- sb.length() to lenUUID-1) {
      strZ.append(Integer.toOctalString(0))
    }
    strZ.append(sb)
    strZ.toString()
  }
  }