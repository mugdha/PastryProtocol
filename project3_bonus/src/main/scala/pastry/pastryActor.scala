package pastry

import akka.actor.Actor
import scala.concurrent.Future
import scala.concurrent.{ ExecutionContext, Promise }
import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.ask
import akka.actor.ActorRef
import akka.util.Timeout
import scala.util.Random
import scala.concurrent.{ Future, Await }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{ Failure, Success }
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.ask
import java.util.concurrent.TimeoutException
import akka.pattern.AskTimeoutException

case class FirstJoin(nodeid: String, b: Int, l: Int, lenUUID: Int)
case class Join(nearestPeer: ActorRef)
case class AddMe(destination: String, rT: Array[Array[String]], lLeaf: List[Int], sLeaf: List[Int], lev: Int)
case class NextPeer(nextPeer: ActorRef, rt: Array[Array[String]], lLT: List[Int], sLT: List[Int], level: Int)
case class Deliver(rt: Array[Array[String]], lLT: List[Int], sLT: List[Int])
case class startRouting(message: String)
case class Forward(destination: String, level: Int, noHops: Int)

class pastryActor(nid: String, numReq: Int, numNodes: Int, b: Int, l: Int, lenUUID: Int, logBaseB: Int) extends Actor {

  var nodeID: String = nid;
  var rTable: Array[Array[String]] = Array.ofDim[String](lenUUID, lenUUID) /*(logBaseB, lenUUID)*/
  var largeLeaf: List[Int] = List()
  var smallLeaf: List[Int] = List()
  var largeLeafD: List[Int] = List() //to be removed
  var smallLeafD: List[Int] = List() //to be removed
  var master: ActorRef = context.actorFor("akka://pastry/user/master")

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def receive = {
    /*
     * First Node Joins network and sends message to master
     */
    case FirstJoin(nodeid: String, b: Int, l: Int, lenUUID: Int) =>
      sender ! Joined(nodeID)

    //---------------------------------------------------------------------------------------------------------------------------------
    /*
     * All other nodes join one by one 
     */
    case Join(nearestPeer: ActorRef) =>
      var nextNID: String = nearestPeer.path.toString().substring(19)
      var l: Int = shl(nodeID, nextNID)
      nearestPeer ! AddMe(nodeID, rTable, largeLeaf, smallLeaf, 0)

    //---------------------------------------------------------------------------------------------------------------------------------
    /*
     * THE FOLLOWING THREE FUNCTIONS ARE USED WHILE INITIALIZING THE NETWORK I.E. JOIN PHASE
     * 
     * In this case, the sender requests this node to add it(sender) into the network. The leaf and routing tables of both the sender 
     * and this node are updated. The sender sends a reference to its tables and this node contributes the relevent values to those tables
     */
    case AddMe(destination: String, rT: Array[Array[String]], lLeaf: List[Int], sLeaf: List[Int], lev: Int) =>
      var isLastHop: Boolean = false
      var dRT: Array[Array[String]] = rT.clone
      var lLT: List[Int] = lLeaf map (x => x)
      var sLT: List[Int] = sLeaf map { x => x }

      var level = shl(destination, nodeID)

      if (smallLeaf.isEmpty && largeLeaf.isEmpty) {
        //ONLY ONE NODE IN NETWORK
        isLastHop = true
      }

      var next = Route(destination, level, "join")
      if (next == null) {
        isLastHop = true
      }

      UpdateLeafT(level, destination)
      UpdateSelfRT(level, destination)
      dRT = UpdateNewRT(destination, level, rT, lev).clone //update one row in route table of destination node
      lLT = UpdateNewLarge(Integer.parseInt(nodeID, 8), destination, lLT) map { x => x }
      sLT = UpdateNewSmall(Integer.parseInt(nodeID, 8), destination, sLT) map { x => x }
      for (curr <- largeLeaf) {
        lLT = UpdateNewLarge(curr, destination, lLT) map { x => x }
        sLT = UpdateNewSmall(curr, destination, sLT) map { x => x }
      }
      for (curr <- smallLeaf) {
        lLT = UpdateNewLarge(curr, destination, lLT) map { x => x }
        sLT = UpdateNewSmall(curr, destination, sLT) map { x => x }
      }
      if (!isLastHop) {
        var nextPeer = context.actorFor("akka://pastry/user/" + next)
        sender ! NextPeer(nextPeer, dRT, lLT, sLT, level)

      } else {
        sender ! Deliver(dRT, lLT, sLT)
      }

    //---------------------------------------------------------------------------------------------------------------------------------
    /*
     * This message informs the node about the next node that it should contact inorder to join the network
     */
    case NextPeer(nextPeer: ActorRef, rt: Array[Array[String]], lLT: List[Int], sLT: List[Int], level: Int) =>
      rTable = rt.clone
      largeLeaf = lLT map { x => x }
      smallLeaf = sLT map { x => x }
      nextPeer ! AddMe(nodeID, rTable, largeLeafD, smallLeafD, level)

    //---------------------------------------------------------------------------------------------------------------------------------
    /*
     * When the nearest neighbor is found, this node performs a final update on it's tables and informs the master that it has joined 
     */
    case Deliver(rt: Array[Array[String]], lLT: List[Int], sLT: List[Int]) =>
      rTable = rt.clone
      largeLeaf = lLT map { x => x }
      smallLeaf = sLT map { x => x }
      makeRoute(numNodes)
      master ! Joined(nodeID)

    //---------------------------------------------------------------------------------------------------------------------------------
    /*
     * THE FOLLOWING Two FUNCTIONS ARE USED WHILE ROUTING I.E. ROUTE PHASE
     * 
     * In this case, every node creates #numReq random keys and starts routing them. Without loss of generalization, we have kept the
     * value of key to be less than nodeid of greatest node in the network. This will be helpful for getting better results in case of fewer nodes.
     */
    case startRouting(message: String) =>
      for (i <- 1 to numReq) {
        var key: String = createRandomString(numNodes)
        var level = shl(key, nodeID)
        self ! Forward(key, level, 0)
      }

    //---------------------------------------------------------------------------------------------------------------------------------
    /*
     * This case uses the Route method to find next hop and routes the message forward
     */
    case Forward(destination: String, level: Int, noHops: Int) =>
      var nHops: Int = noHops
      var next = Route(destination, level, "route"): String

      if (next == null) {
        master ! Finished(nHops)
      } else {
        nHops += 1
        var newlevel = shl(destination, next)
        var nextHop = context.actorFor("akka://pastry/user/" + next)
        nextHop ! Forward(destination, newlevel, nHops)
      }

    //---------------------------------------------------------------------------------------------------------------------------------
    /*
     * This case prints the values of leaf tables. It was written for debuging the code
     */
    case "printTables" =>
      println("For Node::::::::" + nodeID)
      printList(smallLeaf)
      printList(largeLeaf)

    //---------------------------------------------------------------------------------------------------------------------------------
    /*
     * Sends acknowledgement back to the sender.
     */
    case "ACK" =>
      sender ! "hello"

    //---------------------------------------------------------------------------------------------------------------------------------
    /*
     * Default case
     */
    case _ => println("Entering default case of pastryActor")

  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
   * THIS METHOD CALCULATES LENGTH OF SHARED PREFIX BETWEEN TWO STRINGS
   */
  def shl(nodeID1: String, nodeID2: String): Int = {
    val maxSize = scala.math.min(nodeID1.length, nodeID2.length)
    var i: Int = 0;
    while (i < maxSize && nodeID1(i) == nodeID2(i)) i += 1;
    i
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
   * THIS METHOD CONTAINS THE ACTUAL ROUTING ALGORITHM OF PASTRY. IT FOLLOWS ALL THREE STEPS OF ROUTING AS EXPLAINED IN THE PAPER.
   * IT USES A BOOLEAN "FOUND" TO KEEP TRACK OF WHETHER IT HAS FOUND THE NEXTHOP. IF IT DOES NOT FIND NEXTHOP IN ALL THREE STEPS, 
   * IT ASSUMES ITSELF TO BE THE LASTHOP AND RETURNS NULL
   */
  def Route(dest: String, level: Int, func: String): String = {
    var found: Boolean = false
    var next: String = "somebody"
    var nextDec: Int = -1
    var destDec = Integer.parseInt(dest, 8)
    var currentDec = Integer.parseInt(nodeID, 8)
    var mindiff = (destDec - currentDec).abs
    var isNextAlive: Boolean = false

    if (level == lenUUID) {
      next = null
      found = true
    }
    if (!found) {
      //search in leaf tables first
      if (destDec > currentDec) {
        if (!largeLeaf.isEmpty) {
          if (func.equals("join")) {
            if (destDec < largeLeaf.last) {
              for (i <- 0 to largeLeaf.length - 1) {
                if (largeLeaf(i) < destDec) {
                  nextDec = largeLeaf(i)
                }
              }
            }
          } else if (func.equals("route")) {
            if (destDec <= largeLeaf.last) {
              for (i <- 0 to largeLeaf.length - 1) {
                if (largeLeaf(i) <= destDec) {
                  nextDec = largeLeaf(i)
                }
              }
            }
          }

          if (nextDec != -1) { //found in large leaf table
            next = convertToNodeID(nextDec)
            found = true
          }
        }

      } else if (destDec < currentDec) {
        if (!smallLeaf.isEmpty) {
          if (func.equals("join")) {
            if (destDec > smallLeaf.head) {
              //next hop is in largeLeaf
              for (i <- 0 to smallLeaf.length - 1) {
                if (smallLeaf(i) < destDec) {
                  nextDec = smallLeaf(i)
                }
              }
            }
          } else if (func.equals("route")) {
            if (destDec >= smallLeaf.head) {
              //next hop is in largeLeaf
              for (i <- 0 to smallLeaf.length - 1) {
                if (smallLeaf(i) <= destDec) {
                  nextDec = smallLeaf(i)
                }
              }
            }

          }
          if (nextDec != -1) { //found in small leaf table
            next = convertToNodeID(nextDec)
            found = true

          }
        }
      }
    }

    //search in route table
    if (!found) {
      var Dl = dest.charAt(level).toString.toInt
      if (rTable(level)(Dl) != null) { //check values again!!!
        if (func.equals("join")) {
          if (Integer.parseInt(rTable(level)(Dl), 8) < destDec) {
            next = rTable(level)(Dl)
          } else {
            next = null //value found in RT was equal to destination node. Hence this is the last node
          }
          found = true
        }
        if (func.equals("route")) {
          if (Integer.parseInt(rTable(level)(Dl), 8) <= destDec) {
            next = rTable(level)(Dl)
            found = true
          }
        }
      }
    }

    //case three.....try to get closer
    if (!found) {
      var eligibleNodes: Set[Int] = Set()

      for (i <- 0 to largeLeaf.length - 1) {
        var largeleafNode = convertToNodeID(largeLeaf(i))
        if (shl(largeleafNode, dest) >= level) {
          eligibleNodes = eligibleNodes.+(largeLeaf(i))
        }
      }

      for (i <- 0 to smallLeaf.length - 1) {
        var smallleafNode = convertToNodeID(smallLeaf(i))
        if (shl(smallleafNode, dest) >= level) {
          eligibleNodes = eligibleNodes.+(smallLeaf(i))
        }
      }

      for (i <- level to rTable.length - 1) {
        for (j <- 0 to rTable(i).length - 1) {
          if (rTable(i)(j) != null) {
            eligibleNodes = eligibleNodes.+(Integer.parseInt(rTable(i)(j), 8))
          }
        }
      }

      for (iterateSet <- eligibleNodes) {
        if (iterateSet != destDec) {
          var diff: Int = (destDec - iterateSet).abs
          if (diff < mindiff) {
            mindiff = diff
            nextDec = iterateSet
            found = true
          }
        }
      }
      if (found)
        next = convertToNodeID(nextDec)
    }

    if (!found) { //current node is the last node
      //no one is eligible
      next = null
      found = true
    }

    /*
     * Handle Failure before sending
     */
    if (next != null) {
      //println(next)
      //while (!isNextAlive) {
        var nextP = context.actorFor("akka://pastry/user/" + next)
        implicit val timeout = Timeout(1 seconds)
        try {
          var test = Await.result(nextP ? "ACK", timeout.duration).asInstanceOf[String]
          isNextAlive = true
        } catch {
          case ex: TimeoutException =>
            //println("Node " + next + " is dead")
            next = handleFailure(next, dest, level, func)
        }
      //}
    }

    next
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
   * THE FOLLOWING THREE FUNCTIONS HANDLE NODE FAILURES IN THE SYSTEM. WHEN WE DON'T RECEIVE AN "ACK" FROM SOME NODE WITHING THE STIPULATED TIMEOUT, WE MARK THAT NODE AS FAILED.
   * THAT NODE IS REMOVED FROM ALL OUR TABLES AND WE TRY FINDING ANOTHER NODE IN OUR TABLE TO ROUTE THE MESSAGE
   */
  def handleFailure(nextNID: String, dest: String, lDest: Int, func: String): String = {
    var level: Int = shl(nextNID, nodeID)
    var nextDec = Integer.parseInt(nextNID, 8)
    removeFromLL(nextDec)
    removeFromSL(nextDec)
    removeFromRT(nextNID, level)
    var newNext = Route(dest, lDest, func)

    newNext
  }

  def removeFromLL(dest: Int) = {
    if (largeLeaf.contains(dest)) {
      largeLeaf = largeLeaf diff List(dest)
    }
  }
  def removeFromSL(dest: Int) = {
    if (smallLeaf.contains(dest)) {
      smallLeaf = smallLeaf diff List(dest)
    }
  }

  def removeFromRT(nextNID: String, level: Int) = {
    var Dl = nextNID.charAt(level).toString.toInt
    if (rTable(level)(Dl) != null) {
      if (rTable(level)(Dl).equals(nextNID)) {
        rTable(level)(Dl) = null
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
   * UPDATE LEAF TABLES OF NEIGHBOUR NODE BY ADDING NEW NODE IN THE APPROPRIATE TABLE 
   */
  def UpdateLeafT(level: Int, dest: String) = {

    var destDec = Integer.parseInt(dest, 8)
    var currentDec = Integer.parseInt(nodeID, 8)

    var isLargeFull: Boolean = false
    var isSmallFull: Boolean = false

    if (largeLeaf.length == l / 2) {
      isLargeFull = true
    }

    if (smallLeaf.length == l / 2) {
      isSmallFull = true
    }

    if (destDec > currentDec) {
      if (!largeLeaf.contains(destDec)) {
        if (isLargeFull) {
          largeLeaf = largeLeaf :+ destDec
          largeLeaf = largeLeaf.sorted
          largeLeaf = largeLeaf.take(largeLeaf.length - 1)
        } else {

          largeLeaf = largeLeaf :+ destDec
          largeLeaf = largeLeaf.sorted
        }
      }
    } else if (destDec < currentDec) {
      if (!smallLeaf.contains(destDec)) {
        if (isSmallFull) {
          smallLeaf = smallLeaf :+ destDec
          smallLeaf = smallLeaf.sorted
          smallLeaf = smallLeaf.take(0)
        } else {
          smallLeaf = smallLeaf :+ destDec
          smallLeaf = smallLeaf.sorted
        }
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
   * UPDATE LEAF TABLES OF NEW NODE BY ADDING THE VALUE "CURRENTDEC" TO IT
   */
  def UpdateNewLarge(currentDec: Int, dest: String, largeL: List[Int]): List[Int] = {

    var destDec = Integer.parseInt(dest, 8)
    var ll: List[Int] = largeL map { x => x }
    var isLargeFull: Boolean = false

    if (ll.length == l / 2) {
      isLargeFull = true
    }

    //insert currentDec into destination large table
    if (currentDec > destDec) {
      if (!ll.contains(currentDec)) {
        if (isLargeFull) {
          ll = ll :+ currentDec
          ll = ll.sorted
          ll = ll.take(ll.length - 1)
        } else {
          ll = ll :+ currentDec
          ll = ll.sorted
        }
      }
    }

    ll
  }

  def UpdateNewSmall(currentDec: Int, dest: String, smallL: List[Int]): List[Int] = {

    var destDec = Integer.parseInt(dest, 8)
    var sl: List[Int] = smallL map { x => x }
    var isSmallFull: Boolean = false

    if (sl.length == l / 2) {
      isSmallFull = true
    }

    //insert currentDec into destination large table
    if (currentDec < destDec) {
      if (!sl.contains(currentDec)) {
        if (isSmallFull) {
          sl = sl :+ currentDec
          sl = sl.sorted
          sl = sl.take(0)
        } else {
          sl = sl :+ currentDec
          sl = sl.sorted
        }
      }
    }

    sl
  }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
   * THE NEW NODE IS ADDED TO THE RT OF NEIGHBOUR.
   * IF THERE IS A CLASH, IT WILL KEEP THE NODE WITH LARGER NODEID
   */
  def UpdateSelfRT(level: Int, dest: String) = {
    //update routing table
    var DLevel: Int = dest.toCharArray()(level).toString.toInt //digit at index "level" in node to be added
    rTable(level)(DLevel) = dest
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
   * UPDATE RT OF NEW NODE. 
   * THIS METHOD IS WRITTEN DIFFERENTLY BUT DOES THE SAME THING AS ALGORITM GIVEN IN THE PAPER.
   * HERE EACH NEIGHBOUR NODE CONTRIBUTES RELEVANT ROWS TO THE NEW NODE'S RT.
   * FINALLY, THE UPDATED RT IS RETURNED TO THE NEW NODE
   */
  def UpdateNewRT(dest: String, level: Int, rt: Array[Array[String]], lastLevel: Int): Array[Array[String]] = {
    //copy only non-null values from correct rows of RTable of peer. This is necessary for initial phase. or else table will never be full
    var newRT: Array[Array[String]] = Array.ofDim[String](lenUUID, lenUUID)
    rt.copyToArray(newRT)

    for (i <- lastLevel to level) {
      for (j <- 0 to rTable(level).length - 1) {
        if (rTable(i)(j) != null) //copy only those values that are not null
          newRT(i)(j) = rTable(i)(j)
      }
    }

    //add nodeid of peer to table - WORKING
    var DLevel: Int = nodeID.toCharArray()(level).toString.toInt
    if (newRT(level)(DLevel) != null) { //if there is a value in that place, compare it with the new value and take the larger one
      newRT(level)(DLevel) = convertToNodeID(Math.max(Integer.parseInt(newRT(level)(DLevel), 8), Integer.parseInt(nodeID, 8)))
    } else {
      newRT(level)(DLevel) = nodeID
    }

    //remove own nodeid from table - NOT WORKING!!! CHECK NOW
    DLevel = dest.toCharArray()(level).toString.toInt
    newRT(level)(DLevel) = null
    newRT
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
   * THESE FUNCTIONS ARE REQUIRED TO GET A VALID NODEID AND KEY.
   * WE HAVE CREATED 8-DIGIT NODEIDS BY CONVERTING INTEGERS TO OCTAL-STRINGS AND THEN APPENDING ZEROES TO THE FRONT
   */
  def createRandomString(numNodes: Int): String = {

    var r = new Random().nextInt(numNodes) + 1;
    var sb = new StringBuffer();
    var strZ = new StringBuffer();
    var flag = true
    sb.append(Integer.toOctalString(r))
    for (i <- sb.length() to lenUUID - 1) {
      strZ.append(Integer.toOctalString(0))
    }
    strZ.append(sb)
    strZ.toString()
  }

  def convertToNodeID(i: Int): String = {
    var sb = new StringBuffer()
    var strZ = new StringBuffer()
    sb.append(Integer.toOctalString(i))

    for (i <- sb.length() to lenUUID - 1) {
      strZ.append(Integer.toOctalString(0))
    }
    strZ.append(sb)
    strZ.toString()
  }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
   * THESE FUNCTIONS WERE WRITTEN FOR DEBUGGING THE CODE
   */
  def printTable(table: Array[String]) = {
    for (i <- 0 to table.length - 1) {
      if (table(i) != null) {
        print(table(i) + '\t')
      } else {
        print("@\t")
      }
    }
    println()
  }

  def printRTable(Table: Array[Array[String]]) = {
    for (i <- 0 to Table.length - 1) {
      printTable(Table(i))
    }
  }

  def printList(list: List[Int]) {
    print(list.mkString(", "))
    println()
  }

  def makeSmallLeaf(numNodes: Int) = {
    var currDec = Integer.parseInt(nodeID, 8)

    for (i <- 1 to l / 2) {
      if ((currDec.toInt - i) > 0) {
        var temp: Int = (currDec.toInt - i)
        smallLeaf = smallLeaf :+ (temp)
      }
    }
    smallLeaf = smallLeaf.sorted

  }

  def makeLargeLeaf(numNodes: Int) = {
    var currDec = Integer.parseInt(nodeID, 8)

    for (i <- 1 to l / 2) {
      if ((currDec.toInt + i) <= numNodes) {
        var temp: Int = (currDec.toInt + i)
        largeLeaf = largeLeaf :+ (temp)
      }
    }
    largeLeaf = largeLeaf.sorted

  }

  def makeRoute(numNodes: Int) = {
    for (i <- 1 to numNodes) {
      var temp: String = convertToNodeID(i)
      if (!temp.equals(nodeID)) {
        var level = shl(nodeID, temp)
        var DLevel: Int = temp.toCharArray()(level).toString.toInt //digit at index "level" in node to be added
        rTable(level)(DLevel) = temp
      }
    }
    //println()
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////