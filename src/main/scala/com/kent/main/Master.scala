package com.kent.main

import akka.actor.Actor
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.UnreachableMember
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberEvent
import com.kent.main.ClusterRole.Registration
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.ActorRef
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import com.kent.coordinate.CoordinatorManager
import com.kent.workflow.WorkFlowManager
import com.kent.db.PersistManager
import com.kent.coordinate.CoordinatorManager.GetManagers
import com.kent.workflow.WorkFlowManager.AddWorkFlow
import com.kent.coordinate.CoordinatorManager.AddCoor
import com.kent.coordinate.CoordinatorManager.Start
import com.kent.main.Worker.CreateAction
import com.kent.workflow.node.ActionNodeInstance
import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import com.kent.main.Master.GetWorkers
import com.kent.main.Master.AskWorkers

class Master extends ClusterRole {
  var coordinatorManager: ActorRef = _
  var workflowManager: ActorRef = _
  
  def receive: Actor.Receive = {
    case MemberUp(member) => 
      log.info("Member is Up: {}", member.address)
    case UnreachableMember(member) =>
      log.info("Member detected as Unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}", member.address, previousStatus)
    case state: CurrentClusterState =>
    
    case _:MemberEvent => // ignore 
      
    case Registration() => {
      //worker请求注册
      context watch sender
      workers = workers :+ sender
      log.info("Interceptor registered: " + sender)
      log.info("Registered interceptors: " + workers.size)
    }
    case Start() => this.start()
    case Terminated(workerActorRef) =>
      //worker终止，更新缓存的ActorRef
      workers = workers.filterNot(_ == workerActorRef)
    case AddWorkFlow(wfStr) => workflowManager ! AddWorkFlow(wfStr)
    case AddCoor(coorStr) => coordinatorManager ! AddCoor(coorStr)
    case AskWorkers() => sender ! GetWorkers(workers)
  }
  
  def start():Boolean = {
    coordinatorManager = context.actorOf(Props(CoordinatorManager(List())),"cm")
    workflowManager = context.actorOf(Props(WorkFlowManager(List())),"wfm")
    PersistManager.pm = context.actorOf(Props(PersistManager("jdbc:mysql://localhost:3306/wf","root","root")),"pm")
    coordinatorManager ! GetManagers(workflowManager,coordinatorManager, PersistManager.pm)
    workflowManager ! GetManagers(workflowManager,coordinatorManager, PersistManager.pm)
    Thread.sleep(1000)
    coordinatorManager ! Start()
    true
  }
}
object Master extends App {
  case class GetWorkers(workers: IndexedSeq[ActorRef])
  case class AskWorkers()
  def props = Props[Master]
  val port = "2751"
  // 创建一个Config对象
  val config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + port)
      .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
      .withFallback(ConfigFactory.load())
  // 创建一个ActorSystem实例
  val system = ActorSystem("workflow-system", config)
  val master = system.actorOf(Master.props, name = "master")
  
  master ! Start()
  
  val coorStr_win = """
	     <coordinator name="coor1_1" start="2016-09-10 10:00:00" end="2017-09-10 10:00:00">    
        <trigger>
            <cron config="* * * * * *"/>
        </trigger>
        <workflow-list>
          <workflow path="wf_join_1"></workflow>
      		<workflow path="wf_join_2"></workflow>
        </workflow-list>
        <param-list>
            <param name="yestoday" value="${time.today|yyyy-MM-dd|-1 day}"/>
            <param name="lastMonth" value="${time.today|yyyyMM|-1 month}"/>
            <param name="yestoday2" value="${time.yestoday}"/>
        </param-list>
    </coordinator>
	    """
  val coorStr_mac = """
	     <coordinator name="coor1_1" start="2016-09-10 10:00:00" end="2017-09-10 10:00:00">    
        <trigger>
            <cron config="* * * * * *"/>
        </trigger>
        <workflow-list>
          <workflow path="wf_join"></workflow>
        </workflow-list>
        <param-list>
            <param name="yestoday" value="${time.today|yyyy-MM-dd|-1 day}"/>
            <param name="lastMonth" value="${time.today|yyyyMM|-1 month}"/>
            <param name="yestoday2" value="${time.yestoday}"/>
        </param-list>
    </coordinator>
	    """
    
    val wfStr_win_1 = """
      <work-flow name="wf_join_1" id="c80c53">
          <start name="start_node_1" to="fork_node" />
          <fork name="fork_node">
              <path to="action_node_1" />
              <path to="action_node_2" />
          </fork> 
          <action name="action_node_1" retry-times="3" interval="10" timeout="500">
              <host-script>
                  <host>127.0.0.1</host>
                  <script>D://Strawberry//perl//bin//perl F://test.pl</script>
              </host-script>
              <ok to="join_node"/>
              <error to="join_node"/>
          </action>
          <action name="action_node_2" retry-times="1" interval="3" timeout="500">
              <host-script>
                  <host>127.0.0.1</host>
                  <script>D://Strawberry//perl//bin//perl F://test2.pl</script>
              </host-script>
              <ok to="join_node"/>
              <error to="join_node"/>
          </action>
          <kill name="kill_node">
              <message>kill by node(被kill node杀掉了)</message>
          </kill>
          <join name="join_node" to="end_node"/>
          <end name="end_node"/>
      </work-flow>
      """
    val wfStr_win_2 = """
      <work-flow name="wf_join_2" id="a80c54">
          <start name="start_node_1" to="fork_node" />
          <fork name="fork_node">
              <path to="action_node_1" />
              <path to="action_node_2" />
          </fork>
          <action name="action_node_1" retry-times="3" interval="10" timeout="500">
              <host-script>
                  <host>127.0.0.1</host>
                  <script>D://Strawberry//perl//bin//perl F://test.pl</script>
              </host-script>
              <ok to="join_node"/>
              <error to="join_node"/>
          </action>
          <action name="action_node_2" retry-times="1" interval="3" timeout="500">
              <host-script>
                  <host>127.0.0.1</host>
                  <script>D://Strawberry//perl//bin//perl F://test2.pl</script>
              </host-script>
              <ok to="join_node"/>
              <error to="join_node"/>
          </action>
          <kill name="kill_node">
              <message>kill by node(被kill node杀掉了)</message>
          </kill>
          <join name="join_node" to="end_node"/>
          <end name="end_node"/>
      </work-flow>
      """
    val wfStr_mac = """
      <work-flow name="wf_join">
          <start name="start_node" to="fork_node" />
          <fork name="fork_node">
              <path to="action_node_1" />
              <path to="action_node_2" />
          </fork>
          <action name="action_node_1" retry-times="3" interval="10" timeout="500">
              <host-script>
                  <host>127.0.0.1</host>
                  <script>/Users/kent/tmp/test_1.sh</script>
              </host-script>
              <ok to="join_node"/>
              <error to="join_node"/>
          </action>
          <action name="action_node_2" retry-times="1" interval="3" timeout="500">
              <host-script>
                  <host>127.0.0.1</host>
                  <script>/Users/kent/tmp/test_2.sh</script>
              </host-script>
              <ok to="join_node"/>
              <error to="join_node"/>
          </action>
          <kill name="kill_node">
              <message>kill by node(被kill node杀掉了)</message>
          </kill>
          <join name="join_node" to="end_node"/>
          <end name="end_node"/>
      </work-flow>
      """
    
    //master ! AddWorkFlow(wfStr_win_1)
    //master ! AddWorkFlow(wfStr_win_2)
    //master ! AddCoor(coorStr_win) 
    Thread.sleep(30000)
    master ! AddWorkFlow(wfStr_mac)
    master ! AddCoor(coorStr_mac) 
}