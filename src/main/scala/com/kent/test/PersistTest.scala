package com.kent.test

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import com.kent.db.PersistManager
import com.kent.workflow.controlnode.StartNodeInfo
import com.kent.util.Util
import com.kent.workflow.WorkflowInfo
import com.kent.workflow.WorkflowInstance
import java.util.Date
import com.kent.pub.Event._

object PersistTest extends App{
    val conf = """
    akka {
        actor {
            provider = "akka.remote.RemoteActorRefProvider"
        }
        remote {
            enabled-transports = ["akka.remote.netty.tcp"]  
            netty.tcp {
                hostname = "0.0.0.0"
                port = 2551  
            }
        }
    }
  """
      val wfStr_mac = """
      <work-flow name="wf_join" id='1111'>
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
    
  import com.kent.coordinate.CoordinatorManager._
  import com.kent.workflow.WorkFlowManager._
  val config = ConfigFactory.parseString(conf)
  val system = ActorSystem("akkaflow", config)
  
  //
  val pm = system.actorOf(Props(PersistManager("jdbc:mysql://localhost:3306/wf","root","root", true)),"pm")
  
  //val wf = WorkflowInfo(wfStr_mac)
  //pm ! Save(wfi)
  //pm ! Save(wfi)
  
 // val wfi = WorkflowInstance(wf)
 // wfi.nodeInstanceList.foreach { x => x.startTime = Util.nowDate; x.endTime = Util.nowDate }
 // wfi.startTime = new Date()
 // wfi.endTime = new Date()
 // pm ! Save(wfi)
  //val wf = WorkflowInfo(wfStr_mac)
  //val wfi = WorkflowInstance(wf)
  val wf = new WorkflowInfo(null)
  val wfi = wf.createInstance()
  wfi.id = "b2bdfe0c"
  pm ! Get(wfi)
  
}