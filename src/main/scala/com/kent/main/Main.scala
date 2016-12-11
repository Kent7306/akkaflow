package com.kent.main

import com.kent.coordinate.Coordinator
import com.kent.coordinate.CoordinatorManager
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import com.kent.workflow.WorkFlowManager
import com.kent.db.PersistManager

object Main extends App{
    val coorStr4 = """
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
    
    val wfStr_win_1 = """
      <work-flow name="wf_join_1" id="c80c53">
          <start name="start_node_1" to="fork_node" />
          <fork name="fork_node">
              <path to="action_node_1" />
              <path to="action_node_2" />
          </fork> 
          <action name="action_node_1" retry-times="3" interval="10" timeout="500" host="127.0.0.1" desc = "这是节点测试">
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
          <action name="action_node_1" retry-times="3" interval="10" timeout="500" host="127.0.0.1" desc = "这是节点测试">
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
      <work-flow name="wf_join" desc="这是工作流描述">
          <start name="start_node" to="fork_node" />
          <fork name="fork_node">
              <path to="action_node_1" />
              <path to="action_node_2" />
          </fork>
          <action name="action_node_1" retry-times="3" interval="10" timeout="500" host="127.0.0.1" desc = "这是节点测试">
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
  import com.kent.coordinate.CoordinatorManager._
  import com.kent.workflow.WorkFlowManager._
  val config = ConfigFactory.parseString(conf)
  val system = ActorSystem("workflow-system", config)
  
  //
  val cm = system.actorOf(Props(CoordinatorManager(List())),"cm")
  val wfm = system.actorOf(Props(WorkFlowManager(List())),"wfm")
  val pm = system.actorOf(Props(PersistManager("jdbc:mysql://localhost:3306/wf","root","root")),"pm")
  PersistManager.pm = pm
  cm ! GetManagers(wfm,cm,pm)
  wfm ! GetManagers(wfm,cm,pm)
  
  wfm ! AddWorkFlow(wfStr_win_1)
  wfm ! AddWorkFlow(wfStr_win_2)
  //wfm ! AddWorkFlow(wfStr_mac)
  cm ! AddCoor(coorStr4) 
  
  Thread.sleep(3000)
  cm ! Start()
  
  //Thread.sleep(40000)
  //wfm ! KillWorkFlow("wf_join")

}