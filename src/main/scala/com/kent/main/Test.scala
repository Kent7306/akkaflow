package com.kent.main
import java.util.Date
import org.json4s.JsonAST.JString
import org.json4s.JsonAST.JObject


object Test extends App{
  
/*  val hsan = HostScriptActionNodeInfo("nihao")
  val hsan2 = hsan.deepClone()
  hsan2.workflowId = "0"
  hsan.workflowId = "1"
  val hsani = HostScriptActionNodeInstance(hsan)
  hsani.nodeInfo.workflowId = "2"
  val hsani2 = hsani.deepClone()
  hsani2.nodeInfo.workflowId = "3"
  println( hsani.toString())
  println( hsani2.toString())*/
  
  import org.json4s.jackson.JsonMethods._
  import org.json4s.JsonDSL._
  val paths = parse("""{"paths":["list1","list2","list3"]}""")
  val json = parse("""
         { "name": "joe",
           "children": [
             {
               "name": "Mary",
               "age": 5
             },
             {
               "name": "Mazy",
               "age": 3
             }
           ]
         }
       """)
   val l = (paths \ "paths" \\ classOf[JString]).asInstanceOf[List[String]]
  // println(l)
  
  
/*  val json1 = parse("""
         { "name": "joe",
           "at": "sss",
           "ser": "eee"
         }
       """)
       
   val list = for{
     JObject(ele) <- json1
     (k, JString(v)) <- ele
   } yield (k->v)
   println(list)*/
  val m = Map("1" -> "1v","2" -> "2v", "3" -> "3v")
  val arr = List("111","222","333")
  import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
  val paramStr = compact(render(m))
 // println(paramStr)
  println(compact(render(arr)))
  import com.kent.workflow.WorkflowInfo.WStatus
  import com.kent.workflow.WorkflowInfo.WStatus._
  val a = WStatus.W_FAILED
  println(a.id)
  println(WStatus.getWstatusWithId(a.id))
  
}