package com.kent.test

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import scala.io.StdIn

object HttpTest extends App{
    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher
    type Str = String
    val wfRoute =  path("akkaflow" / "workflow" / Segment){  //workflow
      id => parameter('action){
        action => {
          if(action == "del"){
        	  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>workflow delete ${id}</h1>"))                      
          }else if(action == "get"){
        	  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>workflow get ${id}</h1>"))                      
          }else{
        	  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>error</h1>"))                      
          }
        }
      }
    } ~ path("akkaflow" / "workflow"){
      parameter('action){
        action => {
        	complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>add workflow</h1>"))          
        }
      }
    }
    val coorRoute = path("akkaflow" / "coor" / Segment){            //coordinator
      id => parameter('action){
        action => {
          if(action == "del"){
        	  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>coor delete ${id}</h1>"))                      
          }else if(action == "get"){
        	  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>coor get ${id}</h1>"))                      
          }else{
        	  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>error</h1>"))                      
          }
        }
      }
    } ~ path("akkaflow" / "coor"){
      parameter('action){
        action => {
        	complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>add coor</h1>"))          
        }
      }
    }
    
    val wfiRoute = path("akkaflow" / "workflow" / "instance" / Segment){            //workflow instance
      id => parameter('action){
        action => {
          if(action == "rerun"){
        	  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>workflow instance rerun ${id}</h1>"))                      
          }else if(action == "kill"){
        	  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>workflow instance kill ${id}</h1>"))                      
          }else{
        	  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>error</h1>"))                      
          }
        }
      }
    }
    
    val route = wfRoute ~ coorRoute ~ wfiRoute
      
      
      
      
      /*path("workflow" / ) {
        get {
           complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
        }
      }*/

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
}