package com.kent.test

import akka.http.scaladsl.server.Route

object DirectiveTest extends App{
  val a: Route = {
    println("MARK1")
    ctx => ctx.complete("yeah")
  }

  val b: Route = { ctx =>
    println("MARK2")
    ctx.complete("yeah")
  }
}