package com.lxk


object HelloWorld {

def main(args: Array[String]) {
  println(matchTest(List(1,2,3,4,5)))

  println((1 to 10).getClass)

}
    def matchTest(x: List[Int]): Int = x match {
      case List(_,_*)=>1
      case _ => 999

    }

}
