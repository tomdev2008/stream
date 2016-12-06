package com.sdu.scala.advance

/**
  * Scala隐式类型转换时机
    1: 当方法中的参数的类型与目标类型不一致时
    2: 当对象调用类中不存在的方法或成员时,编译器会自动将对象进行隐式转换

    Scala编译器会在方法调用处的当前范围内查找隐式转换函数,如果没有找到则会尝试在源类型或目标类型的伴随对象中查找转换函数,
    如果还是没找到则拒绝编译
  *
  *
  * @author hanhan.zhang
  * */
object ImplicitStudy {

  // 隐式值
  implicit val defaultWord = "hello world !"

  def say(word : String) = {
    println("say : " + word)
  }

  // 类型转换
  implicit def intCastString(num : Int): String = {
    num.toString
  }

  def main(args: Array[String]): Unit = {
    // say() ===>> 接收String类型参数,传入Int类型参数,则会调用intCastString()进行类型转换
    ImplicitStudy.say(1)

  }

}
