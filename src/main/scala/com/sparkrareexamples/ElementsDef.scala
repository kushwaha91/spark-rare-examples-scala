package com.sparkrareexamples

sealed trait Element {
  def id : Int
  def title: String
}

case class ElementRoot(
                      id: Int,
                      title: String,
                      elements: Seq[ElementRoot]
                      ) extends Element

case class ElementNode(
                      id : Int,
                      title: String,
                      elements: Seq[ElementRoot]
                      ) extends Element