package com.servicesource.spark

import job._

object JobRunner {

  def main(args: Array[String]) {
    
    FindMissingLinks.run
//    Transformer.run
  }
}