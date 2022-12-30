package com.sparkrareexamples.executionMetrics

case class Metrics(
                    numFiles: Long,
                    numOutputRows: Long,
                    numDynamicPartitions: Long,
                    numOutputBytes: Long,
                    duration: Long
                  ) {

  def getDurationSeconds() =
    if (duration == 0 )
      0
    else
      duration/1000

  def getOutputKBytes() =
    if (numOutputBytes == 0)
      0
    else
      numOutputBytes / 1024

  def getOutputMBytes() =
    if (numOutputBytes == 0)
      0
    else
      numOutputBytes / (1024*1024)
}


