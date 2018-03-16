package ch.xsenda.recipes

class scoreFunction {

  def speedtest_scorefunction (x : Int) : Double = {
    val zeroThresh = 80
    val slopePos = 0.1
    val slopeNeg = 0.6
    val maxPos = 5
    val maxNeg = -2
    var y:Double = 0

    val factorPos = maxPos/scala.math.atan(slopePos*(100 - zeroThresh))
    val factorNeg = maxNeg/scala.math.atan(-slopeNeg * (zeroThresh))

    if (x > zeroThresh) {
      y = factorPos * scala.math.atan(slopePos*(x-zeroThresh))
    }
    else {
      y = factorNeg * scala.math.atan(-slopeNeg * (zeroThresh-x))
    }

    return(y)
  }

  def hfc_scorefunction () = {
/*
func <- function(x){
     y <- (maxPos - maxMin) * exp(-slope * x) - (maxPos - maxMin) + maxPos
     return(y)
}

slope <- 1.2
maxPos <- 0
maxMin <- -2
 */
  }

}
