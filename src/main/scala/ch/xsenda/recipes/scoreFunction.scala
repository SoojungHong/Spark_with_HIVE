package ch.xsenda.recipes

class scoreFunction {

  def speedTestScoreFunction (x : Double) : Double = { //symetric tangent
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

  def HFCScoreFunction (x : Double) : Double = { //negative decay function
    var score = 0.0
    val slope = 1.2
    val maxPos = 0
    val maxMin = -2

    score = (maxPos - maxMin) * math.exp(-slope * x) - (maxPos - maxMin) + maxPos
    return(score)
  }

}
