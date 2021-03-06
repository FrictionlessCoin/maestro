//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.maestro.core.task

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, Thermometer}, Thermometer._

import au.com.cba.omnia.maestro.core.clean.Clean
import au.com.cba.omnia.maestro.core.filter.RowFilter
import au.com.cba.omnia.maestro.core.split.Splitter
import au.com.cba.omnia.maestro.core.time.TimeSource
import au.com.cba.omnia.maestro.core.validate.Validator

import au.com.cba.omnia.maestro.core.thrift.scrooge.StringPair

private object LoadExec extends LoadExecution

object LoadExecutionSpec extends ThermometerSpec with StringPairSupport { def is = s2"""

Load execution properties
=========================

  can load using execution monad                                 $normal
  can load fixed length using execution monad                    $fixed
  returns the right load info for no data                        $noData
  returns the right load info for an acceptable number of errors $someErrors
  returns the right load info for too many errors                $manyErrors
  calculates the right number of rows after filtering            $filtered
  generating keys doesn't throw exceptions                       $normalGenKey

"""

  val conf = LoadConfig[StringPair](errors = "errors", timeSource = TimeSource.now(), none = "null")

  def normal = {
    withEnvironment(path(getClass.getResource("/load-execution").toString)) {
      val exec = LoadExec.load[StringPair](conf, List("normal"))
      executesSuccessfully(exec)._2 must_== LoadSuccess(4, 4, 4, 0)
    }
  }

  def fixed = {
    withEnvironment(path(getClass.getResource("/load-execution").toString)) {
      val fixedConf = conf.copy(splitter = Splitter.fixed(List(4)))
      val exec      = LoadExec.load[StringPair](fixedConf, List("fixed"))
      executesSuccessfully(exec)._2 must_== LoadSuccess(4, 4, 4, 0)
    }
  }

  def noData = {
    withEnvironment(path(getClass.getResource("/load-execution").toString)) {
      val exec = LoadExec.load[StringPair](conf, List("no-data"))
      executesSuccessfully(exec)._2 must_== EmptyLoad
    }
  }

  def someErrors = {
    withEnvironment(path(getClass.getResource("/load-execution").toString)) {
      val tolerantConf = conf.copy(errorThreshold = 0.25)
      val exec         = LoadExec.load[StringPair](tolerantConf, List("some-errors"))
      executesSuccessfully(exec)._2 must_== LoadSuccess(5, 5, 4, 1)
    }
  }

  def manyErrors = {
    withEnvironment(path(getClass.getResource("/load-execution").toString)) {
      val tolerantConf = conf.copy(errorThreshold = 0.25)
      val exec         = LoadExec.load[StringPair](tolerantConf, List("many-errors"))
      executesSuccessfully(exec)._2 must_== LoadFailure(5, 5, 3, 2)
    }
  }

  def filtered = {
    withEnvironment(path(getClass.getResource("/load-execution").toString)) {
      val filteredConf = conf.copy(errorThreshold = 0.25, filter = RowFilter.byRowLeader("D"))
      val exec         = LoadExec.load[StringPair](filteredConf, List("filtered"))
      executesSuccessfully(exec)._2 must_== LoadSuccess(5, 3, 3, 0)
    }
  }

  // This checks for lack of exceptions, but not success - a new thrift type is required.
  val confGenKey = LoadConfig[StringPair](errors = "errors", timeSource = TimeSource.now(), none = "null", generateKey = true)
  def normalGenKey = {
    withEnvironment(path(getClass.getResource("/load-execution").toString)) {
      val exec = LoadExec.load[StringPair](confGenKey, List("normal"))
      executesSuccessfully(exec)._2 must_== LoadFailure(4, 4, 0, 4)
    }
  }
}
