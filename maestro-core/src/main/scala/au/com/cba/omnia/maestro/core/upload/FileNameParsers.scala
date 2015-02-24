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

package au.com.cba.omnia.maestro.core
package upload

import scala.util.parsing.input.CharSequenceReader

import org.joda.time.{DateTimeFieldType, MutableDateTime}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scalaz._, Scalaz._

import au.com.cba.omnia.omnitool.time.DateFormatInfo

/** The results of a match against a file name */
sealed trait MatchResult

/** The file does not match the pattern */
case object NoMatch extends MatchResult

/**
  * The file matches the pattern.
  *
  * Also contains the directories for the file
  * corresponding to the fields in the pattern.
  */
case class Match(dirs: List[String]) extends MatchResult

/** Contains makeFileNameParser, the FileNameParser, and the PartialParser */
trait FileNameParsers extends ParserBase {

  /** Apply a pattern parser to a pattern and get a file name parsing function back. */
  def makeFileNameParser(patternParser: Parser[FileNameParser], pattern: String): MayFail[String => MayFail[MatchResult]] =
    patternParser(new CharSequenceReader(pattern)) match {
      case NoSuccess(msg, _) => {
        msg.left
      }
      case Success(fileNameParser, _) => {
        val func: String => MayFail[MatchResult] =
          (fileName: String) => fileNameParser(new CharSequenceReader(fileName)) match {
            case Error(msg, _)    => msg.left
            case Failure(_, _)    => NoMatch.right
            case Success(dirs, _) => Match(dirs).right
          }
        func.right
      }
    }

  /**
    * Input file name parser
    *
    * Parses a file name and returns the directories which the file should be
    * placed in.
    */
  type FileNameParser = Parser[List[String]]

  /**
    * Fields in some part of the file name.
    *
    * misc is a list of miscellaneous fields in the same order
    * that they appear in the file name.
    * */
  case class Fields(time: Map[DateTimeFieldType, Int], misc: List[String]) {
    def followedBy(that: Fields): MayFail[Fields] = {
      val timeVals = this.time.mapValues(List(_)) |+| that.time.mapValues(List(_))
      val fields   = timeVals.toList.traverse[MayFail, (DateTimeFieldType, Int)] { case (k,vs) => vs.distinct match {
        case List(v)   => (k,v).right
        case conflicts => s"conflicting values found for $k: ${vs.mkString(", ")}".left
      }}
      fields.map(fs => Fields(fs.toMap, this.misc ++ that.misc))
    }
  }

  /** We allow unknown characters (?) and wildcards (*) in misc fields */
  sealed trait MiscFieldItem
  case object Unknown extends MiscFieldItem
  case object Wildcard extends MiscFieldItem

  /** Building block of a file name parser. */
  case class PartialParser(fields: List[DateTimeFieldType], parser: Parser[Fields])
      extends Parser[Fields] {
    def apply(in: Input) = parser(in)

    /**
      * Sequential composition with another partial file name parser
      *
      * It is a parse error (not failure) if the date time fields are consistent.
      */
    def followedBy(that: PartialParser) = {
      val combinedFields = (this.fields ++ that.fields).distinct
      val combinedParser = for {
        fields1 <- this.parser
        fields2 <- that.parser
        fields3 <- mandatory(fields1 followedBy fields2)
      } yield fields3
      PartialParser(combinedFields, combinedParser)
    }
  }

  /** Factory for partial file name parsers */
  object PartialParser {
    /** Create a PartialParser => PartialParser from a parser which does not produce any Fields info */
    def empty(parser: Parser[_]) =
      (continuation: PartialParser) => PartialParser(continuation.fields, parser ~> continuation.parser)

    /** Partial file name parser expecting a literal */
    def literal(lit: String) = empty(acceptSeq(lit))

    /** PartialParser for matching any single char */
    val unknown = empty(next)

    /** PartialParser consuming the largest sequence of characters for the rest of the glob to succeed */
    def unknownSeq(continuation: PartialParser) = {
      // rhs of ~> is lazy, so parser does not recurse indefinitely
      def parser: Parser[Fields] = (next ~> parser) | continuation.parser
      PartialParser(continuation.fields, parser)
    }

    /**
      * PartialParser which stores a list of wildcards as a misc field
      *
      * With this way of implementing wildcards by passing around the continuation
      * parser, I am not sure how to avoid duplicating logic with unknownSeq above.
      */
    def miscField(items: List[MiscFieldItem]) =
      (continuation: PartialParser) => {
        def makeParser(items: List[MiscFieldItem]): Parser[(String, Fields)] = items match {
          case Nil => continuation.parser ^^ (fields => ("", fields))

          case (Unknown :: restItems) => for {
            chr                 <- next
            (restField, fields) <- makeParser(restItems)
          } yield (chr.toString + restField, fields)

          case currItems @ (Wildcard :: restItems) => {
            val wildcardMove = for {
              chr                 <- next
              (restField, fields) <- makeParser(currItems)
            } yield (chr.toString + restField, fields)
            val wildcardStop = makeParser(restItems)
            wildcardMove | wildcardStop
          }
        }

        val parser = makeParser(items) ^? {
          case (miscField, fields) if !miscField.isEmpty => Fields(fields.time, miscField :: fields.misc)
        }
        PartialParser(continuation.fields, parser)
      }

    /** Input parser expecting a timestamp following a joda-time pattern */
    def timestamp(pattern: String): MayFail[PartialParser => PartialParser] = for {
      formatter <- MayFail.safe(DateTimeFormat.forPattern(pattern))
      fields    <- MayFail.fromOption(DateFormatInfo.fields(formatter), s"Could not find fields in date time pattern <$pattern>.")
    } yield PartialParser(fields, timestampParser(formatter, fields)).followedBy

    def timestampParser(formatter: DateTimeFormatter, fields: List[DateTimeFieldType]): Parser[Fields] =
      Parser(in => in match {
        // if we have underlying string, we can convert DateTimeFormatter.parseInto method into a scala Parser
        case _: CharSequenceReader => {
          if (in.atEnd)
            Failure("Cannot parse date time when at end of input", in)
          else {
            val underlying = in.source.toString
            val pos        = in.offset
            val dateTime   = new MutableDateTime(0)
            val parseRes   = MayFail.safe(formatter.parseInto(dateTime, underlying, pos))
            parseRes match {
              case \/-(newPos) if newPos >= 0 => {
                val timeFields = fields.map(field => (field, dateTime.get(field)))
                val negFields  = timeFields filter { case (field, value) => value < 0 } map { case (field, value) => field }
                if (negFields.nonEmpty) Failure(s"Negative fields: ${negFields.mkString(", ")}", in)
                else                    Success(Fields(timeFields.toMap, List.empty[String]), new CharSequenceReader(underlying, newPos))
              }
              case \/-(failPosCompl) => {
                val failPos     = ~failPosCompl
                val beforeStart = underlying.substring(0, pos)
                val beforeFail  = underlying.substring(pos, failPos)
                val afterFail   = underlying.substring(failPos, underlying.length)
                val msg         = s"Failed to parse date time. Date time started at the '@' and failed at the '!' here: $beforeStart @ $beforeFail ! $afterFail"
                Failure(msg, new CharSequenceReader(underlying, failPos))
              }
              case -\/(msg) => {
                Error(msg, in)
              }
            }
          }
        }

        // if we don't have underlying string, we're hosed
        case _ => Error(s"PartialParser only works on CharSequenceReaders", in)
      })

    /** Partial file name parser that succeeds when at the end of the file name */
    val finished = PartialParser(List.empty[DateTimeFieldType], eof ^^ (_ => Fields(Map.empty[DateTimeFieldType, Int], List.empty[String])))

    /** Take the next character */
    def next = elem("char", _ => true)

    /**
      * Turns a PartialParser into a FileNameParser.
      *
      * Checks that the fields in the file pattern are valid. The restrictions
      * on file pattern fields are described in
      * [[au.cba.com.omnia.maestro.core.task.Upload]].
      */
    def validate(parser: PartialParser): MayFail[FileNameParser] = for {
      _           <- MayFail.guard(parser.fields.nonEmpty, s"no date time fields found")
      fieldOrders <- parser.fields.traverse[MayFail, Int](fieldOrder(_))
      expected     = uploadFields.take(fieldOrders.max + 1)
      missing      = expected.filter(!parser.fields.contains(_))
      _           <- MayFail.guard(missing.empty, s"missing date time fields: ${missing.mkString(", ")}")
    } yield parser ^^ (fields => {
      val timeDirs = expected.map(field => {
        val value = fields.time(field)
        if (field equals DateTimeFieldType.year) f"$value%04d" else f"$value%02d"
      })
      timeDirs ++ fields.misc
    })

    /** Ordered list of fields we permit in upload time stamps */
    val uploadFields = List(
      DateTimeFieldType.year,
      DateTimeFieldType.monthOfYear,
      DateTimeFieldType.dayOfMonth,
      DateTimeFieldType.hourOfDay,
      DateTimeFieldType.minuteOfHour,
      DateTimeFieldType.secondOfMinute
    )

    /** Assign numbers to each field that upload works with */
    def fieldOrder(t: DateTimeFieldType) = {
      val index = uploadFields indexOf t
      if (index >= 0) index.right
      else            s"Upload does not accept $t fields".left
    }
  }
}
