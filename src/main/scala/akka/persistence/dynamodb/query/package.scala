package akka.persistence.dynamodb

import akka.NotUsed
import akka.stream.scaladsl.Source

package object query {
  private[dynamodb] implicit class RichOption[+A](val option: Option[A]) extends AnyVal {
    def toSource: Source[A, NotUsed] =
      option match {
        case Some(value) => Source.single(value)
        case None        => Source.empty
      }
  }
}
