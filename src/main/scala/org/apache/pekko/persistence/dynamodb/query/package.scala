package org.apache.pekko.persistence.dynamodb

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

package object query {
  private[dynamodb] implicit class RichOption[+A](val option: Option[A]) extends AnyVal {
    def toSource: Source[A, NotUsed] =
      option match {
        case Some(value) => Source.single(value)
        case None        => Source.empty
      }
  }
}
