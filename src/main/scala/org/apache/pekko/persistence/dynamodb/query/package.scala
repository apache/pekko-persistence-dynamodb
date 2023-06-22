/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

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
