/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, derived from Akka.
 */

/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */

package org.apache.pekko.persistence.dynamodb

package object snapshot {

  // field names
  val Key = "par"
  val Timestamp = "ts"
  val TimestampIndex = "ts-idx"

  val Payload = "pay"
  val SequenceNr = "seq"
  val SerializerId = "ser_id"
  val SerializerManifest = "ser_manifest"
  val PayloadData = "pay_data"
}
