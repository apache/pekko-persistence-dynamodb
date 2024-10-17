/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
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

  /**
   * Returns (a slightly overestimated) size of the fixed fields in a snapshot DynamoDB record.
   *
   * Assumes the maximum of 21 bytes for a number.
   *
   * Sources
   * https://zaccharles.medium.com/calculating-a-dynamodb-items-size-and-consumed-capacity-d1728942eb7c
   * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
   */
  val DynamoFixedByteSize =
    Key.length() + // + partitionKey.size
    SequenceNr.length() + 21 +
    Timestamp.length() + 21 +
    Payload.length() + // + payload.size
    100 + // Standard 100 bytes overhead
    100 // Safety factor for enabling extra features

}
