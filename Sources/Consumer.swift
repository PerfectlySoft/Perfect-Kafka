//
//  Consumer.swift
//  Perfect-Kafka
//
//  Created by Rockford Wei on 2017-03-02.
//  Copyright © 2017 PerfectlySoft. All rights reserved.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2017 - 2018 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//

import ckafka

public class Consumer: Kafka {

  internal var topicHandle: OpaquePointer? = nil

  internal var topicName = ""

  public var topic: String { get { return topicName } }

  init(_ topic: String, topicConfig: TopicConfig? = nil, globalConfig: Config? = nil) throws {
    topicName = topic
    try super.init(type: .CONSUMER, config: globalConfig)
    guard let h = rd_kafka_topic_new(_handle, topic, topicConfig == nil ? nil : topicConfig?.conf) else {
        let reason = rd_kafka_errno2err(errno)
        throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
      }//end guard
    topicHandle = h
    Producer.instances[_handle] = self
  }//end init

  deinit {
    guard let h = topicHandle else { return }
    let _ = rd_kafka_consume_stop(h, RD_KAFKA_PARTITION_UA)
    rd_kafka_topic_destroy(h)
  }//end

  public func store(_ offset: Int64, partition: Int32 = RD_KAFKA_PARTITION_UA) throws {
    guard let h = topicHandle else { throw Exception.UNKNOWN }
    let reason = rd_kafka_offset_store(h, partition, offset)
    if reason.rawValue == Exception.NO_ERROR.rawValue { return }
    throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
  }//end store

  public func start(_ from: Int64 = Int64(RD_KAFKA_OFFSET_BEGINNING), partition: Int32 = RD_KAFKA_PARTITION_UA) throws {
    guard let h = topicHandle else { throw Exception.UNKNOWN }
    let r = rd_kafka_consume_start(h, partition, from)
    if r == 0 { return }
    let reason = rd_kafka_errno2err(errno)
    throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
  }//end start

  public func stop (_ partition: Int32 = RD_KAFKA_PARTITION_UA) throws {
    guard let h = topicHandle else { return }
    let r = rd_kafka_consume_stop(h, RD_KAFKA_PARTITION_UA)
    if r == 0 { return }
    let reason = rd_kafka_errno2err(errno)
    throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
  }//end stop
}//end class
