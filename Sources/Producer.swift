//
//  Producer.swift
//  Perfect-Kafka
//
//  Created by Rockford Wei on 2017-03-01.
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

public class Producer: Kafka {

  internal var topicHandle: OpaquePointer? = nil

  internal var topicName = ""

  public var topic: String { get { return topicName } }

  internal var sequenceId = 0

  internal var queue = Set<UnsafeMutablePointer<Int>>()

  public func pop(_ msgId: UnsafeMutableRawPointer?) {
    guard let ticket = msgId else { return }
    let t = unsafeBitCast(ticket, to: UnsafeMutablePointer<Int>.self)
    queue.remove(t)
    t.deallocate(capacity: 1)
  }//end pop

  init(_ topic: String, topicConfig: TopicConfig? = nil, globalConfig: Config? = nil) throws {
    topicName = topic
    let gConf = try ( globalConfig ?? (try Config()))

    rd_kafka_conf_set_dr_cb(gConf.conf, { rk, _, _, _, _, ticket in
      guard let pk = rk else { return }
      guard let k = Kafka.instances[pk] else { return }
      guard let producer = k as? Producer else { return }
      producer.pop(ticket)
    })

    rd_kafka_conf_set_error_cb(gConf.conf, { conf, _, reason, _ in
      guard let pConf = conf else { return }
      guard let cnf = Kafka.instances[pConf] else { return }
      guard let r = reason else { return }
      cnf.OnError(String(cString: r))
    })

    try super.init(type: .PRODUCER, config: gConf)
    guard let h = rd_kafka_topic_new(_handle, topic, topicConfig == nil ? nil : topicConfig?.conf) else {
      let reason = rd_kafka_errno2err(errno)
      throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
    }//end guard
    topicHandle = h
    Producer.instances[_handle] = self
  }//end init

  public func flush(_ timeout: Int) {
    let then = time(nil)
    var now = time(nil)
    let limitation = time_t(timeout)
    while(!queue.isEmpty && limitation > (now - then)) {
      rd_kafka_poll(_handle, 100)
      now = time(nil)
    }//end while
  }//end flush

  deinit {
    guard let h = topicHandle else { return }
    // there may be some messages in queue waiting to send, so wait for at least one second
    flush(1)
    rd_kafka_topic_destroy(h)
    queue.forEach { $0.deallocate(capacity: 1) }
  }//end

  public func send(message: String, key: String? = nil) throws {
    guard let h = topicHandle else { throw Exception.UNKNOWN }

    var r:Int32 = 0

    sequenceId += 1

    let ticket = UnsafeMutablePointer<Int>.allocate(capacity: 1)
    ticket.pointee = sequenceId

    if let k = key {
      r = rd_kafka_produce(h, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE, strdup(message), message.utf8.count, k, k.utf8.count, ticket)
    }else{
      r = rd_kafka_produce(h, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE, strdup(message), message.utf8.count, nil, 0, ticket)
    }//end if
    if r == 0 {
      queue.insert(ticket)
      return
    }//end if
    ticket.deallocate(capacity: 1)
    let reason = rd_kafka_errno2err(errno)
    throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
  }//end send

  public func send(messages: [(String, String?)]) throws -> Int {
    if messages.isEmpty { return 0 }
    guard let h = topicHandle else { throw Exception.UNKNOWN }
    let batch = UnsafeMutablePointer<rd_kafka_message_t>.allocate(capacity: messages.count)
    for i in 0 ... messages.count - 1 {
      let p = batch.advanced(by: i)
      let m = messages[i]
      p.pointee.partition = RD_KAFKA_PARTITION_UA
      p.pointee.payload = unsafeBitCast(strdup(m.0), to: UnsafeMutableRawPointer.self)
      p.pointee.len = m.0.utf8.count
      sequenceId += 1
      let ticket = UnsafeMutablePointer<Int>.allocate(capacity: 1)
      ticket.pointee = sequenceId
      queue.insert(ticket)
      p.pointee._private = unsafeBitCast(ticket, to: UnsafeMutableRawPointer.self)
      if let key = m.1 {
        p.pointee.key = unsafeBitCast(strdup(key), to: UnsafeMutableRawPointer.self)
        p.pointee.key_len = key.utf8.count
      } else {
        p.pointee.key = nil
        p.pointee.key_len = 0
      }//end key
    }//next i
    let r = rd_kafka_produce_batch(h, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE, batch, Int32(messages.count))
    batch.deallocate(capacity: messages.count)
    return Int(r)
  }//
}//end class