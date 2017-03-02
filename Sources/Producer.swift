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

  internal var sequenceId:Int64 = 0

  internal var queue = Set<UnsafeMutablePointer<Int64>>()

  public typealias DeliveryCallback = (Int64) -> Void

  public var OnSent: DeliveryCallback = { _ in }

  public func pop(_ msgId: UnsafeMutableRawPointer?) {
    guard let ticket = msgId else { return }
    let t = unsafeBitCast(ticket, to: UnsafeMutablePointer<Int64>.self)
    queue.remove(t)
    let id = t.pointee
    t.deallocate(capacity: 1)
    OnSent(id)
  }//end pop

  init(_ topic: String, topicConfig: TopicConfig? = nil, globalConfig: Config? = nil) throws {
    topicName = topic
    let gConf = try ( globalConfig ?? (try Config()))

    rd_kafka_conf_set_dr_cb(gConf.conf, { rk, _, _, _, _, ticket in
      guard let pk = rk else { return }
      guard let k = Producer.instances[pk] else { return }
      guard let producer = k as? Producer else { return }
      producer.pop(ticket)
    })

    rd_kafka_conf_set_error_cb(gConf.conf, { conf, _, reason, _ in
      guard let pConf = conf else { return }
      guard let cnf = Producer.instances[pConf] else { return }
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

  public var outbox: [Int64] { get {
    return queue.map { $0.pointee }.sorted()
    }//end get
  }//end toSend

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

  @discardableResult
  public func send(message: String, key: String? = nil) throws -> Int64 {
    guard let h = topicHandle else { throw Exception.UNKNOWN }

    var r:Int32 = 0

    sequenceId += 1

    let ticket = UnsafeMutablePointer<Int64>.allocate(capacity: 1)
    ticket.pointee = sequenceId

    if let k = key {
      r = rd_kafka_produce(h, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE, strdup(message), message.utf8.count, k, k.utf8.count, ticket)
    }else{
      r = rd_kafka_produce(h, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE, strdup(message), message.utf8.count, nil, 0, ticket)
    }//end if
    if r == 0 {
      queue.insert(ticket)
      return ticket.pointee
    }//end if
    ticket.deallocate(capacity: 1)
    let reason = rd_kafka_errno2err(errno)
    throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
  }//end send

  @discardableResult
  public func send(message: [Int8], key: [Int8] = []) throws -> Int64 {
    guard let h = topicHandle else { throw Exception.UNKNOWN }
    if message.count < 1 { return -1 }

    var r:Int32 = 0

    sequenceId += 1

    let ticket = UnsafeMutablePointer<Int64>.allocate(capacity: 1)
    ticket.pointee = sequenceId

    let buffer = malloc(message.count)
    #if os(Linux)
      let _ = message.withUnsafeBufferPointer { memcpy(buffer!, $0.baseAddress!, message.count) }
    #else
      let _ = message.withUnsafeBufferPointer { memcpy(buffer, $0.baseAddress, message.count) }
    #endif
    if key.count > 0 {
      let kbuf = UnsafeMutablePointer<Int8>.allocate(capacity: key.count)
      #if os(Linux)
        let _ = message.withUnsafeBufferPointer { memcpy(kbuf, $0.baseAddress!, key.count) }
      #else
        let _ = message.withUnsafeBufferPointer { memcpy(kbuf, $0.baseAddress, key.count) }
      #endif
      r = rd_kafka_produce(h, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE, buffer, message.count, kbuf, key.count, ticket)
    }else{
      r = rd_kafka_produce(h, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE, buffer, message.count, nil, 0, ticket)
    }//end if
    if r == 0 {
      queue.insert(ticket)
      return ticket.pointee
    }//end if
    ticket.deallocate(capacity: 1)
    let reason = rd_kafka_errno2err(errno)
    throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
  }//end send

  @discardableResult
  public func send(messages: [(String, String?)]) throws -> [Int64] {
    var tickets = [Int64]()
    if messages.isEmpty { return tickets }
    guard let h = topicHandle else { throw Exception.UNKNOWN }
    let batch = UnsafeMutablePointer<rd_kafka_message_t>.allocate(capacity: messages.count)

    for i in 0 ... messages.count - 1 {
      let p = batch.advanced(by: i)
      let m = messages[i]
      p.pointee.partition = RD_KAFKA_PARTITION_UA
      p.pointee.payload = unsafeBitCast(strdup(m.0), to: UnsafeMutableRawPointer.self)
      p.pointee.len = m.0.utf8.count
      sequenceId += 1
      let ticket = UnsafeMutablePointer<Int64>.allocate(capacity: 1)
      ticket.pointee = sequenceId
      queue.insert(ticket)
      tickets.append(ticket.pointee)
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
    if Int(r) == messages.count { return tickets}
    throw Exception.UNKNOWN
  }//end send

  public func send(messages: [([Int8], [Int8])]) throws -> [Int64] {
    var tickets = [Int64]()
    if messages.isEmpty { return tickets }
    guard let h = topicHandle else { throw Exception.UNKNOWN }
    let batch = UnsafeMutablePointer<rd_kafka_message_t>.allocate(capacity: messages.count)
    for i in 0 ... messages.count - 1 {
      let p = batch.advanced(by: i)
      let m = messages[i]
      if m.0.count < 1 { continue}

      p.pointee.partition = RD_KAFKA_PARTITION_UA
      p.pointee.payload = malloc(m.0.count)
      #if os(Linux)
        let _ = m.0.withUnsafeBufferPointer { memcpy(p.pointee.payload, $0.baseAddress!, m.0.count) }
      #else
        let _ = m.0.withUnsafeBufferPointer { memcpy(p.pointee.payload, $0.baseAddress, m.0.count) }
      #endif
      p.pointee.len = m.0.count
      sequenceId += 1
      let ticket = UnsafeMutablePointer<Int64>.allocate(capacity: 1)
      ticket.pointee = sequenceId
      queue.insert(ticket)
      tickets.append(ticket.pointee)
      p.pointee._private = unsafeBitCast(ticket, to: UnsafeMutableRawPointer.self)
      if m.1.count > 0 {
        p.pointee.key = malloc(m.1.count)
        #if os(Linux)
          let _ = m.1.withUnsafeBufferPointer { memcpy(p.pointee.key, $0.baseAddress!, m.1.count) }
        #else
          let _ = m.1.withUnsafeBufferPointer { memcpy(p.pointee.key, $0.baseAddress, m.1.count) }
        #endif
        p.pointee.key_len = m.1.count
      } else {
        p.pointee.key = nil
        p.pointee.key_len = 0
      }//end key
    }//next i
    let r = rd_kafka_produce_batch(h, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE, batch, Int32(messages.count))
    batch.deallocate(capacity: messages.count)
    if Int(r) == messages.count { return tickets }
    throw Exception.UNKNOWN
  }//end send
}//end class
