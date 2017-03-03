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

  public func brokerInfo(topicExclusive: Bool = true, timeout: UInt = 1000) throws -> MetaData {
    return try getBrokerInfo(topicHandle: topicExclusive ? topicHandle : nil, timeout: timeout)
  }//end brokerInfo

  public enum Position {
    case BEGIN, END, STORED
    case SPECIFY(Int64)
  }//end Position

  public class Message {
    public var err: Exception = .NO_ERROR
    public var topic = ""
    public var partition: Int32 = 0
    public var isText = false
    public var data = [Int8]()
    public var text = ""
    public var keyIsText = false
    public var keybuf = [Int8]()
    public var key = ""
    public var offset = Int64(-1)
    init () { }
    init(consumer: Consumer, pointer: UnsafeMutablePointer<rd_kafka_message_t>?) {
      guard let h = consumer.topicHandle else {
        err = Exception.UNKNOWN
        return
      }//end guard
      guard let ptr = pointer else {
        err = Exception.UNKNOWN
        return
      }//end guard
      let msg = ptr.pointee
      guard h == msg.rkt else {
        err = Exception.UNKNOWN
        return
      }//end guard
      err = Exception(rawValue: msg.err.rawValue) ?? Exception.UNKNOWN
      topic = consumer.topic
      partition = msg.partition
      if let payloadPointer = msg.payload {
        let pData = unsafeBitCast(payloadPointer, to: UnsafePointer<Int8>.self)
        let pArrayData = UnsafeBufferPointer<Int8>(start: pData, count: msg.len)
        data = Array(pArrayData)
        if let string = String(validatingUTF8: pData) {
          text = string
          isText = true
        } else {
          isText = false
        }//end if
      }//end if

      if let keyPointer = msg.key {
        let pKey = unsafeBitCast(keyPointer, to: UnsafePointer<Int8>.self)
        let pKeyData = UnsafeBufferPointer<Int8>(start: pKey, count: msg.key_len)
        keybuf = Array(pKeyData)
        if let string = String(validatingUTF8: pKey) {
          key = string
          keyIsText = true
        } else {
          keyIsText = false
        }//end 
      }
      offset = msg.offset
      // don't do this, kafka will handle it
      //rd_kafka_message_destroy(pointer)
    }//init
  }//message

  public typealias DeliveryCallback = (Message)-> Void
  public var OnArrival: DeliveryCallback = { _ in }
  
  init(_ topic: String, topicConfig: TopicConfig? = nil, globalConfig: Config? = nil) throws {
    topicName = topic
    try super.init(type: .CONSUMER, config: globalConfig)
    guard let h = rd_kafka_topic_new(_handle, topic, topicConfig == nil ? nil : topicConfig?.conf) else {
        let reason = rd_kafka_errno2err(errno)
        throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
      }//end guard
    topicHandle = h
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

  public func start(_ from: Position = .BEGIN, partition: Int32 = RD_KAFKA_PARTITION_UA) throws {
    guard let h = topicHandle else { throw Exception.UNKNOWN }
    var pos = Int64(0)
    switch (from) {
    case .BEGIN: pos = Int64(RD_KAFKA_OFFSET_BEGINNING)
    case .END: pos = Int64(RD_KAFKA_OFFSET_END)
    case .SPECIFY(let position): pos = position
    default:
      pos = Int64(RD_KAFKA_OFFSET_STORED)
    }//end case
    let r = rd_kafka_consume_start(h, partition, pos)
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

  public func poll(_ timeout: UInt = 10, partition: Int32 = RD_KAFKA_PARTITION_UA) throws -> Int {
    guard let h = topicHandle else { throw Exception.UNKNOWN }
    Consumer.instances[h] = self
    let r = rd_kafka_consume_callback(h, partition, Int32(timeout), { pMsg, ticket in
      guard let pk = ticket else { return }
      let opk = unsafeBitCast(pk, to: OpaquePointer.self)
      guard let k = Consumer.instances[opk] else { return }
      guard let consumer = k as? Consumer else { return }
      let msg = Message(consumer: consumer, pointer: pMsg)
      consumer.OnArrival(msg)
    }, unsafeBitCast(h, to: UnsafeMutableRawPointer.self))
    if r < 0 {
      throw Exception.UNKNOWN
    }//end if
    return Int(r)
  }//end poll
}//end class
