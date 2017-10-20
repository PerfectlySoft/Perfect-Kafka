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

/// Consumer client based on Kafka base client
public class Consumer: Kafka {

  /// the inner topic handle for C api
  internal var topicHandle: OpaquePointer? = nil

  /// topic name, internally
  internal var topicName = ""

  /// topic name, read only
  public var topic: String { get { return topicName } }

  /// get broker meta data info
  /// - parameters:
  ///   - topicExclusive: Bool, true for gathering meta data only for the current topic, otherwise will return all topics instead
  ///   - timeout: UInt, milliseconds to wait for meta data gathering, default value is 1 second
  /// - returns:
  ///   See MetaData
  /// - throws:
  ///   Exception
  public func brokerInfo(topicExclusive: Bool = true, timeout: UInt = 1000) throws -> MetaData {
    return try getBrokerInfo(topicHandle: topicExclusive ? topicHandle : nil, timeout: timeout)
  }//end brokerInfo

  /// message offset position in a topic queue
  public enum Position {
    /// from the very beginning of the topic
    case BEGIN,
    /// from the end of topic message queue
    END,
    /// from a stored message in queue
    STORED
    /// specific position by giving the exact offset
    case SPECIFY(Int64)
  }//end Position

  /// Message to consume
  public class Message {

    /// Error: if the message is good or not
    public var err: Exception = .NO_ERROR

    /// topic name of the message
    public var topic = ""

    /// partition of the message
    public var partition: Int32 = 0

    /// if the message is a valid UTF-8 text
    public var isText = false

    /// the original binary data of message body
    public var data = [Int8]()

    /// decoded message body in a UTF-8 string
    public var text = ""

    /// if the key is a valid UTF-8 text
    public var keyIsText = false

    /// the original binary data of optional key
    public var keybuf = [Int8]()

    /// decoded key in a UTF-8 string
    public var key = ""

    /// offset inside the topic
    public var offset = Int64(-1)

    /// default constructor - just create an empty message
    public init () { }

    /// constructor that initialize from a librdkafka message pointer
    /// - parameters:
    ///   - consumer: Consumer, the consumer client that hold the topic info
    ///   - pointer: librdkafka message pointer
    public init(consumer: Consumer, pointer: UnsafeMutablePointer<rd_kafka_message_t>?) {

      // get the topic handle
      guard let h = consumer.topicHandle else {
        err = Exception.UNKNOWN
        return
      }//end guard

      // get the pointer
      guard let ptr = pointer else {
        err = Exception.UNKNOWN
        return
      }//end guard

      // get the message body
      let msg = ptr.pointee
      guard h == msg.rkt else {
        err = Exception.UNKNOWN
        return
      }//end guard

      // convert a librdkafka message into a Swift style
      err = Exception(rawValue: msg.err.rawValue) ?? Exception.UNKNOWN
      topic = consumer.topic
      partition = msg.partition

      // save message pointer into [Int8]
      if let payloadPointer = msg.payload {
        let pData = payloadPointer.assumingMemoryBound(to: Int8.self)
        let pArrayData = UnsafeBufferPointer<Int8>(start: pData, count: msg.len)
        data = Array(pArrayData)

        // convert it into a string if possible
        if let string = String(validatingUTF8: pData) {
          text = string
          isText = true
        } else {
          isText = false
        }//end if
      }//end if

      // save the key pointer into [Int8]
      if let keyPointer = msg.key {
        let pKey = keyPointer.assumingMemoryBound(to: Int8.self)
        let pKeyData = UnsafeBufferPointer<Int8>(start: pKey, count: msg.key_len)
        keybuf = Array(pKeyData)

        // convert it into a string if possible
        if let string = String(validatingUTF8: pKey) {
          key = string
          keyIsText = true
        } else {
          keyIsText = false
        }//end
      }//end if

      // set the message offset in topic
      offset = msg.offset

      // don't do this, kafka will handle it
      //rd_kafka_message_destroy(pointer)
    }//init
  }//message

  /// function callback of delivery, the only parameter is the message body that just arrived
  public typealias DeliveryCallback = (Message)-> Void
  public var OnArrival: DeliveryCallback = { _ in }

  /// consumer constructor
  /// - parameters:
  ///   - topic: String, name of topic to create or connect to
  ///   - topicConfig: TopicConfig? configuration for this topic, nil to use the default setting.
  ///   - globalConfig: Config? configuration for global kafka setting, nil to use the default setting.
  /// - throws:
  ///   Exception
  public init(_ topic: String, topicConfig: TopicConfig? = nil, globalConfig: Config? = nil) throws {

    // set the topic name
    topicName = topic

    // call the Kafka base client to create a consumer
    try super.init(type: .CONSUMER, config: globalConfig)

    // create a topic or connect to the existing one
    guard let h = rd_kafka_topic_new(_handle, topic, topicConfig == nil ? nil : topicConfig?.conf) else {
        let reason = rd_kafka_errno2err(errno)
        throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
      }//end guard
    topicHandle = h
  }//end init

  /// destructor
  deinit {
    guard let h = topicHandle else { return }

    /// must stop the consumer processs before release resources
    let _ = rd_kafka_consume_stop(h, RD_KAFKA_PARTITION_UA)
    rd_kafka_topic_destroy(h)
  }//end

  /// store a specific message
  /// - parameters:
  ///   - offset: Int64, message Id in a topic
  ///   - partition: Int32, message from which partition
  /// - throws:
  ///   Exception
  public func store(_ offset: Int64, partition: Int32 = RD_KAFKA_PARTITION_UA) throws {
    guard let h = topicHandle else { throw Exception.UNKNOWN }
    let reason = rd_kafka_offset_store(h, partition, offset)
    if reason.rawValue == Exception.NO_ERROR.rawValue { return }
    throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
  }//end store

  /// start downloading messages, must call `connect()` before downloading
  /// - parameters:
  ///   - from: Position, message offset position in a topic, see definition of Position
  ///   - partition: Int32, partition info of the topic to download message
  /// - throws:
  ///   Exception
  public func start(_ from: Position = .BEGIN, partition: Int32 = RD_KAFKA_PARTITION_UA) throws {
    guard let h = topicHandle else { throw Exception.UNKNOWN }

    // validate the position info and turn it into librdkafka position format
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

  /// stop downloading the messages
  /// - parameters:
  ///   - partition: Int32, partition info of the topic to download message
  /// - throws:
  ///   Exception
  public func stop (_ partition: Int32 = RD_KAFKA_PARTITION_UA) throws {
    guard let h = topicHandle else { return }
    let r = rd_kafka_consume_stop(h, partition)
    if r == 0 { return }
    let reason = rd_kafka_errno2err(errno)
    throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
  }//end stop

  /// poll the messages from broker; once called, the message will arrive and call `OnArrival()`
  /// - parameters:
  ///   - timout: UInt, milliseconds to wait for polling
  ///   - partition: Int32, partition info of the topic to download message
  /// - throws:
  ///   Exception
  /// - returns:
  ///   numbers of messages that downloaded during this period
  public func poll(_ timeout: UInt = 10, partition: Int32 = RD_KAFKA_PARTITION_UA) throws -> Int {
    guard let h = topicHandle else { throw Exception.UNKNOWN }

    // get the instance of calling
    Consumer.instances[h] = self

    // call the polling and pass the instance for callbacks
    let r = rd_kafka_consume_callback(h, partition, Int32(timeout), { pMsg, ticket in

      // once message arrived, turn it into Consumer.Message and call the `OnArrival()`
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
