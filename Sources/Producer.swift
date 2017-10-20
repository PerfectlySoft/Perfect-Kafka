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

/// Kafka Client Producer, based on Kafka Client base.
public class Producer: Kafka {

  /// inner handler of topic
  internal var topicHandle: OpaquePointer? = nil

  /// topic name
  internal var topicName = ""

  /// topic name, read only
  public var topic: String { get { return topicName } }

  /// serial number for each message sent by this instance of producer, for internal usage
  internal var sequenceId:Int64 = 0

  /// message numbers that represent all messages in sending.
  internal var queue = Set<UnsafeMutablePointer<Int64>>()

  /// function type of delivery callback, with the sequence id of message that sent as parameter
  public typealias DeliveryCallback = (Int64) -> Void

  /// delivery call back function
  public var OnSent: DeliveryCallback = { _ in }

  /// get broker meta data info
  /// - parameters:
  ///   - topicExclusive: Bool, true for gathering meta data only for the current topic, otherwise will return all topics instead
  ///   - timeout: UInt, milliseconds to wait for meta data gathering, default value is 1 second
  /// - returns:
  ///   See MetaData
  /// - throws:
  ///   Exception
  public func brokerInfo(topicExclusive: Bool = true, timeout: UInt = 1000) throws -> MetaData {

    /// call the inner base client version
    return try getBrokerInfo(topicHandle: topicExclusive ? topicHandle : nil, timeout: timeout)
  }//end brokerInfo

  /// pop a message from the queue, used by C api callback. *NOTE* do not use for end users.
  /// - parameters:
  ///   - msgId: UnsafeMutableRawPointer? pointer of Int64 message id
  public func pop(_ msgId: UnsafeMutableRawPointer?) {

    // convert the input parameter into an Int64 pointer
    guard let ticket = msgId else { return }
    let t = ticket.assumingMemoryBound(to: Int64.self)

    // remove it from the sending queue
    queue.remove(t)

    // release resources
    let id = t.pointee
    t.deallocate(capacity: 1)

    // call the onSent callback
    OnSent(id)
  }//end pop

  /// producer constructor
  /// - parameters:
  ///   - topic: String, name of topic to create or connect to
  ///   - topicConfig: TopicConfig? configuration for this topic, nil to use the default setting.
  ///   - globalConfig: Config? configuration for global kafka setting, nil to use the default setting.
  /// - throws:
  ///   Exception
  public init(_ topic: String, topicConfig: TopicConfig? = nil, globalConfig: Config? = nil) throws {

    // set the topic
    topicName = topic

    // load global configuration and create the default setting if nil
    let gConf = try ( globalConfig ?? (try Config()))

    // set the delivery callback
    rd_kafka_conf_set_dr_cb(gConf.conf, { rk, _, _, _, _, ticket in

      // get kafka instance as producer
      guard let pk = rk else { return }
      guard let k = Producer.instances[pk] else { return }
      guard let producer = k as? Producer else { return }

      // pop out the sent message id
      producer.pop(ticket)
    })

    // set the error callback
    rd_kafka_conf_set_error_cb(gConf.conf, { rk, _, reason, _ in

      // get kafka instance as producer
      guard let pk = rk else { return }
      guard let k = Producer.instances[pk] else { return }
      guard let r = reason else { return }

      // call the error
      k.OnError(String(cString: r))
    })

    // call the base kafka client to generate a producer with the newly created configuration
    try super.init(type: .PRODUCER, config: gConf)

    // create a new topic; or connect to the existing topic
    guard let h = rd_kafka_topic_new(_handle, topic, topicConfig == nil ? nil : topicConfig?.conf) else {
      let reason = rd_kafka_errno2err(errno)
      throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
    }//end guard

    // set the C api handle
    topicHandle = h

    // deposit the newly created instance into management pool
    Producer.instances[_handle] = self
  }//end init

  /// get out going messages, as an array of Int64 message id
  public var outbox: [Int64] { get {
    return queue.map { $0.pointee }.sorted()
    }//end get
  }//end toSend

  /// wait a while for message sending, for synchronizing purposes only
  /// - parameters:
  ///   - timeout: UInt, milliseconds to wait
  public func flush(_ timeout: UInt) {
    rd_kafka_poll(_handle, Int32(timeout))
  }//end flush

  /// destructor
  deinit {

    guard let h = topicHandle else { return }
    // there may be some messages in queue waiting to send, so wait for at least one second
    flush(1)
    rd_kafka_topic_destroy(h)

    // abortion: release all messages that not sent
    queue.forEach { $0.deallocate(capacity: 1) }
  }//end

  /// send a message with an optional key
  /// - parameters:
  ///   - message: String, message to send
  ///   - key: String?, an optional key for this message
  /// - returns:
  ///   an Int64 ticket that represents the message to send, could be found in outbox or callback onSent
  /// - throws:
  ///   Exception
  @discardableResult
  public func send(message: String, key: String? = nil) throws -> Int64 {

    // get the topic handle first
    guard let h = topicHandle else { throw Exception.UNKNOWN }

    var r:Int32 = 0

    // increase the sequence id
    sequenceId += 1

    // create a pointer ticket for this message
    let ticket = UnsafeMutablePointer<Int64>.allocate(capacity: 1)

    // assign the value of pointer into the increased message serial number
    ticket.pointee = sequenceId

    // send a message with the optional key by duplicate the message body and librdkafka will release it once sent automatically.
    if let k = key {
      r = rd_kafka_produce(h, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE, strdup(message), message.utf8.count, k, k.utf8.count, ticket)
    }else{
      r = rd_kafka_produce(h, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE, strdup(message), message.utf8.count, nil, 0, ticket)
    }//end if

    // if success, append the ticket into the queue and return the ticket
    if r == 0 {
      queue.insert(ticket)
      return ticket.pointee
    }//end if

    // otherwise release resources if failed
    ticket.deallocate(capacity: 1)
    let reason = rd_kafka_errno2err(errno)
    throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
  }//end send

  /// send a message with an optional key, in binary form
  /// - parameters:
  ///   - message: [Int8], a binary message to send
  ///   - key: [Int8], an optional binary key for this message
  /// - returns:
  ///   an Int64 ticket that represents the message to send, could be found in outbox or callback onSent
  /// - throws:
  ///   Exception
  @discardableResult
  public func send(message: [Int8], key: [Int8] = []) throws -> Int64 {

    // get the topic handle first
    guard let h = topicHandle else { throw Exception.UNKNOWN }

    // return if nothing to send
    if message.count < 1 { return -1 }

    var r:Int32 = 0

    // generate the serial number for this new message
    sequenceId += 1

    // create a pointer for this new serial number
    let ticket = UnsafeMutablePointer<Int64>.allocate(capacity: 1)
    ticket.pointee = sequenceId

    // create a buffer to copy the binary message which will be deleted automatically once sent.
    let buffer = malloc(message.count)
    #if os(Linux)
      let _ = message.withUnsafeBufferPointer { memcpy(buffer!, $0.baseAddress!, message.count) }
    #else
      let _ = message.withUnsafeBufferPointer { memcpy(buffer, $0.baseAddress, message.count) }
    #endif

    // create buffer to copy the optional key which will be deleted automatically by librdkfaka once sent
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

    // if success, append the ticket into the queue and return the ticket
    if r == 0 {
      queue.insert(ticket)
      return ticket.pointee
    }//end if

    // otherwise release resources if failed
    ticket.deallocate(capacity: 1)
    let reason = rd_kafka_errno2err(errno)
    throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
  }//end send

  /// send messages with optional keys
  /// - parameters:
  ///   - messages: [(message: String, key: String?)], message to send, in an array of message / key tuple and will be sent by the array order.
  /// - returns:
  ///   [Int64] tickets that represent the messages to send, could be found in outbox or callback onSent
  /// - throws:
  ///   Exception
  @discardableResult
  public func send(messages: [(String, String?)]) throws -> [Int64] {

    // prepare a ticket box
    var tickets = [Int64]()

    // return immediately if nothing to send
    if messages.isEmpty { return tickets }

    // get the topic
    guard let h = topicHandle else { throw Exception.UNKNOWN }

    // create a message box pointer
    let batch = UnsafeMutablePointer<rd_kafka_message_t>.allocate(capacity: messages.count)

    // save all messages with optional keys into the pointers
    for i in 0 ... messages.count - 1 {
      let p = batch.advanced(by: i)
      let m = messages[i]

      // mark all messages to random partitions
      p.pointee.partition = RD_KAFKA_PARTITION_UA

      // duplicate the message because it would be automatically released by librdkafka
      p.pointee.payload = unsafeBitCast(strdup(m.0), to: UnsafeMutableRawPointer.self)
      p.pointee.len = m.0.utf8.count

      // generate ticket for each message
      sequenceId += 1
      let ticket = UnsafeMutablePointer<Int64>.allocate(capacity: 1)
      ticket.pointee = sequenceId

      // append ticket to the outbox queue
      queue.insert(ticket)
      tickets.append(ticket.pointee)

      // pass the instance to callback
      p.pointee._private = UnsafeMutableRawPointer(ticket)

      // add the optional key
      if let key = m.1 {
        p.pointee.key = unsafeBitCast(strdup(key), to: UnsafeMutableRawPointer.self)
        p.pointee.key_len = key.utf8.count
      } else {
        p.pointee.key = nil
        p.pointee.key_len = 0
      }//end key
    }//next i

    // send the message batch
    let r = rd_kafka_produce_batch(h, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE, batch, Int32(messages.count))

    // release intermediate pointer allocated
    batch.deallocate(capacity: messages.count)

    // return the tickets if success
    if Int(r) == messages.count { return tickets}

    // or failed
    throw Exception.UNKNOWN
  }//end send

  /// send messages with optional keys, in binary form
  /// - parameters:
  ///   - messages: [(message:[Int8], key: [Int8])], binary messages to send, in an array of message / key tuple and will be sent by the array order.
  /// - returns:
  ///   [Int64] tickets that represent the messages to send, could be found in outbox or callback onSent
  /// - throws:
  ///   Exception
  public func send(messages: [([Int8], [Int8])]) throws -> [Int64] {

    // prepare a ticket box
    var tickets = [Int64]()

    // return immediately if nothing to send
    if messages.isEmpty { return tickets }

    // get the topic
    guard let h = topicHandle else { throw Exception.UNKNOWN }

    // create a message box pointer
    let batch = UnsafeMutablePointer<rd_kafka_message_t>.allocate(capacity: messages.count)

    // save all messages with optional keys into the pointers
    for i in 0 ... messages.count - 1 {
      let p = batch.advanced(by: i)
      let m = messages[i]
      if m.0.count < 1 { continue}

      // mark all messages to random partitions
      p.pointee.partition = RD_KAFKA_PARTITION_UA

      // duplicate the message because it would be automatically released by librdkafka
      p.pointee.payload = malloc(m.0.count)
      #if os(Linux)
        let _ = m.0.withUnsafeBufferPointer { memcpy(p.pointee.payload, $0.baseAddress!, m.0.count) }
      #else
        let _ = m.0.withUnsafeBufferPointer { memcpy(p.pointee.payload, $0.baseAddress, m.0.count) }
      #endif
      p.pointee.len = m.0.count

      // generate ticket for each message
      sequenceId += 1
      let ticket = UnsafeMutablePointer<Int64>.allocate(capacity: 1)
      ticket.pointee = sequenceId
      queue.insert(ticket)
      tickets.append(ticket.pointee)

      // pass the instance to callback
      p.pointee._private = UnsafeMutableRawPointer(ticket)

      // add the optional key
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

    // send the message batch
    let r = rd_kafka_produce_batch(h, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE, batch, Int32(messages.count))

    // release intermediate pointer allocated
    batch.deallocate(capacity: messages.count)

    // return the tickets if success
    if Int(r) == messages.count { return tickets }

    // or failed
    throw Exception.UNKNOWN
  }//end send
}//end class
