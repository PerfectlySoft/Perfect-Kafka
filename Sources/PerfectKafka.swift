//
//  PerfectKafka.swift
//  Perfect-Kafka
//
//  Created by Rockford Wei on 2017-02-28.
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
#if os(Linux)
  import SwiftGlibc
  import LinuxBridge

var errno: Int32 {
  return linux_errno()
  }//end var
#else
  import Darwin
#endif


import ckafka

/// Base Class of Kafka Client. Don't use this class directly! Use Producer or Consumer instead.
public class Kafka {

  /// size required for C api string conversion
  public static let szString = 1024

  /// Kafka 0.8 only support two types of clients
  public enum `Type` { case PRODUCER, CONSUMER }

  /// only effective when initial failure with specific error information
  public enum Failure: Error { case INIT(String) }

  /// inner handle for C api
  internal var _handle: OpaquePointer

  /// inner record for global config
  internal var _config: Config

  /// Instance hash table for management of better and safer C pointer applications and callbacks
  public static var instances:[OpaquePointer: Kafka] = [:]

  /// Function type of Error Callback, with the only parameter of error message
  public typealias ErrorCallback = (String) -> Void

  /// Error Callback Function. Users should customize this function for error control
  public var OnError:ErrorCallback = { _ in }


  /// Kafka Errors, directly copy from librdkafka 0.10
  public enum Exception: Int32, Error {
    /* Internal errors to rdkafka: */
    ///  Begin internal error codes
    case _BEGIN = -200
    ///  Received message is incorrect
    case _BAD_MSG = -199
    ///  Bad/unknown compression
    case _BAD_COMPRESSION = -198
    ///  Broker is going away
    case _DESTROY = -197
    ///  Generic failure
    case _FAIL = -196
    ///  Broker transport failure
    case _TRANSPORT = -195
    ///  Critical system resource
    case _CRIT_SYS_RESOURCE = -194
    ///  Failed to resolve broker
    case _RESOLVE = -193
    ///  Produced message timed out
    case _MSG_TIMED_OUT = -192
    /** Reached the end of the topic+partition queue on
     * the broker. Not really an error. */
    case _PARTITION_EOF = -191
    ///  Permanent: Partition does not exist in cluster.
    case _UNKNOWN_PARTITION = -190
    ///  File or filesystem error
    case _FS = -189
    ///  Permanent: Topic does not exist in cluster.
    case _UNKNOWN_TOPIC = -188
    ///  All broker connections are down.
    case _ALL_BROKERS_DOWN = -187
    ///  Invalid argument, or invalid configuration
    case _INVALID_ARG = -186
    ///  Operation timed out
    case _TIMED_OUT = -185
    ///  Queue is full
    case _QUEUE_FULL = -184
    ///  ISR count < required.acks
    case _ISR_INSUFF = -183
    ///  Broker node update
    case _NODE_UPDATE = -182
    ///  SSL error
    case _SSL = -181
    ///  Waiting for coordinator to become available.
    case _WAIT_COORD = -180
    ///  Unknown client group
    case _UNKNOWN_GROUP = -179
    ///  Operation in progress
    case _IN_PROGRESS = -178
    ///  Previous operation in progress, wait for it to finish.
    case _PREV_IN_PROGRESS = -177
    ///  This operation would interfere with an existing subscription
    case _EXISTING_SUBSCRIPTION = -176
    ///  Assigned partitions (rebalance_cb)
    case _ASSIGN_PARTITIONS = -175
    ///  Revoked partitions (rebalance_cb)
    case _REVOKE_PARTITIONS = -174
    ///  Conflicting use
    case _CONFLICT = -173
    ///  Wrong state
    case _STATE = -172
    ///  Unknown protocol
    case _UNKNOWN_PROTOCOL = -171
    ///  Not implemented
    case _NOT_IMPLEMENTED = -170
    ///  Authentication failure
    case _AUTHENTICATION = -169
    ///  No stored offset
    case _NO_OFFSET = -168
    ///  Outdated
    case _OUTDATED = -167
    ///  Timed out in queue
    case _TIMED_OUT_QUEUE = -166
    ///  Feature not supported by broker
    case _UNSUPPORTED_FEATURE = -165
    ///  Awaiting cache update
    case _WAIT_CACHE = -164

    ///  End internal error codes
    case _END = -100

    /* Kafka broker errors: */
    ///  Unknown broker error
    case UNKNOWN = -1
    ///  Success
    case NO_ERROR = 0
    ///  Offset out of range
    case OFFSET_OUT_OF_RANGE = 1
    ///  Invalid message
    case INVALID_MSG = 2
    ///  Unknown topic or partition
    case UNKNOWN_TOPIC_OR_PART = 3
    ///  Invalid message size
    case INVALID_MSG_SIZE = 4
    ///  Leader not available
    case LEADER_NOT_AVAILABLE = 5
    ///  Not leader for partition
    case NOT_LEADER_FOR_PARTITION = 6
    ///  Request timed out
    case REQUEST_TIMED_OUT = 7
    ///  Broker not available
    case BROKER_NOT_AVAILABLE = 8
    ///  Replica not available
    case REPLICA_NOT_AVAILABLE = 9
    ///  Message size too large
    case MSG_SIZE_TOO_LARGE = 10
    ///  StaleControllerEpochCode
    case STALE_CTRL_EPOCH = 11
    ///  Offset metadata string too large
    case OFFSET_METADATA_TOO_LARGE = 12
    ///  Broker disconnected before response received
    case NETWORK_EXCEPTION = 13
    ///  Group coordinator load in progress
    case GROUP_LOAD_IN_PROGRESS = 14
    ///  Group coordinator not available
    case GROUP_COORDINATOR_NOT_AVAILABLE = 15
    ///  Not coordinator for group
    case NOT_COORDINATOR_FOR_GROUP = 16
    ///  Invalid topic
    case TOPIC_EXCEPTION = 17
    ///  Message batch larger than configured server segment size
    case RECORD_LIST_TOO_LARGE = 18
    ///  Not enough in-sync replicas
    case NOT_ENOUGH_REPLICAS = 19
    ///  Message(s) written to insufficient number of in-sync replicas
    case NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20
    ///  Invalid required acks value
    case INVALID_REQUIRED_ACKS = 21
    ///  Specified group generation id is not valid
    case ILLEGAL_GENERATION = 22
    ///  Inconsistent group protocol
    case INCONSISTENT_GROUP_PROTOCOL = 23
    ///  Invalid group.id
    case INVALID_GROUP_ID = 24
    ///  Unknown member
    case UNKNOWN_MEMBER_ID = 25
    ///  Invalid session timeout
    case INVALID_SESSION_TIMEOUT = 26
    ///  Group rebalance in progress
    case REBALANCE_IN_PROGRESS = 27
    ///  Commit offset data size is not valid
    case INVALID_COMMIT_OFFSET_SIZE = 28
    ///  Topic authorization failed
    case TOPIC_AUTHORIZATION_FAILED = 29
    ///  Group authorization failed
    case GROUP_AUTHORIZATION_FAILED = 30
    ///  Cluster authorization failed
    case CLUSTER_AUTHORIZATION_FAILED = 31
    ///  Invalid timestamp
    case INVALID_TIMESTAMP = 32
    ///  Unsupported SASL mechanism
    case UNSUPPORTED_SASL_MECHANISM = 33
    ///  Illegal SASL state
    case ILLEGAL_SASL_STATE = 34
    ///  Unuspported version
    case UNSUPPORTED_VERSION = 35
    ///  Topic already exists
    case TOPIC_ALREADY_EXISTS = 36
    ///  Invalid number of partitions
    case INVALID_PARTITIONS = 37
    ///  Invalid replication factor
    case INVALID_REPLICATION_FACTOR = 38
    ///  Invalid replica assignment
    case INVALID_REPLICA_ASSIGNMENT = 39
    ///  Invalid config
    case INVALID_CONFIG = 40
    ///  Not controller for cluster
    case NOT_CONTROLLER = 41
    ///  Invalid request
    case INVALID_REQUEST = 42
    ///  Message format on broker does not support request
    case UNSUPPORTED_FOR_MESSAGE_FORMAT = 43
    /// All other errors
    case END_ALL = 44
  }//end

  /// Topic Configuration
  public class TopicConfig {

    /// inner handle for C api
    internal var conf: OpaquePointer

    /// constructor of Topic Configuration
    /// - parameters:
    ///   - configuration: TopicConfig? .  nil for new configuration; or duplicate it if not nil
    public init (_ configuration: TopicConfig? = nil) throws {

      // check if the original configuration is available
      if let config = configuration {

        // if available, duplicate it.
        guard let cnf = rd_kafka_topic_conf_dup(config.conf) else {
          throw Exception.UNKNOWN
        }//end guard

        // set handler
        conf = cnf
      }else {

        // if not avaialble, create a new one.
        guard let cnf = rd_kafka_topic_conf_new() else {
          throw Exception.UNKNOWN
        }//end guard

        // set handler
        conf = cnf
      }//end if
    }//end init

    deinit {
      // don't destroy it, producer / consumer will release it themselves.
      //rd_kafka_topic_conf_destroy(conf)
    }//end deconstruction

    /// list all keys and values in a configuration as a dictionary, read only.
    public var properties: [String: String] {
      get {

        // prepare an empty dictionary for the configuration.
        var dic: [String:String] = [:]
        var cnt = 0

        // load the configuration into a C pointer array
        guard let array = rd_kafka_topic_conf_dump(conf, &cnt) else {
          return dic
        }//end guard

        // return if empty
        if cnt < 1 {
          return dic
        }//end if

        // key and values are pair in the same C pointer array
        for i in 0 ... cnt / 2 {
          guard let k = array.advanced(by: i * 2).pointee,
            let v = array.advanced(by: i * 2 + 1).pointee
            else {
              break
          }//end guard

          // save the key & value to the new dictionary
          let key = String(cString: k)
          let value = String(cString: v)
          dic[key] = value
        }//next

        // release resources.
        rd_kafka_conf_dump_free(array, cnt)

        // return the new created dictionary
        return dic
      }//end get
    }//end properties

    /// get a variable value from the current configuration
    /// - parameters:
    ///   - variable: String for a valid variable name
    /// - returns:
    ///   value as String.
    /// - throws:
    ///   Exception
    public func `get` (_ variable: String) throws -> String {
      guard let value = properties[variable] else { throw Exception.UNKNOWN }
      return value
    }//end get

    /// set a variable value to the current configuration
    /// - parameters:
    ///   - variable: String for a valid variable name
    ///   - value: String value to set of this variable
    /// - throws:
    ///   Exception
    public func `set` (_ variable: String, value: String) throws {
      let reason = rd_kafka_topic_conf_set(conf, variable, value, nil, 0)
      guard reason.rawValue == Exception.NO_ERROR.rawValue else {
        throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
      }//end guard
    }//end set
  }

  /// Global Configuration Class
  public class Config {

    /// internal handle for C api
    internal var conf: OpaquePointer

    /// constructor of configuration class
    /// - parameters:
    ///   - configuration: Config? nil for creating new instance, otherwise will duplicate
    /// - throws:
    ///   Exception
    public init (_ configuration: Config? = nil) throws {

      // check if not nil
      if let config = configuration {

        // duplicate the current instance
        guard let cnf = rd_kafka_conf_dup(config.conf) else {
          throw Exception.UNKNOWN
        }//end guard

        // set the handler
        conf = cnf
      }else {

        // otherwise create a new instance
        guard let cnf = rd_kafka_conf_new() else {
          throw Exception.UNKNOWN
        }//end guard

        // set the handler
        conf = cnf
      }//end if
    }//end init

    /// deconstructor
    deinit {
      // don't destroy it, producer / consumer will release it themselves.
      // rd_kafka_conf_destroy(conf)
    }//end deconstruction

    /// list all keys and values in a configuration as a dictionary, read only.
    public var properties: [String: String] {
      get {

        // prepare an empty dictionary
        var dic: [String:String] = [:]
        var cnt = 0

        // retrieve all variables
        guard let array = rd_kafka_conf_dump(conf, &cnt) else {
          return dic
        }//end guard
        if cnt < 1 {
          return dic
        }//end if

        // save all keys and values into the dictionary
        for i in 0 ... cnt / 2 {
          guard let k = array.advanced(by: i * 2).pointee,
            let v = array.advanced(by: i * 2 + 1).pointee
          else {
            break
          }//end guard
          let key = String(cString: k)
          let value = String(cString: v)
          dic[key] = value
        }//next
        rd_kafka_conf_dump_free(array, cnt)
        return dic
      }//end get
    }//end properties

    /// get a variable value from the current configuration
    /// - parameters:
    ///   - variable: String for a valid variable name
    /// - returns:
    ///   value as String.
    /// - throws:
    ///   Exception
    public func `get` (_ variable: String) throws -> String {
      /*
        The following code is reserved for librdkafka 0.9 and above
        until Feb 28, 2017, Ubuntu 16.04 distribution is only 0.8
        watch for updates; if librdkafka-dev was upgraded, then please remove
        the linux conditional code
       */
    #if os(Linux)
      guard let value = properties[variable] else {
        throw Exception.UNKNOWN
      }//end guard
      return value
    #else
      var size = Kafka.szString
      let value = UnsafeMutablePointer<CChar>.allocate(capacity: size)
      let reason = rd_kafka_conf_get(conf, variable, value, &size)
      guard reason.rawValue == Exception.NO_ERROR.rawValue else {
        value.deallocate(capacity: Kafka.szString)
        throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
      }//end guard
      let valueString = String(cString: value)
      value.deallocate(capacity: Kafka.szString)
      return valueString
    #endif
    }//end get

    /// set a variable value to the current configuration
    /// - parameters:
    ///   - variable: String for a valid variable name
    ///   - value: String value to set of this variable
    /// - throws:
    ///   Exception
    public func `set` (_ variable: String, value: String) throws {
      let reason = rd_kafka_conf_set(conf, variable, value, nil, 0)
      guard reason.rawValue == Exception.NO_ERROR.rawValue else {
        throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
      }//end guard
    }//end set
  }//end Config

  /// constructor of Kafka client base class
  /// - parameters:
  ///   - type: Type of Kafka client, must be either producer or consumer
  ///   - config: Config? global configuration, nil for new instance creation (default)
  public init(type: `Type`, config: Config? = nil) throws {

    // determine the client type
    let kType = type == .PRODUCER ? RD_KAFKA_PRODUCER : RD_KAFKA_CONSUMER

    // check if need to create new configuration by default
    if config == nil {
      _config = try Config()
    }else {
      _config = config!
    }//end if

    // create new client instance
    let errstr = UnsafeMutablePointer<CChar>.allocate(capacity: Kafka.szString)
    guard let h = rd_kafka_new(kType, _config.conf, errstr, Kafka.szString) else {
      let e = String(cString: errstr)
      errstr.deallocate(capacity: Kafka.szString)
      throw Failure.INIT(e)
    }//end guard
    errstr.deallocate(capacity: Kafka.szString)
    _handle = h

  }//end init

  /// destructor of Kafka base client
  deinit { rd_kafka_destroy(_handle) }

  /// name of the client
  public var name: String {
    get {
      guard let n = rd_kafka_name(_handle) else { return "" }
      return String(cString: n)
    }//end get
  }//end name

  /// connect to brokers
  /// - parameter:
  ///   - brokers: String, in form of "host:port,...", e.g., "host1:9092,host2:9092,host3:9092"
  /// - returns:
  ///   quantity of brokers that connected.
  public func connect(brokers: String) -> Int {
    return Int(rd_kafka_brokers_add(_handle, brokers))
  }//end add

  /// connect to brokers
  /// - parameter:
  ///   - brokers: [String], in form of ["host:port"], e.g., ["host1:9092","host2:9092","host3:9092"]
  /// - returns:
  ///   quantity of brokers that connected.
  public func connect(brokers: [String]) -> Int {
    return connect(brokers: brokers.joined(separator: ","))
  }//end func add

  /// connect to brokers
  /// - parameter:
  ///   - brokers: [String: Int], in form of ["host":port], e.g., ["host1":9092,"host2":9092,"host3":9092]
  /// - returns:
  ///   quantity of brokers that connected.
  public func connect(brokers: [String: Int]) -> Int {
    return connect(brokers: brokers.reduce("") { $0.isEmpty ? "\($1.key):\($1.value)" : "\($0),\($1.key):\($1.value)" })
  }//end func add

  /// Broker structure, a part of MetaData
  public struct Broker {
    /// Broker Id
    public var id = 0
    /// Host name of the broker
    public var host = ""
    /// Host port that listens
    public var port = 0
  }//end Broker

  /// Partition structure, a part of MetaData
  public struct Partition {
    /// Partition Id
    public var id = 0
    /// Partition error reported by broker
    public var err = Exception.NO_ERROR
    /// Leader broker
    public var leader = 0
    /// Replica brokers
    public var replicas = [Int]()
    /// In-Sync-Replica brokers
    public var isrs = [Int]()
  }//end Partition

  /// Topic structure, a part of MetaData
  public struct Topic {
    /// Topic name
    public var name = ""
    /// Topic error reported by broker
    public var err = Exception.NO_ERROR
    /// Partitions of this topic
    public var partitions = [Partition]()
  }//end Topic

  /// Meta data for broker report
  public struct MetaData {
    /// Brokers that connected
    public var brokers = [Broker]()
    /// Available topics
    public var topics = [Topic]()
    /// Broker originating this metadata
    public var origBrokerId = 0
    /// Name of originating broker
    public var origBrokerName = ""
  }//end MetaData

  /// internal function for getting meta data from a broker
  /// - parameters:
  ///   - topicHandle: OpaquePointer?, nil for all topics, otherwise for specific topic.
  ///   - timeout: UInt, timeout for retrieving information, in milli seconds. Default is 1 second.
  /// - returns
  ///   MetaData
  /// - throws
  ///   Exception
  @discardableResult
  internal func getBrokerInfo(topicHandle: OpaquePointer?, timeout: UInt = 1000) throws -> MetaData {

    // prepare an empty meta data container
    var m = MetaData()

    // prepare the pointer to hold the data returned
    let ppMeta = UnsafeMutablePointer<UnsafePointer<rd_kafka_metadata>?>.allocate(capacity: 1)

    // C api return value
    var reason: rd_kafka_resp_err_t

    // if handle is valid, then try to gather information only for this specific handle
    if let handle = topicHandle {

      // get meta data by assigning the specific topic handle
      reason = rd_kafka_metadata(_handle, 0, handle, ppMeta, Int32(timeout))
    } else {

      // otherwise should collect all possible topics from the same broker
      reason = rd_kafka_metadata(_handle, 1, nil, ppMeta, Int32(timeout))
    }//end if

    // check return value for errors
    guard reason.rawValue == Exception.NO_ERROR.rawValue else {
      ppMeta.deallocate(capacity: 1)
      throw Exception(rawValue: reason.rawValue) ?? Exception.UNKNOWN
    }//end guard

    // get the newly allocated meta data pointer
    guard let pMeta = ppMeta.pointee else {
      throw Exception.UNKNOWN
    }//end guard

    // get the meta data structure
    let d = pMeta.pointee

    // translate C structure to Swift structure

    // set originated broker id
    m.origBrokerId = Int(d.orig_broker_id)

    // set originated broker name
    m.origBrokerName = String(cString: d.orig_broker_name)

    // save broker info into an array
    for i in 0 ... d.broker_cnt - 1 {

      // jump to the right pointer
      let b = d.brokers.advanced(by: Int(i)).pointee
      let broker = Broker(id: Int(b.id), host: String(cString:b.host), port: Int(b.port))
      m.brokers.append(broker)
    }//next i

    // walk through all topics
    for i in 0 ... d.topic_cnt - 1 {

      // jump to the right pointer
      let t = d.topics.advanced(by: Int(i)).pointee

      // create an empty topic to store data
      var topic = Topic()

      // set topic basic info
      topic.name = String(cString: t.topic)
      topic.err = Exception(rawValue: t.err.rawValue) ?? Exception.UNKNOWN

      // get all partitions for the specific topic
      for j in 0 ... t.partition_cnt - 1 {
        let p = t.partitions.advanced(by: Int(j)).pointee

        // create an empty partition to store data
        var part = Partition()

        // get partition id
        part.id = Int(p.id)

        // get partition errors
        part.err = Exception(rawValue: p.err.rawValue) ?? Exception.UNKNOWN

        // get partition leader
        part.leader = Int(p.leader)

        // get partition replicas
        for k in 0 ... p.replica_cnt - 1 {
          let replica = p.replicas.advanced(by: Int(k)).pointee
          part.replicas.append(Int(replica))
        }//next k

        // get ISR brokers in this partition
        for k in 0 ... p.isr_cnt - 1 {
          let isr = p.isrs.advanced(by: Int(k)).pointee
          part.isrs.append(Int(isr))
        }//next i

        // save partition info into topic data structure
        topic.partitions.append(part)
      }//next j

      // add the newly allocated topic to meta data return struture
      m.topics.append(topic)
    }//next i

    // release intermediate resources
    rd_kafka_metadata_destroy(pMeta)
    ppMeta.deallocate(capacity: 1)

    // return the meta data expected
    return m
  }//end fun
}//end class
