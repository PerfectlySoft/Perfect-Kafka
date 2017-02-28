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
#else
import Darwin
#endif

import ckafka

public class Kafka {

  public static let szString = 1024

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

  public class TopicConfig {
    internal var conf: OpaquePointer

    init (_ configuration: TopicConfig? = nil) throws {
      if let config = configuration {
        guard let cnf = rd_kafka_topic_conf_dup(config.conf) else {
          throw Exception.UNKNOWN
        }//end guard
        conf = cnf
      }else {
        guard let cnf = rd_kafka_topic_conf_new() else {
          throw Exception.UNKNOWN
        }
        conf = cnf
      }//end if
    }//end init

    deinit {
      rd_kafka_topic_conf_destroy(conf)
    }//end deconstruction

    public var properties: [String: String] {
      get {
        var dic: [String:String] = [:]
        var cnt = 0
        guard let array = rd_kafka_topic_conf_dump(conf, &cnt) else {
          return dic
        }//end guard
        if cnt < 1 {
          return dic
        }//end if
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

    public func `get` (_ variable: String) throws -> String {
        guard let value = properties[variable] else {
          throw Exception.UNKNOWN
        }//end guard
        return value
    }//end get

    public func `set` (_ variable: String, value: String) throws {
      let r = rd_kafka_topic_conf_set(conf, variable, value, nil, 0)
      guard r.rawValue == Exception.NO_ERROR.rawValue else {
        throw Exception(rawValue: r.rawValue)!
      }//end guard
    }//end set
  }

  public class Config {
    internal var conf: OpaquePointer

    init (_ configuration: Config? = nil) throws {
      if let config = configuration {
        guard let cnf = rd_kafka_conf_dup(config.conf) else {
          throw Exception.UNKNOWN
        }//end guard
        conf = cnf
      }else {
        guard let cnf = rd_kafka_conf_new() else {
          throw Exception.UNKNOWN
        }
        conf = cnf
      }//end if
    }//end init

    deinit {
      rd_kafka_conf_destroy(conf)
    }//end deconstruction

    public var properties: [String: String] {
      get {
        var dic: [String:String] = [:]
        var cnt = 0
        guard let array = rd_kafka_conf_dump(conf, &cnt) else {
          return dic
        }//end guard
        if cnt < 1 {
          return dic
        }//end if
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
      let r = rd_kafka_conf_get(conf, variable, value, &size)
      guard r.rawValue == Exception.NO_ERROR.rawValue else {
        value.deallocate(capacity: Kafka.szString)
        throw Exception(rawValue: r.rawValue)!
      }//end guard
      let valueString = String(cString: value)
      value.deallocate(capacity: Kafka.szString)
      return valueString
    #endif
    }//end get

    public func `set` (_ variable: String, value: String) throws {
      let r = rd_kafka_conf_set(conf, variable, value, nil, 0)
      guard r.rawValue == Exception.NO_ERROR.rawValue else {
        throw Exception(rawValue: r.rawValue)!
      }//end guard
    }//end set
  }//end Config
}
