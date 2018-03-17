//
//  PerfectKafkaTests.swift
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
let OS = "Linux"
#else
    import Darwin
let OS = "OS X"
#endif

import XCTest
@testable import PerfectKafka

extension String {
    public var buffer: [Int8] {
        get {
            return self.withCString { ptr -> [Int8]  in
                let buffer = UnsafeBufferPointer(start: ptr, count: self.utf8.count)
                return Array(buffer)
            }//end with
        }//end get
    }//end property
}//end extension


class PerfectKafkaTests: XCTestCase {
    
    let hosts = "nut.krb5.ca:9092"
    let topic = "testing"
    
    func testConfig() {
        do {
            let k = try Kafka.Config()
            let dic = k.properties
            print(dic)
            XCTAssertGreaterThan(dic.count, 0)
            
            let conf = try Kafka.Config(k)
            let d2 = conf.properties
            print(d2)
            
            let testKey = "fetch.wait.max.ms"
            var fwait = try conf.get(testKey)
            print(fwait)
            let testValue = "200"
            try conf.set(testKey, value: testValue)
            fwait =  try conf.get(testKey)
            print(fwait)
            XCTAssertEqual(testValue, fwait)
        }catch(let err) {
            XCTFail("Config \(err)")
        }
    }
    
    func testTopicConfig() {
        do {
            let config = try Kafka.TopicConfig()
            let conf = try Kafka.TopicConfig(config)
            let dic = conf.properties
            print(dic)
            XCTAssertGreaterThan(dic.count, 0)
            let testKey = "request.timeout.ms"
            var fwait = try conf.get(testKey)
            print(fwait)
            let testValue = "4000"
            try conf.set(testKey, value: testValue)
            fwait =  try conf.get(testKey)
            print(fwait)
            XCTAssertEqual(testValue, fwait)
        }catch(let err) {
            XCTFail("Topic Config \(err)")
        }
    }
    
    func testProducer () {
        do {
            let producer = try Producer(topic)
            producer.OnError = { XCTFail("producer error: \($0)") }
            producer.OnSent = { print("message \($0) has been sent") }
            let brokers = producer.connect(brokers: hosts)
            XCTAssertGreaterThan(brokers, 0)
            var now = time(nil)
            let _ = try producer.send(message: "\(OS) message test \(now)")
            var messages = [(String, String?)]()
            for i in 1 ... 10 {
                messages.append(("\(OS) 字符串批量 #\(i) -> \(now)", nil))
            }//next
            var r = try producer.send(messages: messages)
            XCTAssertEqual(r.count, messages.count)
            
            print("       --------     binaries    ----------")
            now = time(nil)
            let _ = try producer.send(message: "\(OS) binary data test \(now)".buffer)
            var data = [([Int8], [Int8])]()
            for i in 1 ... 10 {
                data.append(("\(OS) 二进制批量流 #\(i) -> \(now)".buffer, [Int8]()))
            }//next
            r = try producer.send(messages: data)
            XCTAssertEqual(r.count, messages.count)
            
            producer.flush(1)
            print(try producer.brokerInfo())
        }catch(let err) {
            XCTFail("Producer \(err)")
        }
    }
    
    func testConsumer () {
        do {
            let consumer = try Consumer(topic)
            var stack = [Consumer.Message()]
            consumer.OnArrival = { m in
                print("message arrival #\(m.offset) \(m.text)")
                stack.append(m)
            }//end on received
            let brokers = consumer.connect(brokers: hosts)
            XCTAssertGreaterThan(brokers, 0)
            try consumer.start(partition: 0)
            var total = 0
            while (total < 20) {
                total += try consumer.poll(partition: 0)
            }//end while
            XCTAssertLessThanOrEqual(total, stack.count)
            print(try consumer.brokerInfo())
        }catch(let err) {
            XCTFail("Consumer \(err)")
        }
    }
    static var allTests : [(String, (PerfectKafkaTests) -> () throws -> Void)] {
        return [
            ("testConfig", testConfig),
            ("testTopicConfig", testTopicConfig),
            ("testProducer", testProducer),
            ("testConsumer", testConsumer)
        ]
    }
}
