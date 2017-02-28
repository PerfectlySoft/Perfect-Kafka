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
#else
import Darwin
#endif

import XCTest
@testable import PerfectKafka

class PerfectKafkaTests: XCTestCase {
    func testExample() {
        do {
          let k = try Kafka.Conf()
          let dic = k.properties
          print(dic)
          XCTAssertGreaterThan(dic.count, 0)

          let conf = try Kafka.Conf(k)
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
          XCTFail("\(err)")
        }
    }


    static var allTests : [(String, (PerfectKafkaTests) -> () throws -> Void)] {
        return [
            ("testExample", testExample),
        ]
    }
}
