import PackageDescription

#if os(Linux)
let package = Package(
    name: "PerfectKafka",
    dependencies:[
      .Package(url: "https://github.com/PerfectlySoft/Perfect-LinuxBridge.git", majorVersion: 2),
      .Package(url: "https://github.com/PerfectlySoft/Perfect-libKafka.git", majorVersion: 1)
    ]
)
#else
let package = Package(
    name: "PerfectKafka",
    dependencies:[
      .Package(url: "https://github.com/PerfectlySoft/Perfect-libKafka.git", majorVersion: 1)
    ]
)
#endif
