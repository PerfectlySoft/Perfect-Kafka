import PackageDescription

let package = Package(
    name: "PerfectKafka",
    dependencies:[
      .Package(url: "https://github.com/PerfectlySoft/Perfect-libKafka.git", majorVersion: 1)
    ]
)
