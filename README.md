# Perfect-Kafka [简体中文](README.zh_CN.md)

<p align="center">
    <a href="http://perfect.org/get-involved.html" target="_blank">
        <img src="http://perfect.org/assets/github/perfect_github_2_0_0.jpg" alt="Get Involed with Perfect!" width="854" />
    </a>
</p>

<p align="center">
    <a href="https://github.com/PerfectlySoft/Perfect" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_1_Star.jpg" alt="Star Perfect On Github" />
    </a>  
    <a href="http://stackoverflow.com/questions/tagged/perfect" target="_blank">
        <img src="http://www.perfect.org/github/perfect_gh_button_2_SO.jpg" alt="Stack Overflow" />
    </a>  
    <a href="https://twitter.com/perfectlysoft" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_3_twit.jpg" alt="Follow Perfect on Twitter" />
    </a>  
    <a href="http://perfect.ly" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_4_slack.jpg" alt="Join the Perfect Slack" />
    </a>
</p>

<p align="center">
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Swift-3.0-orange.svg?style=flat" alt="Swift 3.0">
    </a>
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Platforms-OS%20X%20%7C%20Linux%20-lightgray.svg?style=flat" alt="Platforms OS X | Linux">
    </a>
    <a href="http://perfect.org/licensing.html" target="_blank">
        <img src="https://img.shields.io/badge/License-Apache-lightgrey.svg?style=flat" alt="License Apache">
    </a>
    <a href="http://twitter.com/PerfectlySoft" target="_blank">
        <img src="https://img.shields.io/badge/Twitter-@PerfectlySoft-blue.svg?style=flat" alt="PerfectlySoft Twitter">
    </a>
    <a href="http://perfect.ly" target="_blank">
        <img src="http://perfect.ly/badge.svg" alt="Slack Status">
    </a>
</p>



This project provides an express Swift wrapper of librdkafka.

This package builds with Swift Package Manager and is part of the [Perfect](https://github.com/PerfectlySoft/Perfect) project but can also be used as an independent module.

## Release Notes for MacOS X

Before importing this library, please install librdkafka first:

```
$ brew install librdkafka
```

Please also note that a proper pkg-config path setting is required:

```
$ export PKG_CONFIG_PATH="/usr/local/lib/pkgconfig"
```

## Release Notes for Linux

Before importing this library, please install librdkafka-dev first:

```
$ sudo apt-get install librdkafka-dev
```

## Quick Start

### Kafka Client Configurations

Before starting any stream operations, it is necessary to apply settings to clients, i.e., producers or consumers.

Perfect Kafka provides two different categories of configuration, i.e. `Kafka.Config()` for global configurations and `Kafka.TopicConfig()` for topic configurations.

#### Initialization of Global Configurations

To create a configuration set with default value settings, simple call:

``` swift
let conf = try Kafka.Config()
```

or, if another configuration based on an existing one can be also duplicated in such a form:

``` swift
let conf = try Kafka.Config()
// this will keep the original settings and duplicate a new one
let conf2 = try Kafka.Config(conf)
```

#### Initialization of Topic Configurations

Topic configuration shares the same initialization fashion with global configuration.

To create a topic configuration with default settings, call:

``` swift
let conf = try Kafka.TopicConfig()
```

or, if another configuration based on an existing one can be also duplicated in such a form:

``` swift
let conf = try Kafka.TopicConfig()
// this will keep the original settings and duplicate a new one
let conf2 = try Kafka.TopicConfig(conf)
```

### Access Settings of Configuration

Both `Kafka.Config` and `Kafka.TopicConfig` have the same api of accessing settings.

#### List All Variables with Value

`Kafka.Config.properties` and `Kafka.TopicConfig.properties` provides dictionary type settings:

``` swift
// this will print out all variables in a configuration
print(conf.properties)
// for example, it will print out something like:
// ["topic.metadata.refresh.fast.interval.ms": "250",
// "receive.message.max.bytes": "100000000", ...]
```

#### Get a Variable Value

Call `get()` to retrieve the value from a specific variable:

``` swift
let maxBytes = try conf.get("receive.message.max.bytes")
// maxBytes would be "100000000" by default
```

#### Set a Variable with New Value

Call `set()` to save settings for a specific variable:

``` swift
// this will restrict message receiving buffer to 1MB
try conf.set("receive.message.max.bytes", "1048576")
```

## Issues

We are transitioning to using JIRA for all bugs and support related issues, therefore the GitHub issues has been disabled.

If you find a mistake, bug, or any other helpful suggestion you'd like to make on the docs please head over to [http://jira.perfect.org:8080/servicedesk/customer/portal/1](http://jira.perfect.org:8080/servicedesk/customer/portal/1) and raise it.

A comprehensive list of open issues can be found at [http://jira.perfect.org:8080/projects/ISS/issues](http://jira.perfect.org:8080/projects/ISS/issues)

## Further Information
For more information on the Perfect project, please visit [perfect.org](http://perfect.org).
