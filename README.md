## Introduction

DataKernel is an open-source Java framework, which is comprised of a collection of components useful for creating high-load server systems. For instance, [benchmarks show](http://datakernel.io/docs/http/#benchmark) that our HTTP-server outperforms nginx (with a default configuration), in some cases delivering twice as many requests per second.

The essential components of DataKernel forms the basis of our ad-serving infrastructure at [AdKernel](http://adkernel.com), running in production environments and processing billions of requests. Specifically, DataKernel is the foundation for systems that provide real-time analytics for ad publishers and advertisers, user data processing tools that we use for ad targeting, and also web crawlers that perform content indexing on a large scale.

## Foundation components

* [Eventloop](http://datakernel.io/docs/modules/eventloop.html) Efficient non-blocking network and file I/O, for building Node.js-like client/server applications with high performance requirements.

## Core components

* [HTTP](http://datakernel.io/docs/modules/http.html) High-performance asynchronous HTTP client and server. [Benchmark](http://datakernel.io/docs/modules/http.html/#benchmark)
* [Async Streams](http://datakernel.io/docs/modules/streams.html) Composable asynchronous/reactive streams with powerful data processing capabilities. [Benchmark](http://datakernel.io/docs/modules/streams.html#benchmark)
* [Serializer](http://datakernel.io/docs/modules/serializer.html) Extremely fast and space-efficient serializers, crafted using bytecode engineering. [Benchmark](http://datakernel.io/docs/modules/serializer.html#benchmark)
* [Codegen](http://datakernel.io/docs/modules/codegen.html) Expression-based fluent API on top of ObjectWeb ASM for runtime generation of POJOs, mappers and reducers, etc.

## Cluster components

* [RPC](http://datakernel.io/docs/modules/rpc.html) High-performance and fault-tolerant remote procedure call module for building distributed applications. [Benchmark](http://datakernel.io/docs/modules/rpc.html#benchmark)
* [AggregationDB](http://datakernel.io/docs/modules/aggregation-db.html) Unique database with the possibility to define custom aggregate functions.
* [OLAP Cube](http://datakernel.io/docs/modules/cube.html) Specialized OLAP database for multidimensional data analytics.
* [RemoteFS](http://datakernel.io/docs/modules/remotefs.html) Basis for building efficient, scalable remote file servers.

## Integration components

* [Boot](http://datakernel.io/docs/modules/boot.html) An intelligent way of booting complex applications and services according to their dependencies.
* [UIKernel](http://datakernel.io/docs/modules/uikernel.html) Integration with UIKernel.io JS frontend library: JSON serializers, grid model, basic servlets.