[![Maven Central](https://img.shields.io/maven-central/v/com.github.sazzad16/jedis.svg)](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22com.github.sazzad16%22%20AND%20a%3A%22jedis%22)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE.txt)

# Jedis

This is a fork of [xetorthio/jedis](https://github.com/xetorthio/jedis), a java client for [antirez/redis](https://github.com/antirez/redis).

Jedis is a blazingly small and sane [Redis](https://redis.io/) java client.

Jedis was conceived to be EASY to use.

Jedis is fully compatible with redis 2.8.x, 3.x.x and above*.

*There are still couple of new functionalities added Redis 5 missing in Jedis like Streams.

## So what can I do with Jedis?

All of the following redis features are supported:

- Sorting
- Connection handling
- Commands operating on any kind of values
- Commands operating on string values
- Commands operating on hashes
- Commands operating on lists
- Commands operating on sets
- Commands operating on sorted sets
- Transactions
- Pipelining
- Publish/Subscribe
- Persistence control commands
- Remote server control commands
- Connection pooling
- Sharding (MD5, MurmurHash)
- Key-tags for sharding
- Sharding with pipelining
- Scripting with pipelining
- Redis Cluster

## How do I use it?

You can download the latest build at: 
    https://github.com/sazzad16/jedis/releases

Or use it as a maven dependency:

```xml
<dependency>
    <groupId>com.github.sazzad16</groupId>
    <artifactId>jedis</artifactId>
    <version>2.10.1</version>
</dependency>
```

To use it just:
    
```java
Jedis jedis = new Jedis("localhost");
jedis.set("foo", "bar");
String value = jedis.get("foo");
```

## Jedis Cluster

Redis cluster is implemented.

```java
Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
//Jedis Cluster will attempt to discover cluster nodes automatically
jedisClusterNodes.add(new HostAndPort("127.0.0.1", 7379));
JedisCluster jc = new JedisCluster(jedisClusterNodes);
jc.set("foo", "bar");
String value = jc.get("foo");
```

## License

Copyright (c) 2010 Jonathan Leibiusky

Copyright (c) 2017 Mohammad Sazzadul Hoque

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

