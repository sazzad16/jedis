package redis.clients.jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import redis.clients.jedis.commands.BinaryRedisPipeline;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.commands.RedisPipeline;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;
import redis.clients.jedis.params.LPosParams;

public abstract class PipelineBase<J extends JedisBase> extends AbstractPipelining<J> implements BinaryRedisPipeline, RedisPipeline {

  public PipelineBase(J resource) {
    super(resource);
  }

  @Override
  public Response<Long> append(final String key, final String value) {
    Consumer<Client> consumer = client -> client.append(key, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> append(final byte[] key, final byte[] value) {
    Consumer<Client> consumer = client -> client.append(key, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<List<String>> blpop(final String key) {
    String[] temp = new String[1];
    temp[0] = key;
    Consumer<Client> consumer = client -> client.blpop(temp);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<List<String>> brpop(final String key) {
    String[] temp = new String[1];
    temp[0] = key;
    Consumer<Client> consumer = client -> client.brpop(temp);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<List<byte[]>> blpop(final byte[] key) {
    byte[][] temp = new byte[1][];
    temp[0] = key;
    Consumer<Client> consumer = client -> client.blpop(temp);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);
  }

  @Override
  public Response<List<byte[]>> brpop(final byte[] key) {
    byte[][] temp = new byte[1][];
    temp[0] = key;
    Consumer<Client> consumer = client -> client.brpop(temp);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);
  }

  @Override
  public Response<Long> decr(final String key) {
    Consumer<Client> consumer = client -> client.decr(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> decr(final byte[] key) {
    Consumer<Client> consumer = client -> client.decr(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> decrBy(final String key, final long decrement) {
    Consumer<Client> consumer = client -> client.decrBy(key, decrement);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> decrBy(final byte[] key, final long decrement) {
    Consumer<Client> consumer = client -> client.decrBy(key, decrement);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> del(final String key) {
    Consumer<Client> consumer = client -> client.del(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> del(final byte[] key) {
    Consumer<Client> consumer = client -> client.del(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> unlink(final String key) {
    Consumer<Client> consumer = client -> client.unlink(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> unlink(final byte[] key) {
    Consumer<Client> consumer = client -> client.unlink(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> echo(final String string) {
    Consumer<Client> consumer = client -> client.echo(string);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<byte[]> echo(final byte[] string) {
    Consumer<Client> consumer = client -> client.echo(string);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<Boolean> exists(final String key) {
    Consumer<Client> consumer = client -> client.exists(key);
    return addBatch(consumer, BuilderFactory.BOOLEAN);
  }

  @Override
  public Response<Boolean> exists(final byte[] key) {
    Consumer<Client> consumer = client -> client.exists(key);
    return addBatch(consumer, BuilderFactory.BOOLEAN);
  }

  @Override
  public Response<Long> expire(final String key, final int seconds) {
    Consumer<Client> consumer = client -> client.expire(key, seconds);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> expire(final byte[] key, final int seconds) {
    Consumer<Client> consumer = client -> client.expire(key, seconds);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> expireAt(final String key, final long unixTime) {
    Consumer<Client> consumer = client -> client.expireAt(key, unixTime);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> expireAt(final byte[] key, final long unixTime) {
    Consumer<Client> consumer = client -> client.expireAt(key, unixTime);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> get(final String key) {
    Consumer<Client> consumer = client -> client.get(key);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<byte[]> get(final byte[] key) {
    Consumer<Client> consumer = client -> client.get(key);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<Boolean> getbit(final String key, final long offset) {
    Consumer<Client> consumer = client -> client.getbit(key, offset);
    return addBatch(consumer, BuilderFactory.BOOLEAN);
  }

  @Override
  public Response<Boolean> getbit(final byte[] key, final long offset) {
    Consumer<Client> consumer = client -> client.getbit(key, offset);
    return addBatch(consumer, BuilderFactory.BOOLEAN);
  }

  @Override
  public Response<Long> bitpos(final String key, final boolean value) {
    return bitpos(key, value, new BitPosParams());
  }

  @Override
  public Response<Long> bitpos(final String key, final boolean value, final BitPosParams params) {
    Consumer<Client> consumer = client -> client.bitpos(key, value, params);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> bitpos(final byte[] key, final boolean value) {
    return bitpos(key, value, new BitPosParams());
  }

  @Override
  public Response<Long> bitpos(final byte[] key, final boolean value, final BitPosParams params) {
    Consumer<Client> consumer = client -> client.bitpos(key, value, params);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> getrange(final String key, final long startOffset, final long endOffset) {
    Consumer<Client> consumer = client -> client.getrange(key, startOffset, endOffset);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> getSet(final String key, final String value) {
    Consumer<Client> consumer = client -> client.getSet(key, value);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<byte[]> getSet(final byte[] key, final byte[] value) {
    Consumer<Client> consumer = client -> client.getSet(key, value);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<byte[]> getrange(final byte[] key, final long startOffset, final long endOffset) {
    Consumer<Client> consumer = client -> client.getrange(key, startOffset, endOffset);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<Long> hdel(final String key, final String... field) {
    Consumer<Client> consumer = client -> client.hdel(key, field);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> hdel(final byte[] key, final byte[]... field) {
    Consumer<Client> consumer = client -> client.hdel(key, field);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Boolean> hexists(final String key, final String field) {
    Consumer<Client> consumer = client -> client.hexists(key, field);
    return addBatch(consumer, BuilderFactory.BOOLEAN);
  }

  @Override
  public Response<Boolean> hexists(final byte[] key, final byte[] field) {
    Consumer<Client> consumer = client -> client.hexists(key, field);
    return addBatch(consumer, BuilderFactory.BOOLEAN);
  }

  @Override
  public Response<String> hget(final String key, final String field) {
    Consumer<Client> consumer = client -> client.hget(key, field);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<byte[]> hget(final byte[] key, final byte[] field) {
    Consumer<Client> consumer = client -> client.hget(key, field);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<Map<String, String>> hgetAll(final String key) {
    Consumer<Client> consumer = client -> client.hgetAll(key);
    return addBatch(consumer, BuilderFactory.STRING_MAP);
  }

  @Override
  public Response<Map<byte[], byte[]>> hgetAll(final byte[] key) {
    Consumer<Client> consumer = client -> client.hgetAll(key);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_MAP);
  }

  @Override
  public Response<Long> hincrBy(final String key, final String field, final long value) {
    Consumer<Client> consumer = client -> client.hincrBy(key, field, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> hincrBy(final byte[] key, final byte[] field, final long value) {
    Consumer<Client> consumer = client -> client.hincrBy(key, field, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Set<String>> hkeys(final String key) {
    Consumer<Client> consumer = client -> client.hkeys(key);
    return addBatch(consumer, BuilderFactory.STRING_SET);
  }

  @Override
  public Response<Set<byte[]>> hkeys(final byte[] key) {
    Consumer<Client> consumer = client -> client.hkeys(key);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Long> hlen(final String key) {
    Consumer<Client> consumer = client -> client.hlen(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> hlen(final byte[] key) {
    Consumer<Client> consumer = client -> client.hlen(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<List<String>> hmget(final String key, final String... fields) {
    Consumer<Client> consumer = client -> client.hmget(key, fields);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<List<byte[]>> hmget(final byte[] key, final byte[]... fields) {
    Consumer<Client> consumer = client -> client.hmget(key, fields);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);
  }

  @Override
  public Response<String> hmset(final String key, final Map<String, String> hash) {
    Consumer<Client> consumer = client -> client.hmset(key, hash);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> hmset(final byte[] key, final Map<byte[], byte[]> hash) {
    Consumer<Client> consumer = client -> client.hmset(key, hash);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Long> hset(final String key, final String field, final String value) {
    Consumer<Client> consumer = client -> client.hset(key, field, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> hset(final byte[] key, final byte[] field, final byte[] value) {
    Consumer<Client> consumer = client -> client.hset(key, field, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> hset(final String key, final Map<String, String> hash) {
    Consumer<Client> consumer = client -> client.hset(key, hash);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> hset(final byte[] key, final Map<byte[], byte[]> hash) {
    Consumer<Client> consumer = client -> client.hset(key, hash);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> hsetnx(final String key, final String field, final String value) {
    Consumer<Client> consumer = client -> client.hsetnx(key, field, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> hsetnx(final byte[] key, final byte[] field, final byte[] value) {
    Consumer<Client> consumer = client -> client.hsetnx(key, field, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<List<String>> hvals(final String key) {
    Consumer<Client> consumer = client -> client.hvals(key);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<List<byte[]>> hvals(final byte[] key) {
    Consumer<Client> consumer = client -> client.hvals(key);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);
  }

  @Override
  public Response<Long> incr(final String key) {
    Consumer<Client> consumer = client -> client.incr(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> incr(final byte[] key) {
    Consumer<Client> consumer = client -> client.incr(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> incrBy(final String key, final long increment) {
    Consumer<Client> consumer = client -> client.incrBy(key, increment);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> incrBy(final byte[] key, final long increment) {
    Consumer<Client> consumer = client -> client.incrBy(key, increment);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> lindex(final String key, final long index) {
    Consumer<Client> consumer = client -> client.lindex(key, index);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<byte[]> lindex(final byte[] key, final long index) {
    Consumer<Client> consumer = client -> client.lindex(key, index);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<Long> linsert(final String key, final ListPosition where, final String pivot, final String value) {
    Consumer<Client> consumer = client -> client.linsert(key, where, pivot, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> linsert(final byte[] key, final ListPosition where, final byte[] pivot, final byte[] value) {
    Consumer<Client> consumer = client -> client.linsert(key, where, pivot, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> llen(final String key) {
    Consumer<Client> consumer = client -> client.llen(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> llen(final byte[] key) {
    Consumer<Client> consumer = client -> client.llen(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> lpop(final String key) {
    Consumer<Client> consumer = client -> client.lpop(key);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<byte[]> lpop(final byte[] key) {
    Consumer<Client> consumer = client -> client.lpop(key);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<List<String>> lpop(final String key, final int count) {
    Consumer<Client> consumer = client -> client.lpop(key, count);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<List<byte[]>> lpop(final byte[] key, final int count) {
    Consumer<Client> consumer = client -> client.lpop(key, count);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);
  }

  @Override
  public Response<Long> lpos(final String key, final String element) {
    Consumer<Client> consumer = client -> client.lpos(key, element);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> lpos(final byte[] key, final byte[] element) {
    Consumer<Client> consumer = client -> client.lpos(key, element);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> lpos(final String key, final String element, final LPosParams params) {
    Consumer<Client> consumer = client -> client.lpos(key, element, params);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> lpos(final byte[] key, final byte[] element, final LPosParams params) {
    Consumer<Client> consumer = client -> client.lpos(key, element, params);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<List<Long>> lpos(final String key, final String element, final LPosParams params, final long count) {
    Consumer<Client> consumer = client -> client.lpos(key, element, params, count);
    return addBatch(consumer, BuilderFactory.LONG_LIST);
  }

  @Override
  public Response<List<Long>> lpos(final byte[] key, final byte[] element, final LPosParams params, final long count) {
    Consumer<Client> consumer = client -> client.lpos(key, element, params, count);
    return addBatch(consumer, BuilderFactory.LONG_LIST);
  }

  @Override
  public Response<Long> lpush(final String key, final String... string) {
    Consumer<Client> consumer = client -> client.lpush(key, string);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> lpush(final byte[] key, final byte[]... string) {
    Consumer<Client> consumer = client -> client.lpush(key, string);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> lpushx(final String key, final String... string) {
    Consumer<Client> consumer = client -> client.lpushx(key, string);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> lpushx(final byte[] key, final byte[]... bytes) {
    Consumer<Client> consumer = client -> client.lpushx(key, bytes);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<List<String>> lrange(final String key, final long start, final long stop) {
    Consumer<Client> consumer = client -> client.lrange(key, start, stop);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<List<byte[]>> lrange(final byte[] key, final long start, final long stop) {
    Consumer<Client> consumer = client -> client.lrange(key, start, stop);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);
  }

  @Override
  public Response<Long> lrem(final String key, final long count, final String value) {
    Consumer<Client> consumer = client -> client.lrem(key, count, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> lrem(final byte[] key, final long count, final byte[] value) {
    Consumer<Client> consumer = client -> client.lrem(key, count, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> lset(final String key, final long index, final String value) {
    Consumer<Client> consumer = client -> client.lset(key, index, value);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> lset(final byte[] key, final long index, final byte[] value) {
    Consumer<Client> consumer = client -> client.lset(key, index, value);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> ltrim(final String key, final long start, final long stop) {
    Consumer<Client> consumer = client -> client.ltrim(key, start, stop);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> ltrim(final byte[] key, final long start, final long stop) {
    Consumer<Client> consumer = client -> client.ltrim(key, start, stop);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Long> move(final String key, final int dbIndex) {
    Consumer<Client> consumer = client -> client.move(key, dbIndex);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> move(final byte[] key, final int dbIndex) {
    Consumer<Client> consumer = client -> client.move(key, dbIndex);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> persist(final String key) {
    Consumer<Client> consumer = client -> client.persist(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> persist(final byte[] key) {
    Consumer<Client> consumer = client -> client.persist(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> rpop(final String key) {
    Consumer<Client> consumer = client -> client.rpop(key);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<byte[]> rpop(final byte[] key) {
    Consumer<Client> consumer = client -> client.rpop(key);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<List<String>> rpop(final String key, final int count) {
    Consumer<Client> consumer = client -> client.rpop(key, count);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<List<byte[]>> rpop(final byte[] key, final int count) {
    Consumer<Client> consumer = client -> client.rpop(key, count);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);
  }

  @Override
  public Response<Long> rpush(final String key, final String... string) {
    Consumer<Client> consumer = client -> client.rpush(key, string);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> rpush(final byte[] key, final byte[]... string) {
    Consumer<Client> consumer = client -> client.rpush(key, string);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> rpushx(final String key, final String... string) {
    Consumer<Client> consumer = client -> client.rpushx(key, string);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> rpushx(final byte[] key, final byte[]... string) {
    Consumer<Client> consumer = client -> client.rpushx(key, string);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sadd(final String key, final String... member) {
    Consumer<Client> consumer = client -> client.sadd(key, member);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sadd(final byte[] key, final byte[]... member) {
    Consumer<Client> consumer = client -> client.sadd(key, member);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> scard(final String key) {
    Consumer<Client> consumer = client -> client.scard(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> scard(final byte[] key) {
    Consumer<Client> consumer = client -> client.scard(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> set(final String key, final String value) {
    Consumer<Client> consumer = client -> client.set(key, value);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> set(final byte[] key, final byte[] value) {
    Consumer<Client> consumer = client -> client.set(key, value);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> set(final String key, final String value, SetParams params) {
    Consumer<Client> consumer = client -> client.set(key, value, params);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> set(final byte[] key, final byte[] value, SetParams params) {
    Consumer<Client> consumer = client -> client.set(key, value, params);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Boolean> setbit(final String key, final long offset, boolean value) {
    Consumer<Client> consumer = client -> client.setbit(key, offset, value);
    return addBatch(consumer, BuilderFactory.BOOLEAN);
  }

  @Override
  public Response<Boolean> setbit(final byte[] key, final long offset, final byte[] value) {
    Consumer<Client> consumer = client -> client.setbit(key, offset, value);
    return addBatch(consumer, BuilderFactory.BOOLEAN);
  }

  @Override
  public Response<String> setex(final String key, final int seconds, final String value) {
    Consumer<Client> consumer = client -> client.setex(key, seconds, value);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> setex(final byte[] key, final int seconds, final byte[] value) {
    Consumer<Client> consumer = client -> client.setex(key, seconds, value);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Long> setnx(final String key, final String value) {
    Consumer<Client> consumer = client -> client.setnx(key, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> setnx(final byte[] key, final byte[] value) {
    Consumer<Client> consumer = client -> client.setnx(key, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> setrange(final String key, final long offset, final String value) {
    Consumer<Client> consumer = client -> client.setrange(key, offset, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> setrange(final byte[] key, final long offset, final byte[] value) {
    Consumer<Client> consumer = client -> client.setrange(key, offset, value);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Boolean> sismember(final String key, final String member) {
    Consumer<Client> consumer = client -> client.sismember(key, member);
    return addBatch(consumer, BuilderFactory.BOOLEAN);
  }

  @Override
  public Response<List<Boolean>> smismember(final String key, final String... members) {
    Consumer<Client> consumer = client -> client.smismember(key, members);
    return addBatch(consumer, BuilderFactory.BOOLEAN_LIST);
  }

  @Override
  public Response<Boolean> sismember(final byte[] key, final byte[] member) {
    Consumer<Client> consumer = client -> client.sismember(key, member);
    return addBatch(consumer, BuilderFactory.BOOLEAN);
  }

  @Override
  public Response<List<Boolean>> smismember(final byte[] key, final byte[]... members) {
    Consumer<Client> consumer = client -> client.smismember(key, members);
    return addBatch(consumer, BuilderFactory.BOOLEAN_LIST);
  }

  @Override
  public Response<Set<String>> smembers(final String key) {
    Consumer<Client> consumer = client -> client.smembers(key);
    return addBatch(consumer, BuilderFactory.STRING_SET);
  }

  @Override
  public Response<Set<byte[]>> smembers(final byte[] key) {
    Consumer<Client> consumer = client -> client.smembers(key);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<List<String>> sort(final String key) {
    Consumer<Client> consumer = client -> client.sort(key);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<List<byte[]>> sort(final byte[] key) {
    Consumer<Client> consumer = client -> client.sort(key);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);
  }

  @Override
  public Response<List<String>> sort(final String key, final SortingParams sortingParameters) {
    Consumer<Client> consumer = client -> client.sort(key, sortingParameters);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<List<byte[]>> sort(final byte[] key, final SortingParams sortingParameters) {
    Consumer<Client> consumer = client -> client.sort(key, sortingParameters);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);
  }

  @Override
  public Response<String> spop(final String key) {
    Consumer<Client> consumer = client -> client.spop(key);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Set<String>> spop(final String key, final long count) {
    Consumer<Client> consumer = client -> client.spop(key, count);
    return addBatch(consumer, BuilderFactory.STRING_SET);
  }

  @Override
  public Response<byte[]> spop(final byte[] key) {
    Consumer<Client> consumer = client -> client.spop(key);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<Set<byte[]>> spop(final byte[] key, final long count) {
    Consumer<Client> consumer = client -> client.spop(key, count);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<String> srandmember(final String key) {
    Consumer<Client> consumer = client -> client.srandmember(key);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<List<String>> srandmember(final String key, final int count) {
    Consumer<Client> consumer = client -> client.srandmember(key, count);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<byte[]> srandmember(final byte[] key) {
    Consumer<Client> consumer = client -> client.srandmember(key);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<List<byte[]>> srandmember(final byte[] key, final int count) {
    Consumer<Client> consumer = client -> client.srandmember(key, count);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);
  }

  @Override
  public Response<Long> srem(final String key, final String... member) {
    Consumer<Client> consumer = client -> client.srem(key, member);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> srem(final byte[] key, final byte[]... member) {
    Consumer<Client> consumer = client -> client.srem(key, member);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> strlen(final String key) {
    Consumer<Client> consumer = client -> client.strlen(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> strlen(final byte[] key) {
    Consumer<Client> consumer = client -> client.strlen(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> substr(final String key, final int start, final int end) {
    Consumer<Client> consumer = client -> client.substr(key, start, end);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> substr(final byte[] key, final int start, final int end) {
    Consumer<Client> consumer = client -> client.substr(key, start, end);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Long> touch(final String key) {
    Consumer<Client> consumer = client -> client.touch(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> touch(final byte[] key) {
    Consumer<Client> consumer = client -> client.touch(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> ttl(final String key) {
    Consumer<Client> consumer = client -> client.ttl(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> ttl(final byte[] key) {
    Consumer<Client> consumer = client -> client.ttl(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> type(final String key) {
    Consumer<Client> consumer = client -> client.type(key);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> type(final byte[] key) {
    Consumer<Client> consumer = client -> client.type(key);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Long> zadd(final String key, final double score, final String member) {
    Consumer<Client> consumer = client -> client.zadd(key, score, member);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zadd(final String key, final double score, final String member, final ZAddParams params) {
    Consumer<Client> consumer = client -> client.zadd(key, score, member, params);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zadd(final String key, final Map<String, Double> scoreMembers) {
    Consumer<Client> consumer = client -> client.zadd(key, scoreMembers);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zadd(final String key, final Map<String, Double> scoreMembers, final ZAddParams params) {
    Consumer<Client> consumer = client -> client.zadd(key, scoreMembers, params);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zadd(final byte[] key, final double score, final byte[] member) {
    Consumer<Client> consumer = client -> client.zadd(key, score, member);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zadd(final byte[] key, final double score, final byte[] member, final ZAddParams params) {
    Consumer<Client> consumer = client -> client.zadd(key, score, member, params);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zadd(final byte[] key, final Map<byte[], Double> scoreMembers) {
    Consumer<Client> consumer = client -> client.zadd(key, scoreMembers);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zadd(final byte[] key, final Map<byte[], Double> scoreMembers, final ZAddParams params) {
    Consumer<Client> consumer = client -> client.zadd(key, scoreMembers, params);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zcard(final String key) {
    Consumer<Client> consumer = client -> client.zcard(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zcard(final byte[] key) {
    Consumer<Client> consumer = client -> client.zcard(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zcount(final String key, final double min, final double max) {
    Consumer<Client> consumer = client -> client.zcount(key, min, max);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zcount(final String key, final String min, final String max) {
    Consumer<Client> consumer = client -> client.zcount(key, min, max);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zcount(final byte[] key, final double min, final double max) {
    Consumer<Client> consumer = client -> client.zcount(key, min, max);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zcount(final byte[] key, final byte[] min, final byte[] max) {
    Consumer<Client> consumer = client -> client.zcount(key, min, max);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Double> zincrby(final String key, final double increment, final String member) {
    Consumer<Client> consumer = client -> client.zincrby(key, increment, member);
    return addBatch(consumer, BuilderFactory.DOUBLE);
  }

  @Override
  public Response<Double> zincrby(final String key, final double increment, final String member, ZIncrByParams params) {
    Consumer<Client> consumer = client -> client.zincrby(key, increment, member, params);
    return addBatch(consumer, BuilderFactory.DOUBLE);
  }

  @Override
  public Response<Double> zincrby(final byte[] key, final double increment, final byte[] member) {
    Consumer<Client> consumer = client -> client.zincrby(key, increment, member);
    return addBatch(consumer, BuilderFactory.DOUBLE);
  }

  @Override
  public Response<Double> zincrby(final byte[] key, final double increment, final byte[] member, ZIncrByParams params) {
    Consumer<Client> consumer = client -> client.zincrby(key, increment, member);
    return addBatch(consumer, BuilderFactory.DOUBLE);
  }

  @Override
  public Response<Set<String>> zrange(final String key, final long start, final long stop) {
    Consumer<Client> consumer = client -> client.zrange(key, start, stop);
    return addBatch(consumer, BuilderFactory.STRING_ZSET);
  }

  @Override
  public Response<Set<byte[]>> zrange(final byte[] key, final long start, final long stop) {
    Consumer<Client> consumer = client -> client.zrange(key, start, stop);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Set<String>> zrangeByScore(final String key, final double min, final double max) {
    Consumer<Client> consumer = client -> client.zrangeByScore(key, min, max);
    return addBatch(consumer, BuilderFactory.STRING_ZSET);
  }

  @Override
  public Response<Set<byte[]>> zrangeByScore(final byte[] key, final double min, final double max) {
    Consumer<Client> consumer = client -> client.zrangeByScore(key, min, max);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Set<String>> zrangeByScore(final String key, final String min, final String max) {
    Consumer<Client> consumer = client -> client.zrangeByScore(key, min, max);
    return addBatch(consumer, BuilderFactory.STRING_ZSET);
  }

  @Override
  public Response<Set<byte[]>> zrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
    Consumer<Client> consumer = client -> client.zrangeByScore(key, min, max);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Set<String>> zrangeByScore(final String key, final double min, final double max, final int offset,
      final int count) {
    Consumer<Client> consumer = client -> client.zrangeByScore(key, min, max, offset, count);
    return addBatch(consumer, BuilderFactory.STRING_ZSET);
  }

  @Override
  public Response<Set<String>> zrangeByScore(final String key, final String min, final String max, final int offset,
      final int count) {
    Consumer<Client> consumer = client -> client.zrangeByScore(key, min, max, offset, count);
    return addBatch(consumer, BuilderFactory.STRING_ZSET);
  }

  @Override
  public Response<Set<byte[]>> zrangeByScore(final byte[] key, final double min, final double max, final int offset,
      final int count) {
    Consumer<Client> consumer = client -> client.zrangeByScore(key, min, max, offset, count);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Set<byte[]>> zrangeByScore(final byte[] key, final byte[] min, final byte[] max, final int offset,
      final int count) {
    Consumer<Client> consumer = client -> client.zrangeByScore(key, min, max, offset, count);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrangeByScoreWithScores(final String key, final double min, final double max) {
    Consumer<Client> consumer = client -> client.zrangeByScoreWithScores(key, min, max);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrangeByScoreWithScores(final String key, final String min, final String max) {
    Consumer<Client> consumer = client -> client.zrangeByScoreWithScores(key, min, max);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key, final double min, final double max) {
    Consumer<Client> consumer = client -> client.zrangeByScoreWithScores(key, min, max);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max) {
    Consumer<Client> consumer = client -> client.zrangeByScoreWithScores(key, min, max);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrangeByScoreWithScores(final String key, final double min, final double max,
      final int offset, final int count) {
    Consumer<Client> consumer = client -> client.zrangeByScoreWithScores(key, min, max, offset, count);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrangeByScoreWithScores(final String key, final String min, final String max,
      final int offset, final int count) {
    Consumer<Client> consumer = client -> client.zrangeByScoreWithScores(key, min, max, offset, count);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key, final double min, final double max,
      final int offset, final int count) {
    Consumer<Client> consumer = client -> client.zrangeByScoreWithScores(key, min, max, offset, count);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max,
      final int offset, final int count) {
    Consumer<Client> consumer = client -> client.zrangeByScoreWithScores(key, min, max, offset, count);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<String>> zrevrangeByScore(final String key, final double max, final double min) {
    Consumer<Client> consumer = client -> client.zrevrangeByScore(key, max, min);
    return addBatch(consumer, BuilderFactory.STRING_ZSET);
  }

  @Override
  public Response<Set<byte[]>> zrevrangeByScore(final byte[] key, final double max, final double min) {
    Consumer<Client> consumer = client -> client.zrevrangeByScore(key, max, min);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Set<String>> zrevrangeByScore(final String key, final String max, final String min) {
    Consumer<Client> consumer = client -> client.zrevrangeByScore(key, max, min);
    return addBatch(consumer, BuilderFactory.STRING_ZSET);
  }

  @Override
  public Response<Set<byte[]>> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min) {
    Consumer<Client> consumer = client -> client.zrevrangeByScore(key, max, min);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Set<String>> zrevrangeByScore(final String key, final double max, final double min, final int offset,
      final int count) {
    Consumer<Client> consumer = client -> client.zrevrangeByScore(key, max, min, offset, count);
    return addBatch(consumer, BuilderFactory.STRING_ZSET);
  }

  @Override
  public Response<Set<String>> zrevrangeByScore(final String key, final String max, final String min, final int offset,
      final int count) {
    Consumer<Client> consumer = client -> client.zrevrangeByScore(key, max, min, offset, count);
    return addBatch(consumer, BuilderFactory.STRING_ZSET);
  }

  @Override
  public Response<Set<byte[]>> zrevrangeByScore(final byte[] key, final double max, final double min, final int offset,
      final int count) {
    Consumer<Client> consumer = client -> client.zrevrangeByScore(key, max, min, offset, count);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Set<byte[]>> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min, final int offset,
      final int count) {
    Consumer<Client> consumer = client -> client.zrevrangeByScore(key, max, min, offset, count);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
    Consumer<Client> consumer = client -> client.zrevrangeByScoreWithScores(key, max, min);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key, final String max, final String min) {
    Consumer<Client> consumer = client -> client.zrevrangeByScoreWithScores(key, max, min);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min) {
    Consumer<Client> consumer = client -> client.zrevrangeByScoreWithScores(key, max, min);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min) {
    Consumer<Client> consumer = client -> client.zrevrangeByScoreWithScores(key, max, min);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key, final double max, final double min,
      final int offset, final int count) {
    Consumer<Client> consumer = client -> client.zrevrangeByScoreWithScores(key, max, min, offset, count);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key, final String max, final String min,
      final int offset, final int count) {
    Consumer<Client> consumer = client -> client.zrevrangeByScoreWithScores(key, max, min, offset, count);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min,
      final int offset, final int count) {
    Consumer<Client> consumer = client -> client.zrevrangeByScoreWithScores(key, max, min, offset, count);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min,
      final int offset, final int count) {
    Consumer<Client> consumer = client -> client.zrevrangeByScoreWithScores(key, max, min, offset, count);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrangeWithScores(final String key, final long start, final long stop) {
    Consumer<Client> consumer = client -> client.zrangeWithScores(key, start, stop);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrangeWithScores(final byte[] key, final long start, final long stop) {
    Consumer<Client> consumer = client -> client.zrangeWithScores(key, start, stop);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Long> zrank(final String key, final String member) {
    Consumer<Client> consumer = client -> client.zrank(key, member);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zrank(final byte[] key, final byte[] member) {
    Consumer<Client> consumer = client -> client.zrank(key, member);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zrem(final String key, final String... members) {
    Consumer<Client> consumer = client -> client.zrem(key, members);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zrem(final byte[] key, final byte[]... members) {
    Consumer<Client> consumer = client -> client.zrem(key, members);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zremrangeByRank(final String key, final long start, final long stop) {
    Consumer<Client> consumer = client -> client.zremrangeByRank(key, start, stop);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zremrangeByRank(final byte[] key, final long start, final long stop) {
    Consumer<Client> consumer = client -> client.zremrangeByRank(key, start, stop);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zremrangeByScore(final String key, final double min, final double max) {
    Consumer<Client> consumer = client -> client.zremrangeByScore(key, min, max);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zremrangeByScore(final String key, final String min, final String max) {
    Consumer<Client> consumer = client -> client.zremrangeByScore(key, min, max);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zremrangeByScore(final byte[] key, final double min, final double max) {
    Consumer<Client> consumer = client -> client.zremrangeByScore(key, min, max);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zremrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
    Consumer<Client> consumer = client -> client.zremrangeByScore(key, min, max);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Set<String>> zrevrange(final String key, final long start, final long stop) {
    Consumer<Client> consumer = client -> client.zrevrange(key, start, stop);
    return addBatch(consumer, BuilderFactory.STRING_ZSET);
  }

  @Override
  public Response<Set<byte[]>> zrevrange(final byte[] key, final long start, final long stop) {
    Consumer<Client> consumer = client -> client.zrevrange(key, start, stop);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrevrangeWithScores(final String key, final long start, final long stop) {
    Consumer<Client> consumer = client -> client.zrevrangeWithScores(key, start, stop);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zrevrangeWithScores(final byte[] key, final long start, final long stop) {
    Consumer<Client> consumer = client -> client.zrevrangeWithScores(key, start, stop);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Long> zrevrank(final String key, final String member) {
    Consumer<Client> consumer = client -> client.zrevrank(key, member);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zrevrank(final byte[] key, final byte[] member) {
    Consumer<Client> consumer = client -> client.zrevrank(key, member);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Double> zscore(final String key, final String member) {
    Consumer<Client> consumer = client -> client.zscore(key, member);
    return addBatch(consumer, BuilderFactory.DOUBLE);
  }

  @Override
  public Response<List<Double>> zmscore(final String key, final String... members) {
    Consumer<Client> consumer = client -> client.zmscore(key, members);
    return addBatch(consumer, BuilderFactory.DOUBLE_LIST);
  }

  @Override
  public Response<Double> zscore(final byte[] key, final byte[] member) {
    Consumer<Client> consumer = client -> client.zscore(key, member);
    return addBatch(consumer, BuilderFactory.DOUBLE);
  }

  @Override
  public Response<List<Double>> zmscore(final byte[] key, final byte[]... members) {
    Consumer<Client> consumer = client -> client.zmscore(key, members);
    return addBatch(consumer, BuilderFactory.DOUBLE_LIST);
  }

  @Override
  public Response<Tuple> zpopmax(final String key) {
    Consumer<Client> consumer = client -> client.zpopmax(key);
    return addBatch(consumer, BuilderFactory.TUPLE);
  }

  @Override
  public Response<Tuple> zpopmax(final byte[] key) {
    Consumer<Client> consumer = client -> client.zpopmax(key);
    return addBatch(consumer, BuilderFactory.TUPLE);
  }

  @Override
  public Response<Set<Tuple>> zpopmax(final String key, final int count) {
    Consumer<Client> consumer = client -> client.zpopmax(key, count);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zpopmax(final byte[] key, final int count) {
    Consumer<Client> consumer = client -> client.zpopmax(key, count);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Tuple> zpopmin(final String key) {
    Consumer<Client> consumer = client -> client.zpopmin(key);
    return addBatch(consumer, BuilderFactory.TUPLE);
  }

  @Override
  public Response<Tuple> zpopmin(final byte[] key) {
    Consumer<Client> consumer = client -> client.zpopmin(key);
    return addBatch(consumer, BuilderFactory.TUPLE);
  }

  @Override
  public Response<Set<Tuple>> zpopmin(final byte[] key, final int count) {
    Consumer<Client> consumer = client -> client.zpopmin(key, count);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Set<Tuple>> zpopmin(final String key, final int count) {
    Consumer<Client> consumer = client -> client.zpopmin(key, count);
    return addBatch(consumer, BuilderFactory.TUPLE_ZSET);
  }

  @Override
  public Response<Long> zlexcount(final byte[] key, final byte[] min, final byte[] max) {
    Consumer<Client> consumer = client -> client.zlexcount(key, min, max);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zlexcount(final String key, final String min, final String max) {
    Consumer<Client> consumer = client -> client.zlexcount(key, min, max);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Set<byte[]>> zrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
    Consumer<Client> consumer = client -> client.zrangeByLex(key, min, max);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Set<String>> zrangeByLex(final String key, final String min, final String max) {
    Consumer<Client> consumer = client -> client.zrangeByLex(key, min, max);
    return addBatch(consumer, BuilderFactory.STRING_ZSET);
  }

  @Override
  public Response<Set<byte[]>> zrangeByLex(final byte[] key, final byte[] min, final byte[] max,
      final int offset, final int count) {
    Consumer<Client> consumer = client -> client.zrangeByLex(key, min, max, offset, count);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Set<String>> zrangeByLex(final String key, final String min, final String max,
      final int offset, final int count) {
    Consumer<Client> consumer = client -> client.zrangeByLex(key, min, max, offset, count);
    return addBatch(consumer, BuilderFactory.STRING_ZSET);
  }

  @Override
  public Response<Set<byte[]>> zrevrangeByLex(final byte[] key, final byte[] max, final byte[] min) {
    Consumer<Client> consumer = client -> client.zrevrangeByLex(key, max, min);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Set<String>> zrevrangeByLex(final String key, final String max, final String min) {
    Consumer<Client> consumer = client -> client.zrevrangeByLex(key, max, min);
    return addBatch(consumer, BuilderFactory.STRING_ZSET);
  }

  @Override
  public Response<Set<byte[]>> zrevrangeByLex(final byte[] key, final byte[] max, final byte[] min,
      final int offset, final int count) {
    Consumer<Client> consumer = client -> client.zrevrangeByLex(key, max, min, offset, count);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Set<String>> zrevrangeByLex(final String key, final String max, final String min,
      final int offset, final int count) {
    Consumer<Client> consumer = client -> client.zrevrangeByLex(key, max, min, offset, count);
    return addBatch(consumer, BuilderFactory.STRING_ZSET);
  }

  @Override
  public Response<Long> zremrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
    Consumer<Client> consumer = client -> client.zremrangeByLex(key, min, max);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zremrangeByLex(final String key, final String min, final String max) {
    Consumer<Client> consumer = client -> client.zremrangeByLex(key, min, max);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> bitcount(final String key) {
    Consumer<Client> consumer = client -> client.bitcount(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> bitcount(final String key, final long start, final long end) {
    Consumer<Client> consumer = client -> client.bitcount(key, start, end);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> bitcount(final byte[] key) {
    Consumer<Client> consumer = client -> client.bitcount(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> bitcount(final byte[] key, final long start, final long end) {
    Consumer<Client> consumer = client -> client.bitcount(key, start, end);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<byte[]> dump(final String key) {
    Consumer<Client> consumer = client -> client.dump(key);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<byte[]> dump(final byte[] key) {
    Consumer<Client> consumer = client -> client.dump(key);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<String> migrate(final String host, final int port,
      final String key, final int destinationDb, final int timeout) {
    Consumer<Client> consumer = client -> client.migrate(host, port, key, destinationDb, timeout);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> migrate(final String host, final int port,
      final byte[] key, final int destinationDb, final int timeout) {
    Consumer<Client> consumer = client -> client.migrate(host, port, key, destinationDb, timeout);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Long> objectRefcount(final String key) {
    Consumer<Client> consumer = client -> client.objectRefcount(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> objectRefcount(final byte[] key) {
    Consumer<Client> consumer = client -> client.objectRefcount(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> objectEncoding(final String key) {
    Consumer<Client> consumer = client -> client.objectEncoding(key);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<byte[]> objectEncoding(final byte[] key) {
    Consumer<Client> consumer = client -> client.objectEncoding(key);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<Long> objectIdletime(final String key) {
    Consumer<Client> consumer = client -> client.objectIdletime(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> objectIdletime(final byte[] key) {
    Consumer<Client> consumer = client -> client.objectIdletime(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> objectFreq(byte[] key) {
    Consumer<Client> consumer = client -> client.objectFreq(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> objectFreq(String key) {
    Consumer<Client> consumer = client -> client.objectFreq(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> pexpire(final String key, final long milliseconds) {
    Consumer<Client> consumer = client -> client.pexpire(key, milliseconds);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> pexpire(final byte[] key, final long milliseconds) {
    Consumer<Client> consumer = client -> client.pexpire(key, milliseconds);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> pexpireAt(final String key, final long millisecondsTimestamp) {
    Consumer<Client> consumer = client -> client.pexpireAt(key, millisecondsTimestamp);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> pexpireAt(final byte[] key, final long millisecondsTimestamp) {
    Consumer<Client> consumer = client -> client.pexpireAt(key, millisecondsTimestamp);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> pttl(final String key) {
    Consumer<Client> consumer = client -> client.pttl(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> pttl(final byte[] key) {
    Consumer<Client> consumer = client -> client.pttl(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> restore(final String key, final int ttl, final byte[] serializedValue) {
    Consumer<Client> consumer = client -> client.restore(key, ttl, serializedValue);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> restore(final byte[] key, final int ttl, final byte[] serializedValue) {
    Consumer<Client> consumer = client -> client.restore(key, ttl, serializedValue);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> restoreReplace(final String key, final int ttl, final byte[] serializedValue) {
    Consumer<Client> consumer = client -> client.restoreReplace(key, ttl, serializedValue);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> restoreReplace(final byte[] key, final int ttl, final byte[] serializedValue) {
    Consumer<Client> consumer = client -> client.restoreReplace(key, ttl, serializedValue);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Double> incrByFloat(final String key, final double increment) {
    Consumer<Client> consumer = client -> client.incrByFloat(key, increment);
    return addBatch(consumer, BuilderFactory.DOUBLE);
  }

  @Override
  public Response<Double> incrByFloat(final byte[] key, final double increment) {
    Consumer<Client> consumer = client -> client.incrByFloat(key, increment);
    return addBatch(consumer, BuilderFactory.DOUBLE);
  }

  @Override
  public Response<String> psetex(final String key, final long milliseconds, final String value) {
    Consumer<Client> consumer = client -> client.psetex(key, milliseconds, value);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> psetex(final byte[] key, final long milliseconds, final byte[] value) {
    Consumer<Client> consumer = client -> client.psetex(key, milliseconds, value);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Double> hincrByFloat(final String key, final String field, final double increment) {
    Consumer<Client> consumer = client -> client.hincrByFloat(key, field, increment);
    return addBatch(consumer, BuilderFactory.DOUBLE);
  }

  @Override
  public Response<Double> hincrByFloat(final byte[] key, final byte[] field, final double increment) {
    Consumer<Client> consumer = client -> client.hincrByFloat(key, field, increment);
    return addBatch(consumer, BuilderFactory.DOUBLE);
  }

  @Override
  public Response<Long> pfadd(final byte[] key, final byte[]... elements) {
    Consumer<Client> consumer = client -> client.pfadd(key, elements);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> pfcount(final byte[] key) {
    Consumer<Client> consumer = client -> client.pfcount(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> pfadd(final String key, final String... elements) {
    Consumer<Client> consumer = client -> client.pfadd(key, elements);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> pfcount(final String key) {
    Consumer<Client> consumer = client -> client.pfcount(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> geoadd(final byte[] key, final double longitude, final double latitude, final byte[] member) {
    Consumer<Client> consumer = client -> client.geoadd(key, longitude, latitude, member);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> geoadd(final byte[] key, final Map<byte[], GeoCoordinate> memberCoordinateMap) {
    Consumer<Client> consumer = client -> client.geoadd(key, memberCoordinateMap);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> geoadd(final String key, final double longitude, final double latitude, final String member) {
    Consumer<Client> consumer = client -> client.geoadd(key, longitude, latitude, member);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> geoadd(final String key, final Map<String, GeoCoordinate> memberCoordinateMap) {
    Consumer<Client> consumer = client -> client.geoadd(key, memberCoordinateMap);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Double> geodist(final byte[] key, final byte[] member1, final byte[] member2) {
    Consumer<Client> consumer = client -> client.geodist(key, member1, member2);
    return addBatch(consumer, BuilderFactory.DOUBLE);
  }

  @Override
  public Response<Double> geodist(final byte[] key, final byte[] member1, final byte[] member2, final GeoUnit unit) {
    Consumer<Client> consumer = client -> client.geodist(key, member1, member2, unit);
    return addBatch(consumer, BuilderFactory.DOUBLE);
  }

  @Override
  public Response<Double> geodist(final String key, final String member1, final String member2) {
    Consumer<Client> consumer = client -> client.geodist(key, member1, member2);
    return addBatch(consumer, BuilderFactory.DOUBLE);
  }

  @Override
  public Response<Double> geodist(final String key, final String member1, final String member2, final GeoUnit unit) {
    Consumer<Client> consumer = client -> client.geodist(key, member1, member2);
    return addBatch(consumer, BuilderFactory.DOUBLE);
  }

  @Override
  public Response<List<byte[]>> geohash(final byte[] key, final byte[]... members) {
    Consumer<Client> consumer = client -> client.geohash(key, members);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);
  }

  @Override
  public Response<List<String>> geohash(final String key, final String... members) {
    Consumer<Client> consumer = client -> client.geohash(key, members);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<List<GeoCoordinate>> geopos(final byte[] key, final byte[]... members) {
    Consumer<Client> consumer = client -> client.geopos(key, members);
    return addBatch(consumer, BuilderFactory.GEO_COORDINATE_LIST);
  }

  @Override
  public Response<List<GeoCoordinate>> geopos(final String key, final String... members) {
    Consumer<Client> consumer = client -> client.geopos(key, members);
    return addBatch(consumer, BuilderFactory.GEO_COORDINATE_LIST);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadius(final byte[] key, final double longitude, final double latitude,
      final double radius, final GeoUnit unit) {
    Consumer<Client> consumer = client -> client.georadius(key, longitude, latitude, radius, unit);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadiusReadonly(final byte[] key, final double longitude, final double latitude,
      final double radius, final GeoUnit unit) {
    Consumer<Client> consumer = client -> client.georadiusReadonly(key, longitude, latitude, radius, unit);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadius(final byte[] key, final double longitude, final double latitude,
      final double radius, final GeoUnit unit, final GeoRadiusParam param) {
    Consumer<Client> consumer = client -> client.georadius(key, longitude, latitude, radius, unit, param);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadiusReadonly(final byte[] key, final double longitude, final double latitude,
      final double radius, final GeoUnit unit, final GeoRadiusParam param) {
    Consumer<Client> consumer = client -> client.georadiusReadonly(key, longitude, latitude, radius, unit, param);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadius(final String key, final double longitude, final double latitude,
      final double radius, final GeoUnit unit) {
    Consumer<Client> consumer = client -> client.georadius(key, longitude, latitude, radius, unit);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadiusReadonly(final String key, final double longitude, final double latitude,
      final double radius, final GeoUnit unit) {
    Consumer<Client> consumer = client -> client.georadiusReadonly(key, longitude, latitude, radius, unit);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadius(final String key, final double longitude, final double latitude,
      final double radius, final GeoUnit unit, final GeoRadiusParam param) {
    Consumer<Client> consumer = client -> client.georadius(key, longitude, latitude, radius, unit, param);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadiusReadonly(final String key, final double longitude, final double latitude,
      final double radius, final GeoUnit unit, final GeoRadiusParam param) {
    Consumer<Client> consumer = client -> client.georadiusReadonly(key, longitude, latitude, radius, unit, param);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadiusByMember(final byte[] key, final byte[] member,
      final double radius, final GeoUnit unit) {
    Consumer<Client> consumer = client -> client.georadiusByMember(key, member, radius, unit);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadiusByMemberReadonly(final byte[] key, final byte[] member,
      final double radius, final GeoUnit unit) {
    Consumer<Client> consumer = client -> client.georadiusByMemberReadonly(key, member, radius, unit);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadiusByMember(final byte[] key, final byte[] member,
      final double radius, final GeoUnit unit, final GeoRadiusParam param) {
    Consumer<Client> consumer = client -> client.georadiusByMember(key, member, radius, unit, param);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadiusByMemberReadonly(final byte[] key, final byte[] member,
      final double radius, final GeoUnit unit, final GeoRadiusParam param) {
    Consumer<Client> consumer = client -> client.georadiusByMemberReadonly(key, member, radius, unit, param);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadiusByMember(final String key, final String member,
      final double radius, final GeoUnit unit) {
    Consumer<Client> consumer = client -> client.georadiusByMember(key, member, radius, unit);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadiusByMemberReadonly(final String key, final String member,
      final double radius, final GeoUnit unit) {
    Consumer<Client> consumer = client -> client.georadiusByMemberReadonly(key, member, radius, unit);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadiusByMember(final String key, final String member,
      final double radius, final GeoUnit unit, final GeoRadiusParam param) {
    Consumer<Client> consumer = client -> client.georadiusByMember(key, member, radius, unit, param);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<GeoRadiusResponse>> georadiusByMemberReadonly(final String key, final String member,
      final double radius, final GeoUnit unit, final GeoRadiusParam param) {
    Consumer<Client> consumer = client -> client.georadiusByMemberReadonly(key, member, radius, unit, param);
    return addBatch(consumer, BuilderFactory.GEORADIUS_WITH_PARAMS_RESULT);
  }

  @Override
  public Response<List<Long>> bitfield(final String key, final String... elements) {
    Consumer<Client> consumer = client -> client.bitfield(key, elements);
    return addBatch(consumer, BuilderFactory.LONG_LIST);
  }

  @Override
  public Response<List<Long>> bitfield(final byte[] key, final byte[]... elements) {
    Consumer<Client> consumer = client -> client.bitfield(key, elements);
    return addBatch(consumer, BuilderFactory.LONG_LIST);
  }

  @Override
  public Response<List<Long>> bitfieldReadonly(byte[] key, final byte[]... arguments) {
    Consumer<Client> consumer = client -> client.bitfieldReadonly(key, arguments);
    return addBatch(consumer, BuilderFactory.LONG_LIST);
  }

  @Override
  public Response<List<Long>> bitfieldReadonly(String key, final String... arguments) {
    Consumer<Client> consumer = client -> client.bitfieldReadonly(key, arguments);
    return addBatch(consumer, BuilderFactory.LONG_LIST);
  }

  @Override
  public Response<Long> hstrlen(final String key, final String field) {
    Consumer<Client> consumer = client -> client.hstrlen(key, field);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> hstrlen(final byte[] key, final byte[] field) {
    Consumer<Client> consumer = client -> client.hstrlen(key, field);
    return addBatch(consumer, BuilderFactory.LONG);
  }
  
  @Override
  public Response<StreamEntryID> xadd(String key, StreamEntryID id, Map<String, String> hash){
    return xadd(key, id, hash, Long.MAX_VALUE, true);    
  }
  
  @Override
  public Response<byte[]> xadd(byte[] key, byte[] id, Map<byte[], byte[]> hash){
    return xadd(key, id, hash, Long.MAX_VALUE, true);
  }


  @Override
  public Response<StreamEntryID> xadd(String key, StreamEntryID id, Map<String, String> hash, long maxLen, boolean approximateLength){
    Consumer<Client> consumer = client -> client.xadd(key, id, hash, maxLen, approximateLength);
    return addBatch(consumer, BuilderFactory.STREAM_ENTRY_ID);    
  }
  

  @Override
  public Response<byte[]> xadd(byte[] key, byte[] id, Map<byte[], byte[]> hash, long maxLen, boolean approximateLength){
    Consumer<Client> consumer = client -> client.xadd(key, id, hash, maxLen, approximateLength);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);        
  }

  
  @Override
  public Response<Long> xlen(String key){
    Consumer<Client> consumer = client -> client.xlen(key);
    return addBatch(consumer, BuilderFactory.LONG);
  }
  
  @Override
  public Response<Long> xlen(byte[] key){
    Consumer<Client> consumer = client -> client.xlen(key);
    return addBatch(consumer, BuilderFactory.LONG);    
  }

  @Override
  public Response<List<StreamEntry>> xrange(String key, StreamEntryID start, StreamEntryID end, int count){
    Consumer<Client> consumer = client -> client.xrange(key, start, end, count);
    return addBatch(consumer, BuilderFactory.STREAM_ENTRY_LIST);        
  }

  @Override
  public Response<List<byte[]>> xrange(byte[] key, byte[] start, byte[] end, int count){
    Consumer<Client> consumer = client -> client.xrange(key, start, end, count);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);            
  }

  @Override
  public Response<List<StreamEntry>> xrevrange(String key, StreamEntryID end, StreamEntryID start, int count){
    Consumer<Client> consumer = client -> client.xrevrange(key, start, end, count);
    return addBatch(consumer, BuilderFactory.STREAM_ENTRY_LIST);            
  }

  @Override
  public Response<List<byte[]>> xrevrange(byte[] key, byte[] end, byte[] start, int count){
    Consumer<Client> consumer = client -> client.xrevrange(key, start, end, count);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);                
  }

   
  @Override
  public Response<Long> xack(String key, String group,  StreamEntryID... ids){
    Consumer<Client> consumer = client -> client.xack(key, group, ids);
    return addBatch(consumer, BuilderFactory.LONG);                
  }
  
  @Override
  public Response<Long> xack(byte[] key, byte[] group,  byte[]... ids){
    Consumer<Client> consumer = client -> client.xack(key, group, ids);
    return addBatch(consumer, BuilderFactory.LONG);                    
  }
  
  @Override
  public Response<String> xgroupCreate( String key, String groupname, StreamEntryID id, boolean makeStream){
    Consumer<Client> consumer = client -> client.xgroupCreate(key, groupname, id, makeStream);
    return addBatch(consumer, BuilderFactory.STRING);
  }
  
  @Override
  public Response<String> xgroupCreate(byte[] key, byte[] groupname, byte[] id, boolean makeStream){
    Consumer<Client> consumer = client -> client.xgroupCreate(key, groupname, id, makeStream);
    return addBatch(consumer, BuilderFactory.STRING);    
  }
  
  @Override
  public Response<String> xgroupSetID( String key, String groupname, StreamEntryID id){
    Consumer<Client> consumer = client -> client.xgroupSetID(key, groupname, id);
    return addBatch(consumer, BuilderFactory.STRING);
  }
  
  @Override
  public Response<String> xgroupSetID(byte[] key, byte[] groupname, byte[] id){
    Consumer<Client> consumer = client -> client.xgroupSetID(key, groupname, id);
    return addBatch(consumer, BuilderFactory.STRING);    
  }
  
  @Override
  public Response<Long> xgroupDestroy( String key, String groupname){
    Consumer<Client> consumer = client -> client.xgroupDestroy(key, groupname);
    return addBatch(consumer, BuilderFactory.LONG);
  }
  
  @Override
  public Response<Long> xgroupDestroy(byte[] key, byte[] groupname){
    Consumer<Client> consumer = client -> client.xgroupDestroy(key, groupname);
    return addBatch(consumer, BuilderFactory.LONG);
  }
  
  @Override
  public Response<Long> xgroupDelConsumer( String key, String groupname, String consumername){
    Consumer<Client> consumer = client -> client.xgroupDelConsumer(key, groupname, consumername);
    return addBatch(consumer, BuilderFactory.LONG);
  }
  
  @Override
  public Response<Long> xgroupDelConsumer(byte[] key, byte[] groupname, byte[] consumername){
    Consumer<Client> consumer = client -> client.xgroupDelConsumer(key, groupname, consumername);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<List<StreamPendingEntry>> xpending(String key, String groupname, StreamEntryID start, StreamEntryID end, int count, String consumername){
    Consumer<Client> consumer = client -> client.xpending(key, groupname, start, end, count, consumername);
    return addBatch(consumer, BuilderFactory.STREAM_PENDING_ENTRY_LIST);        
  }
  
  @Override
  public Response<List<StreamPendingEntry>> xpending(byte[] key, byte[] groupname, byte[] start, byte[] end, int count, byte[] consumername){
    Consumer<Client> consumer = client -> client.xpending(key, groupname, start, end, count, consumername);
    return addBatch(consumer, BuilderFactory.STREAM_PENDING_ENTRY_LIST);            
  }

  
  @Override
  public Response<Long> xdel( String key, StreamEntryID... ids){
    Consumer<Client> consumer = client -> client.xdel(key, ids);
    return addBatch(consumer, BuilderFactory.LONG);        
  }

  @Override
  public Response<Long> xdel(byte[] key, byte[]... ids){
    Consumer<Client> consumer = client -> client.xdel(key, ids);
    return addBatch(consumer, BuilderFactory.LONG);            
  }
  
  @Override
  public Response<Long> xtrim( String key, long maxLen, boolean approximateLength){
    Consumer<Client> consumer = client -> client.xtrim(key, maxLen, approximateLength);
    return addBatch(consumer, BuilderFactory.LONG);        
  }
  
  @Override
  public Response<Long> xtrim(byte[] key, long maxLen, boolean approximateLength){
    Consumer<Client> consumer = client -> client.xtrim(key, maxLen, approximateLength);
    return addBatch(consumer, BuilderFactory.LONG);            
  }
 
  @Override
  public Response<List<StreamEntry>> xclaim( String key, String group, String consumername, long minIdleTime, 
      long newIdleTime, int retries, boolean force, StreamEntryID... ids){
    Consumer<Client> consumer = client -> client.xclaim(key, group, consumername, minIdleTime, newIdleTime, retries, force, ids);
    return addBatch(consumer, BuilderFactory.STREAM_ENTRY_LIST);        
  }
 
  @Override
  public Response<List<byte[]>> xclaim(byte[] key, byte[] group, byte[] consumername, long minIdleTime, 
      long newIdleTime, int retries, boolean force, byte[]... ids){
    Consumer<Client> consumer = client -> client.xclaim(key, group, consumername, minIdleTime, newIdleTime, retries, force, ids);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);            
  }

  public Response<Object> sendCommand(final String sampleKey, final ProtocolCommand cmd, final String... args) {
    Consumer<Client> consumer = client -> client.sendCommand(cmd, args);
    return addBatch(consumer, BuilderFactory.OBJECT);
  }

  public Response<Object> sendCommand(final byte[] sampleKey, final ProtocolCommand cmd, final byte[]... args) {
    Consumer<Client> consumer = client -> client.sendCommand(cmd, args);
    return addBatch(consumer, BuilderFactory.OBJECT);
  }
}
