package redis.clients.jedis;

import redis.clients.jedis.commands.*;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.GeoRadiusStoreParam;
import redis.clients.jedis.params.MigrateParams;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public abstract class MultiKeyPipelineBase<J extends JedisBase> extends PipelineBase<J> implements
    MultiKeyBinaryRedisPipeline, MultiKeyCommandsPipeline, ClusterPipeline,
    BinaryScriptingCommandsPipeline, ScriptingCommandsPipeline, BasicRedisPipeline {

  public MultiKeyPipelineBase(J resource) {
    super(resource);
  }

  @Override
  public Response<List<String>> brpop(String... args) {
    Consumer<Client> consumer = client -> client.brpop(args);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  public Response<List<String>> brpop(int timeout, String... keys) {
    Consumer<Client> consumer = client -> client.brpop(timeout, keys);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<List<String>> blpop(String... args) {
    Consumer<Client> consumer = client -> client.blpop(args);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  public Response<List<String>> blpop(int timeout, String... keys) {
    Consumer<Client> consumer = client -> client.blpop(timeout, keys);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  public Response<Map<String, String>> blpopMap(int timeout, String... keys) {
    Consumer<Client> consumer = client -> client.blpop(timeout, keys);
    return addBatch(consumer, BuilderFactory.STRING_MAP);
  }

  @Override
  public Response<List<byte[]>> brpop(byte[]... args) {
    Consumer<Client> consumer = client -> client.brpop(args);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);
  }

  public Response<List<String>> brpop(int timeout, byte[]... keys) {
    Consumer<Client> consumer = client -> client.brpop(timeout, keys);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  public Response<Map<String, String>> brpopMap(int timeout, String... keys) {
    Consumer<Client> consumer = client -> client.blpop(timeout, keys);
    return addBatch(consumer, BuilderFactory.STRING_MAP);
  }

  @Override
  public Response<List<byte[]>> blpop(byte[]... args) {
    Consumer<Client> consumer = client -> client.blpop(args);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);
  }

  public Response<List<String>> blpop(int timeout, byte[]... keys) {
    Consumer<Client> consumer = client -> client.blpop(timeout, keys);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<Long> del(String... keys) {
    Consumer<Client> consumer = client -> client.del(keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> del(byte[]... keys) {
    Consumer<Client> consumer = client -> client.del(keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> unlink(String... keys) {
    Consumer<Client> consumer = client -> client.unlink(keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> unlink(byte[]... keys) {
    Consumer<Client> consumer = client -> client.unlink(keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> exists(String... keys) {
    Consumer<Client> consumer = client -> client.exists(keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> exists(byte[]... keys) {
    Consumer<Client> consumer = client -> client.exists(keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Set<String>> keys(String pattern) {
    Consumer<Client> consumer = client -> client.keys(pattern);
    return addBatch(consumer, BuilderFactory.STRING_SET);
  }

  @Override
  public Response<Set<byte[]>> keys(byte[] pattern) {
    Consumer<Client> consumer = client -> client.keys(pattern);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<List<String>> mget(String... keys) {
    Consumer<Client> consumer = client -> client.mget(keys);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<List<byte[]>> mget(byte[]... keys) {
    Consumer<Client> consumer = client -> client.mget(keys);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_LIST);
  }

  @Override
  public Response<String> mset(String... keysvalues) {
    Consumer<Client> consumer = client -> client.mset(keysvalues);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> mset(byte[]... keysvalues) {
    Consumer<Client> consumer = client -> client.mset(keysvalues);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Long> msetnx(String... keysvalues) {
    Consumer<Client> consumer = client -> client.msetnx(keysvalues);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> msetnx(byte[]... keysvalues) {
    Consumer<Client> consumer = client -> client.msetnx(keysvalues);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> rename(String oldkey, String newkey) {
    Consumer<Client> consumer = client -> client.rename(oldkey, newkey);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> rename(byte[] oldkey, byte[] newkey) {
    Consumer<Client> consumer = client -> client.rename(oldkey, newkey);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Long> renamenx(String oldkey, String newkey) {
    Consumer<Client> consumer = client -> client.renamenx(oldkey, newkey);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> renamenx(byte[] oldkey, byte[] newkey) {
    Consumer<Client> consumer = client -> client.renamenx(oldkey, newkey);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> rpoplpush(String srckey, String dstkey) {
    Consumer<Client> consumer = client -> client.rpoplpush(srckey, dstkey);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<byte[]> rpoplpush(byte[] srckey, byte[] dstkey) {
    Consumer<Client> consumer = client -> client.rpoplpush(srckey, dstkey);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<Set<String>> sdiff(String... keys) {
    Consumer<Client> consumer = client -> client.sdiff(keys);
    return addBatch(consumer, BuilderFactory.STRING_SET);
  }

  @Override
  public Response<Set<byte[]>> sdiff(byte[]... keys) {
    Consumer<Client> consumer = client -> client.sdiff(keys);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Long> sdiffstore(String dstkey, String... keys) {
    Consumer<Client> consumer = client -> client.sdiffstore(dstkey, keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sdiffstore(byte[] dstkey, byte[]... keys) {
    Consumer<Client> consumer = client -> client.sdiffstore(dstkey, keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Set<String>> sinter(String... keys) {
    Consumer<Client> consumer = client -> client.sinter(keys);
    return addBatch(consumer, BuilderFactory.STRING_SET);
  }

  @Override
  public Response<Set<byte[]>> sinter(byte[]... keys) {
    Consumer<Client> consumer = client -> client.sinter(keys);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Long> sinterstore(String dstkey, String... keys) {
    Consumer<Client> consumer = client -> client.sinterstore(dstkey, keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sinterstore(byte[] dstkey, byte[]... keys) {
    Consumer<Client> consumer = client -> client.sinterstore(dstkey, keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> smove(String srckey, String dstkey, String member) {
    Consumer<Client> consumer = client -> client.smove(srckey, dstkey, member);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> smove(byte[] srckey, byte[] dstkey, byte[] member) {
    Consumer<Client> consumer = client -> client.smove(srckey, dstkey, member);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sort(String key, SortingParams sortingParameters, String dstkey) {
    Consumer<Client> consumer = client -> client.sort(key, sortingParameters, dstkey);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
    Consumer<Client> consumer = client -> client.sort(key, sortingParameters, dstkey);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sort(String key, String dstkey) {
    Consumer<Client> consumer = client -> client.sort(key, dstkey);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sort(byte[] key, byte[] dstkey) {
    Consumer<Client> consumer = client -> client.sort(key, dstkey);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Set<String>> sunion(String... keys) {
    Consumer<Client> consumer = client -> client.sunion(keys);
    return addBatch(consumer, BuilderFactory.STRING_SET);
  }

  @Override
  public Response<Set<byte[]>> sunion(byte[]... keys) {
    Consumer<Client> consumer = client -> client.sunion(keys);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Long> sunionstore(String dstkey, String... keys) {
    Consumer<Client> consumer = client -> client.sunionstore(dstkey, keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sunionstore(byte[] dstkey, byte[]... keys) {
    Consumer<Client> consumer = client -> client.sunionstore(dstkey, keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> watch(String... keys) {
    Consumer<Client> consumer = client -> client.watch(keys);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> watch(byte[]... keys) {
    Consumer<Client> consumer = client -> client.watch(keys);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> unwatch() {
    Consumer<Client> consumer = client -> client.unwatch();
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Long> zinterstore(String dstkey, String... sets) {
    Consumer<Client> consumer = client -> client.zinterstore(dstkey, sets);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zinterstore(byte[] dstkey, byte[]... sets) {
    Consumer<Client> consumer = client -> client.zinterstore(dstkey, sets);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zinterstore(String dstkey, ZParams params, String... sets) {
    Consumer<Client> consumer = client -> client.zinterstore(dstkey, params, sets);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
    Consumer<Client> consumer = client -> client.zinterstore(dstkey, params, sets);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zunionstore(String dstkey, String... sets) {
    Consumer<Client> consumer = client -> client.zunionstore(dstkey, sets);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zunionstore(byte[] dstkey, byte[]... sets) {
    Consumer<Client> consumer = client -> client.zunionstore(dstkey, sets);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zunionstore(String dstkey, ZParams params, String... sets) {
    Consumer<Client> consumer = client -> client.zunionstore(dstkey, params, sets);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
    Consumer<Client> consumer = client -> client.zunionstore(dstkey, params, sets);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> bgrewriteaof() {
    Consumer<Client> consumer = client -> client.bgrewriteaof();
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> bgsave() {
    Consumer<Client> consumer = client -> client.bgsave();
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<List<String>> configGet(String pattern) {
    Consumer<Client> consumer = client -> client.configGet(pattern);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<String> configSet(String parameter, String value) {
    Consumer<Client> consumer = client -> client.configSet(parameter, value);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> brpoplpush(String source, String destination, int timeout) {
    Consumer<Client> consumer = client -> client.brpoplpush(source, destination, timeout);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<byte[]> brpoplpush(byte[] source, byte[] destination, int timeout) {
    Consumer<Client> consumer = client -> client.brpoplpush(source, destination, timeout);
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<String> configResetStat() {
    Consumer<Client> consumer = client -> client.configResetStat();
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> save() {
    Consumer<Client> consumer = client -> client.save();
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Long> lastsave() {
    Consumer<Client> consumer = client -> client.lastsave();
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> publish(String channel, String message) {
    Consumer<Client> consumer = client -> client.publish(channel, message);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> publish(byte[] channel, byte[] message) {
    Consumer<Client> consumer = client -> client.publish(channel, message);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> randomKey() {
    Consumer<Client> consumer = client -> client.randomKey();
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<byte[]> randomKeyBinary() {
    Consumer<Client> consumer = client -> client.randomKey();
    return addBatch(consumer, BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<String> flushDB() {
    Consumer<Client> consumer = client -> client.flushDB();
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> flushAll() {
    Consumer<Client> consumer = client -> client.flushAll();
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> info() {
    Consumer<Client> consumer = client -> client.info();
    return addBatch(consumer, BuilderFactory.STRING);
  }

  public Response<String> info(final String section) {
    Consumer<Client> consumer = client -> client.info(section);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Long> dbSize() {
    Consumer<Client> consumer = client -> client.dbSize();
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> shutdown() {
    Consumer<Client> consumer = client -> client.shutdown();
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> ping() {
    Consumer<Client> consumer = client -> client.ping();
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> select(int index) {
    Consumer<Client> consumer = client -> client.select(index);
    Response<String> response = addBatch(consumer, BuilderFactory.STRING);
    getClient().setDb(index);
    return response;
  }

  @Override
  public Response<String> swapDB(int index1, int index2) {
    Consumer<Client> consumer = client -> client.swapDB(index1, index2);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Long> bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
    Consumer<Client> consumer = client -> client.bitop(op, destKey, srcKeys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> bitop(BitOP op, String destKey, String... srcKeys) {
    Consumer<Client> consumer = client -> client.bitop(op, destKey, srcKeys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> clusterNodes() {
    Consumer<Client> consumer = client -> client.clusterNodes();
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> clusterMeet(final String ip, final int port) {
    Consumer<Client> consumer = client -> client.clusterMeet(ip, port);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> clusterAddSlots(final int... slots) {
    Consumer<Client> consumer = client -> client.clusterAddSlots(slots);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> clusterDelSlots(final int... slots) {
    Consumer<Client> consumer = client -> client.clusterDelSlots(slots);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> clusterInfo() {
    Consumer<Client> consumer = client -> client.clusterInfo();
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<List<String>> clusterGetKeysInSlot(final int slot, final int count) {
    Consumer<Client> consumer = client -> client.clusterGetKeysInSlot(slot, count);
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<String> clusterSetSlotNode(final int slot, final String nodeId) {
    Consumer<Client> consumer = client -> client.clusterSetSlotNode(slot, nodeId);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> clusterSetSlotMigrating(final int slot, final String nodeId) {
    Consumer<Client> consumer = client -> client.clusterSetSlotMigrating(slot, nodeId);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> clusterSetSlotImporting(final int slot, final String nodeId) {
    Consumer<Client> consumer = client -> client.clusterSetSlotImporting(slot, nodeId);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<Object> eval(String script) {
    return this.eval(script, 0, new String[0]);
  }

  @Override
  public Response<Object> eval(String script, List<String> keys, List<String> args) {
    String[] argv = Jedis.getParams(keys, args);
    return this.eval(script, keys.size(), argv);
  }

  @Override
  public Response<Object> eval(String script, int keyCount, String... params) {
    Consumer<Client> consumer = client -> client.eval(script, keyCount, params);
    return addBatch(consumer, BuilderFactory.EVAL_RESULT);
  }

  @Override
  public Response<Object> evalsha(String sha1) {
    return this.evalsha(sha1, 0, new String[0]);
  }

  @Override
  public Response<Object> evalsha(String sha1, List<String> keys, List<String> args) {
    String[] argv = Jedis.getParams(keys, args);
    return this.evalsha(sha1, keys.size(), argv);
  }

  @Override
  public Response<Object> evalsha(String sha1, int keyCount, String... params) {
    Consumer<Client> consumer = client -> client.evalsha(sha1, keyCount, params);
    return addBatch(consumer, BuilderFactory.EVAL_RESULT);
  }

  @Override
  public Response<Object> eval(byte[] script) {
    return this.eval(script, 0);
  }

  @Override
  public Response<Object> eval(byte[] script, byte[] keyCount, byte[]... params) {
    Consumer<Client> consumer = client -> client.eval(script, keyCount, params);
    return addBatch(consumer, BuilderFactory.EVAL_BINARY_RESULT);
  }

  @Override
  public Response<Object> eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
    byte[][] argv = BinaryJedis.getParamsWithBinary(keys, args);
    return this.eval(script, keys.size(), argv);
  }

  @Override
  public Response<Object> eval(byte[] script, int keyCount, byte[]... params) {
    Consumer<Client> consumer = client -> client.eval(script, keyCount, params);
    return addBatch(consumer, BuilderFactory.EVAL_BINARY_RESULT);
  }

  @Override
  public Response<Object> evalsha(byte[] sha1) {
    return this.evalsha(sha1, 0);
  }

  @Override
  public Response<Object> evalsha(byte[] sha1, List<byte[]> keys, List<byte[]> args) {
    byte[][] argv = BinaryJedis.getParamsWithBinary(keys, args);
    return this.evalsha(sha1, keys.size(), argv);
  }

  @Override
  public Response<Object> evalsha(byte[] sha1, int keyCount, byte[]... params) {
    Consumer<Client> consumer = client -> client.evalsha(sha1, keyCount, params);
    return addBatch(consumer, BuilderFactory.EVAL_BINARY_RESULT);
  }

  @Override
  public Response<Long> pfcount(String... keys) {
    Consumer<Client> consumer = client -> client.pfcount(keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> pfcount(final byte[]... keys) {
    Consumer<Client> consumer = client -> client.pfcount(keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> pfmerge(byte[] destkey, byte[]... sourcekeys) {
    Consumer<Client> consumer = client -> client.pfmerge(destkey, sourcekeys);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> pfmerge(String destkey, String... sourcekeys) {
    Consumer<Client> consumer = client -> client.pfmerge(destkey, sourcekeys);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<List<String>> time() {
    Consumer<Client> consumer = client -> client.time();
    return addBatch(consumer, BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<Long> touch(String... keys) {
    Consumer<Client> consumer = client -> client.touch(keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> touch(byte[]... keys) {
    Consumer<Client> consumer = client -> client.touch(keys);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<String> moduleUnload(String name) {
    Consumer<Client> consumer = client -> client.moduleUnload(name);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<List<Module>> moduleList() {
    Consumer<Client> consumer = client -> client.moduleList();
    return addBatch(consumer, BuilderFactory.MODULE_LIST);
  }

  @Override
  public Response<String> moduleLoad(String path) {
    Consumer<Client> consumer = client -> client.moduleLoad(path);
    return addBatch(consumer, BuilderFactory.STRING);
  }  
  
  @Override
  public Response<String> migrate(final String host, final int port, final int destinationDB,
      final int timeout, final MigrateParams params, final String... keys) {
    Consumer<Client> consumer = client -> client.migrate(host, port, destinationDB, timeout, params, keys);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  @Override
  public Response<String> migrate(final String host, final int port, final int destinationDB,
      final int timeout, final MigrateParams params, final byte[]... keys) {
    Consumer<Client> consumer = client -> client.migrate(host, port, destinationDB, timeout, params, keys);
    return addBatch(consumer, BuilderFactory.STRING);
  }

  public Response<Object> sendCommand(final ProtocolCommand cmd, final String... args) {
    Consumer<Client> consumer = client -> client.sendCommand(cmd, args);
    return addBatch(consumer, BuilderFactory.OBJECT);
  }

  public Response<Object> sendCommand(final ProtocolCommand cmd, final byte[]... args) {
    Consumer<Client> consumer = client -> client.sendCommand(cmd, args);
    return addBatch(consumer, BuilderFactory.OBJECT);
  }

  @Override
  public Response<Long> georadiusStore(final String key, final double longitude, final double latitude,
      final double radius, final GeoUnit unit, final GeoRadiusParam param, final GeoRadiusStoreParam storeParam) {
    Consumer<Client> consumer = client -> client.georadiusStore(key, longitude, latitude, radius, unit, param, storeParam);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> georadiusStore(final byte[] key, final double longitude, final double latitude,
      final double radius, final GeoUnit unit, final GeoRadiusParam param, final GeoRadiusStoreParam storeParam) {
    Consumer<Client> consumer = client -> client.georadiusStore(key, longitude, latitude, radius, unit, param, storeParam);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> georadiusByMemberStore(final byte[] key, final byte[] member,
      final double radius, final GeoUnit unit, final GeoRadiusParam param, final GeoRadiusStoreParam storeParam) {
    Consumer<Client> consumer = client -> client.georadiusByMemberStore(key, member, radius, unit, param, storeParam);
    return addBatch(consumer, BuilderFactory.LONG);
  }

  @Override
  public Response<Long> georadiusByMemberStore(final String key, final String member,
      final double radius, final GeoUnit unit, final GeoRadiusParam param, final GeoRadiusStoreParam storeParam) {
    Consumer<Client> consumer = client -> client.georadiusByMemberStore(key, member, radius, unit, param, storeParam);
    return addBatch(consumer, BuilderFactory.LONG);
  }
}
