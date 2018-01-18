package redis.clients.jedis;

import redis.clients.jedis.commands.BasicRedisPipeline;
import redis.clients.jedis.commands.BinaryScriptingCommandsPipeline;
import redis.clients.jedis.commands.ClusterPipeline;
import redis.clients.jedis.commands.MultiKeyBinaryRedisPipeline;
import redis.clients.jedis.commands.MultiKeyCommandsPipeline;
import redis.clients.jedis.commands.ScriptingCommandsPipeline;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class MultiKeyPipelineBase extends PipelineBase implements
    MultiKeyBinaryRedisPipeline, MultiKeyCommandsPipeline, ClusterPipeline,
    BinaryScriptingCommandsPipeline, ScriptingCommandsPipeline, BasicRedisPipeline {

  protected abstract Client getClient();

  @Override
  public Response<List<String>> brpop(String... args) {
    getClient().brpop(args);
    return enqueueResponse(getClient(), BuilderFactory.STRING_LIST);
  }

  public Response<List<String>> brpop(int timeout, String... keys) {
    getClient().brpop(timeout, keys);
    return enqueueResponse(getClient(), BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<List<String>> blpop(String... args) {
    getClient().blpop(args);
    return enqueueResponse(getClient(), BuilderFactory.STRING_LIST);
  }

  public Response<List<String>> blpop(int timeout, String... keys) {
    getClient().blpop(timeout, keys);
    return enqueueResponse(getClient(), BuilderFactory.STRING_LIST);
  }

  public Response<Map<String, String>> blpopMap(int timeout, String... keys) {
    getClient().blpop(timeout, keys);
    return enqueueResponse(getClient(), BuilderFactory.STRING_MAP);
  }

  @Override
  public Response<List<byte[]>> brpop(byte[]... args) {
    getClient().brpop(args);
    return enqueueResponse(getClient(), BuilderFactory.BYTE_ARRAY_LIST);
  }

  public Response<List<String>> brpop(int timeout, byte[]... keys) {
    getClient().brpop(timeout, keys);
    return enqueueResponse(getClient(), BuilderFactory.STRING_LIST);
  }

  public Response<Map<String, String>> brpopMap(int timeout, String... keys) {
    getClient().blpop(timeout, keys);
    return enqueueResponse(getClient(), BuilderFactory.STRING_MAP);
  }

  @Override
  public Response<List<byte[]>> blpop(byte[]... args) {
    getClient().blpop(args);
    return enqueueResponse(getClient(), BuilderFactory.BYTE_ARRAY_LIST);
  }

  public Response<List<String>> blpop(int timeout, byte[]... keys) {
    getClient().blpop(timeout, keys);
    return enqueueResponse(getClient(), BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<Long> del(String... keys) {
    getClient().del(keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> del(byte[]... keys) {
    getClient().del(keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> unlink(String... keys) {
    getClient().unlink(keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> unlink(byte[]... keys) {
    getClient().unlink(keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> exists(String... keys) {
    getClient().exists(keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> exists(byte[]... keys) {
    getClient().exists(keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Set<String>> keys(String pattern) {
    getClient(pattern).keys(pattern);
    return enqueueResponse(getClient(), BuilderFactory.STRING_SET);
  }

  @Override
  public Response<Set<byte[]>> keys(byte[] pattern) {
    getClient(pattern).keys(pattern);
    return enqueueResponse(getClient(), BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<List<String>> mget(String... keys) {
    getClient().mget(keys);
    return enqueueResponse(getClient(), BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<List<byte[]>> mget(byte[]... keys) {
    getClient().mget(keys);
    return enqueueResponse(getClient(), BuilderFactory.BYTE_ARRAY_LIST);
  }

  @Override
  public Response<String> mset(String... keysvalues) {
    getClient().mset(keysvalues);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> mset(byte[]... keysvalues) {
    getClient().mset(keysvalues);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<Long> msetnx(String... keysvalues) {
    getClient().msetnx(keysvalues);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> msetnx(byte[]... keysvalues) {
    getClient().msetnx(keysvalues);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<String> rename(String oldkey, String newkey) {
    getClient().rename(oldkey, newkey);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> rename(byte[] oldkey, byte[] newkey) {
    getClient().rename(oldkey, newkey);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<Long> renamenx(String oldkey, String newkey) {
    getClient().renamenx(oldkey, newkey);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> renamenx(byte[] oldkey, byte[] newkey) {
    getClient().renamenx(oldkey, newkey);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<String> rpoplpush(String srckey, String dstkey) {
    getClient().rpoplpush(srckey, dstkey);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<byte[]> rpoplpush(byte[] srckey, byte[] dstkey) {
    getClient().rpoplpush(srckey, dstkey);
    return enqueueResponse(getClient(), BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<Set<String>> sdiff(String... keys) {
    getClient().sdiff(keys);
    return enqueueResponse(getClient(), BuilderFactory.STRING_SET);
  }

  @Override
  public Response<Set<byte[]>> sdiff(byte[]... keys) {
    getClient().sdiff(keys);
    return enqueueResponse(getClient(), BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Long> sdiffstore(String dstkey, String... keys) {
    getClient().sdiffstore(dstkey, keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sdiffstore(byte[] dstkey, byte[]... keys) {
    getClient().sdiffstore(dstkey, keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Set<String>> sinter(String... keys) {
    getClient().sinter(keys);
    return enqueueResponse(getClient(), BuilderFactory.STRING_SET);
  }

  @Override
  public Response<Set<byte[]>> sinter(byte[]... keys) {
    getClient().sinter(keys);
    return enqueueResponse(getClient(), BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Long> sinterstore(String dstkey, String... keys) {
    getClient().sinterstore(dstkey, keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sinterstore(byte[] dstkey, byte[]... keys) {
    getClient().sinterstore(dstkey, keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> smove(String srckey, String dstkey, String member) {
    getClient().smove(srckey, dstkey, member);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> smove(byte[] srckey, byte[] dstkey, byte[] member) {
    getClient().smove(srckey, dstkey, member);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sort(String key, SortingParams sortingParameters, String dstkey) {
    getClient().sort(key, sortingParameters, dstkey);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
    getClient().sort(key, sortingParameters, dstkey);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sort(String key, String dstkey) {
    getClient().sort(key, dstkey);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sort(byte[] key, byte[] dstkey) {
    getClient().sort(key, dstkey);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Set<String>> sunion(String... keys) {
    getClient().sunion(keys);
    return enqueueResponse(getClient(), BuilderFactory.STRING_SET);
  }

  @Override
  public Response<Set<byte[]>> sunion(byte[]... keys) {
    getClient().sunion(keys);
    return enqueueResponse(getClient(), BuilderFactory.BYTE_ARRAY_ZSET);
  }

  @Override
  public Response<Long> sunionstore(String dstkey, String... keys) {
    getClient().sunionstore(dstkey, keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> sunionstore(byte[] dstkey, byte[]... keys) {
    getClient().sunionstore(dstkey, keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<String> watch(String... keys) {
    getClient().watch(keys);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> watch(byte[]... keys) {
    getClient().watch(keys);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<Long> zinterstore(String dstkey, String... sets) {
    getClient().zinterstore(dstkey, sets);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zinterstore(byte[] dstkey, byte[]... sets) {
    getClient().zinterstore(dstkey, sets);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zinterstore(String dstkey, ZParams params, String... sets) {
    getClient().zinterstore(dstkey, params, sets);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
    getClient().zinterstore(dstkey, params, sets);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zunionstore(String dstkey, String... sets) {
    getClient().zunionstore(dstkey, sets);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zunionstore(byte[] dstkey, byte[]... sets) {
    getClient().zunionstore(dstkey, sets);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zunionstore(String dstkey, ZParams params, String... sets) {
    getClient().zunionstore(dstkey, params, sets);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
    getClient().zunionstore(dstkey, params, sets);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<String> bgrewriteaof() {
    getClient().bgrewriteaof();
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> bgsave() {
    getClient().bgsave();
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<List<String>> configGet(String pattern) {
    getClient().configGet(pattern);
    return enqueueResponse(getClient(), BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<String> configSet(String parameter, String value) {
    getClient().configSet(parameter, value);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> brpoplpush(String source, String destination, int timeout) {
    getClient().brpoplpush(source, destination, timeout);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<byte[]> brpoplpush(byte[] source, byte[] destination, int timeout) {
    getClient().brpoplpush(source, destination, timeout);
    return enqueueResponse(getClient(), BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<String> configResetStat() {
    getClient().configResetStat();
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> save() {
    getClient().save();
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<Long> lastsave() {
    getClient().lastsave();
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> publish(String channel, String message) {
    getClient().publish(channel, message);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> publish(byte[] channel, byte[] message) {
    getClient().publish(channel, message);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<String> randomKey() {
    getClient().randomKey();
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<byte[]> randomKeyBinary() {
    getClient().randomKey();
    return enqueueResponse(getClient(), BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<String> flushDB() {
    getClient().flushDB();
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> flushAll() {
    getClient().flushAll();
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> info() {
    getClient().info();
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  public Response<String> info(final String section) {
    getClient().info(section);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<Long> dbSize() {
    getClient().dbSize();
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<String> shutdown() {
    getClient().shutdown();
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> ping() {
    getClient().ping();
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> select(int index) {
    getClient().select(index);
    Response<String> response = enqueueResponse(getClient(), BuilderFactory.STRING);
    getClient().setDb(index);

    return response;
  }

  @Override
  public Response<String> swapDB(int index1, int index2) {
    getClient().swapDB(index1, index2);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<Long> bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
    getClient().bitop(op, destKey, srcKeys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> bitop(BitOP op, String destKey, String... srcKeys) {
    getClient().bitop(op, destKey, srcKeys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<String> clusterNodes() {
    getClient().clusterNodes();
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> clusterMeet(final String ip, final int port) {
    getClient().clusterMeet(ip, port);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> clusterAddSlots(final int... slots) {
    getClient().clusterAddSlots(slots);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> clusterDelSlots(final int... slots) {
    getClient().clusterDelSlots(slots);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> clusterInfo() {
    getClient().clusterInfo();
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<List<String>> clusterGetKeysInSlot(final int slot, final int count) {
    getClient().clusterGetKeysInSlot(slot, count);
    return enqueueResponse(getClient(), BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<String> clusterSetSlotNode(final int slot, final String nodeId) {
    getClient().clusterSetSlotNode(slot, nodeId);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> clusterSetSlotMigrating(final int slot, final String nodeId) {
    getClient().clusterSetSlotMigrating(slot, nodeId);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> clusterSetSlotImporting(final int slot, final String nodeId) {
    getClient().clusterSetSlotImporting(slot, nodeId);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
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
    getClient(script).eval(script, keyCount, params);
    return enqueueResponse(getClient(), BuilderFactory.EVAL_RESULT);
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
    getClient(sha1).evalsha(sha1, keyCount, params);
    return enqueueResponse(getClient(), BuilderFactory.EVAL_RESULT);
  }

  @Override
  public Response<Object> eval(byte[] script) {
    return this.eval(script, 0);
  }

  @Override
  public Response<Object> eval(byte[] script, byte[] keyCount, byte[]... params) {
    getClient(script).eval(script, keyCount, params);
    return enqueueResponse(getClient(), BuilderFactory.EVAL_BINARY_RESULT);
  }

  @Override
  public Response<Object> eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
    byte[][] argv = BinaryJedis.getParamsWithBinary(keys, args);
    return this.eval(script, keys.size(), argv);
  }

  @Override
  public Response<Object> eval(byte[] script, int keyCount, byte[]... params) {
    getClient(script).eval(script, keyCount, params);
    return enqueueResponse(getClient(), BuilderFactory.EVAL_BINARY_RESULT);
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
    getClient(sha1).evalsha(sha1, keyCount, params);
    return enqueueResponse(getClient(), BuilderFactory.EVAL_BINARY_RESULT);
  }

  @Override
  public Response<Long> pfcount(String... keys) {
    getClient().pfcount(keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> pfcount(final byte[]... keys) {
    getClient().pfcount(keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<String> pfmerge(byte[] destkey, byte[]... sourcekeys) {
    getClient().pfmerge(destkey, sourcekeys);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<String> pfmerge(String destkey, String... sourcekeys) {
    getClient().pfmerge(destkey, sourcekeys);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<List<String>> time() {
    getClient().time();
    return enqueueResponse(getClient(), BuilderFactory.STRING_LIST);
  }

  @Override
  public Response<Long> touch(String... keys) {
    getClient().touch(keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<Long> touch(byte[]... keys) {
    getClient().touch(keys);
    return enqueueResponse(getClient(), BuilderFactory.LONG);
  }

  @Override
  public Response<String> moduleUnload(String name) {
    getClient().moduleUnload(name);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }

  @Override
  public Response<List<Module>> moduleList() {
    getClient().moduleList();
    return enqueueResponse(getClient(), BuilderFactory.MODULE_LIST);
  }

  @Override
  public Response<String> moduleLoad(String path) {
    getClient().moduleLoad(path);
    return enqueueResponse(getClient(), BuilderFactory.STRING);
  }  
  
  
}
