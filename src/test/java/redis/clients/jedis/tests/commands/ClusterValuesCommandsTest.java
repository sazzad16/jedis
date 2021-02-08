package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisDataException;

public class ClusterValuesCommandsTest extends ClusterJedisCommandsTestBase {

  @Test
  public void testHincrByFloat() {
    Double value = jedisCluster.hincrByFloat("foo", "bar", 1.5d);
    assertEquals((Double) 1.5d, value);
    value = jedisCluster.hincrByFloat("foo", "bar", -1.5d);
    assertEquals((Double) 0d, value);
    value = jedisCluster.hincrByFloat("foo", "bar", -10.7d);
    assertEquals(Double.valueOf(-10.7d), value);
  }

  @Test
  public void pipeline() {
    try (Pipeline p = jedisCluster.beginPipelining("foo")) {
      p.set("foo", "bar");
      p.get("foo");
      List<Object> results = p.syncAndReturnAll();

      assertEquals(2, results.size());
      assertEquals("OK", results.get(0));
      assertEquals("bar", results.get(1));
    }
  }

  @Test
  public void pipelineResponse() {
    jedisCluster.set("{pipe}.string", "foo");
    jedisCluster.lpush("{pipe}.list", "foo");
    jedisCluster.hset("{pipe}.hash", "foo", "bar");
    jedisCluster.zadd("{pipe}.zset", 1, "foo");
    jedisCluster.sadd("{pipe}.set", "foo");
    jedisCluster.setrange("{pipe}.setrange", 0, "0123456789");
    byte[] bytesForSetRange = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    jedisCluster.setrange("{pipe}.setrangebytes".getBytes(), 0, bytesForSetRange);

    try (Pipeline p = jedisCluster.beginPipelining("{pipe}")) {
      Response<String> string = p.get("{pipe}.string");
      Response<String> list = p.lpop("{pipe}.list");
      Response<String> hash = p.hget("{pipe}.hash", "foo");
      Response<Set<String>> zset = p.zrange("{pipe}.zset", 0, -1);
      Response<String> set = p.spop("{pipe}.set");
      Response<Boolean> blist = p.exists("{pipe}.list");
      Response<Double> zincrby = p.zincrby("{pipe}.zset", 1, "foo");
      Response<Long> zcard = p.zcard("{pipe}.zset");
      p.lpush("{pipe}.list", "bar");
      Response<List<String>> lrange = p.lrange("{pipe}.list", 0, -1);
      Response<Map<String, String>> hgetAll = p.hgetAll("{pipe}.hash");
      p.sadd("{pipe}.set", "foo");
      Response<Set<String>> smembers = p.smembers("{pipe}.set");
      Response<Set<Tuple>> zrangeWithScores = p.zrangeWithScores("{pipe}.zset", 0, -1);
      Response<String> getrange = p.getrange("{pipe}.setrange", 1, 3);
      Response<byte[]> getrangeBytes = p.getrange("{pipe}.setrangebytes".getBytes(), 6, 8);
      p.sync();

      assertEquals("foo", string.get());
      assertEquals("foo", list.get());
      assertEquals("bar", hash.get());
      assertEquals("foo", zset.get().iterator().next());
      assertEquals("foo", set.get());
      assertEquals(false, blist.get());
      assertEquals(Double.valueOf(2), zincrby.get());
      assertEquals(Long.valueOf(1), zcard.get());
      assertEquals(1, lrange.get().size());
      assertNotNull(hgetAll.get().get("foo"));
      assertEquals(1, smembers.get().size());
      assertEquals(1, zrangeWithScores.get().size());
      assertEquals("123", getrange.get());
      byte[] expectedGetRangeBytes = {6, 7, 8};
      assertArrayEquals(expectedGetRangeBytes, getrangeBytes.get());
    }
  }

  @Test
  public void pipelineSelect() {
    try (Pipeline p = jedisCluster.beginPipelining("select")) {
      p.select(1);
      List<Object> resp = p.syncAndReturnAll();
      assertEquals(JedisDataException.class, resp.get(0).getClass());
    }
  }

  @Test
  public void pipelineResponseWithData() {
    jedisCluster.zadd("zset", 1, "foo");

    try (Pipeline p = jedisCluster.beginPipelining("zset")) {
      Response<Double> score = p.zscore("zset", "foo");
      p.sync();

      assertNotNull(score.get());
    }
  }

  @Test
  public void pipelineResponseWithoutData() {
    jedisCluster.zadd("zset", 1, "foo");

    try (Pipeline p = jedisCluster.beginPipelining("zset")) {
      Response<Double> score = p.zscore("zset", "bar");
      p.sync();

      assertNull(score.get());
    }
  }

  @Test(expected = JedisDataException.class)
  public void pipelineResponseWithinPipeline() {
    jedisCluster.set("string", "foo");

    try (Pipeline p = jedisCluster.beginPipelining("string")) {
      Response<String> string = p.get("string");
      string.get();
      p.sync();
    }
  }

  @Test
  public void multi() {
    try (Transaction trans = jedisCluster.beginTransaction("foo")) {
      trans.sadd("foo", "a");
      trans.sadd("foo", "b");
      trans.scard("foo");
      
      List<Object> response = trans.exec();
      
      List<Object> expected = new ArrayList<Object>();
      expected.add(1L);
      expected.add(1L);
      expected.add(2L);
      assertEquals(expected, response);
    }
  }

  @Test
  public void discard() {
    try (Transaction t = jedisCluster.beginTransaction("")) {
      String status = t.discard();
      assertEquals("OK", status);
    }
  }

  @Test
  public void transactionResponse() {
    jedisCluster.set("{tx}string", "foo");
    jedisCluster.lpush("{tx}list", "foo");
    jedisCluster.hset("{tx}hash", "foo", "bar");
    jedisCluster.zadd("{tx}zset", 1, "foo");
    jedisCluster.sadd("{tx}set", "foo");

    try (Transaction t = jedisCluster.beginTransaction("{tx}")) {
      Response<String> string = t.get("{tx}string");
      Response<String> list = t.lpop("{tx}list");
      Response<String> hash = t.hget("{tx}hash", "foo");
      Response<Set<String>> zset = t.zrange("{tx}zset", 0, -1);
      Response<String> set = t.spop("{tx}set");
      t.exec();
      
      assertEquals("foo", string.get());
      assertEquals("foo", list.get());
      assertEquals("bar", hash.get());
      assertEquals("foo", zset.get().iterator().next());
      assertEquals("foo", set.get());
    }
  }

  @Test(expected = JedisDataException.class)
  public void transactionResponseWithinPipeline() {
    jedisCluster.set("string", "foo");

    try (Transaction t = jedisCluster.beginTransaction("string")) {
      Response<String> string = t.get("string");
      string.get();
      t.exec();
    }
  }

  @Test
  public void transactionResponseWithError() {
    Transaction t = jedisCluster.beginTransaction("foo");
    t.set("foo", "bar");
    Response<Set<String>> error = t.smembers("foo");
    Response<String> r = t.get("foo");
    List<Object> l = t.exec();
    assertEquals(JedisDataException.class, l.get(1).getClass());
    try {
      error.get();
      fail("We expect exception here!");
    } catch (JedisDataException e) {
      // that is fine we should be here
    }
    assertEquals("bar", r.get());
  }

  @Test
  public void execGetResponse() {
    Transaction t = jedisCluster.beginTransaction("foo");

    t.set("foo", "bar");
    t.smembers("foo");
    t.get("foo");

    List<Response<?>> lr = t.execGetResponse();
    try {
      lr.get(1).get();
      fail("We expect exception here!");
    } catch (JedisDataException e) {
      // that is fine we should be here
    }
    assertEquals("bar", lr.get(2).get());
  }

  @Test
  public void testCloseable() throws IOException {
//    // we need to test with fresh instance of Jedis
//    Jedis jedis2 = new Jedis(hnp.getHost(), hnp.getPort(), 500);
//    jedis2.auth("foobared");

//    Transaction transaction = jedis2.beginTransaction();
    Transaction transaction = jedisCluster.beginTransaction("{c}");
    transaction.set("{c}a", "1");
    transaction.set("{c}b", "2");

    transaction.close();

    try {
      transaction.exec();
      fail("close should discard transaction");
    } catch (JedisDataException e) {
      assertTrue(e.getMessage().contains("EXEC without MULTI"));
      // pass
    }
  }

}
