package redis.clients.jedis.custom;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.custom.CustomJedisExample.CustomJedis;
import redis.clients.jedis.custom.CustomJedisExample.CustomJedisPool;
import redis.clients.jedis.tests.HostAndPortUtil;
import redis.clients.jedis.tests.commands.JedisCommandTestBase;

public class CustomJedisTest extends JedisCommandTestBase {

  private static final HostAndPort NODE = HostAndPortUtil.getRedisServers().get(0);

  @Test
  public void testCustomJedis() {
    String key = "custom.key";
    try (CustomJedis customJedis = new CustomJedis(NODE,
        DefaultJedisClientConfig.builder().password("foobared").build())) {

      assertEquals(1L, (long) customJedis.incr(key, 1));
      assertEquals(3L, (long) customJedis.incr(key, 2));

      try (Pipeline pipeline = customJedis.beginPipelining()) {
        Response<Long> response1 = pipeline.incr(key);
        Response<Long> response2 = pipeline.incrBy(key, 5);
        pipeline.sync();
        assertEquals(4L, (long) response1.get());
        assertEquals(9L, (long) response2.get());
      }

      try (Transaction transaction = customJedis.beginTransaction()) {
        Response<Long> response1 = transaction.incr(key);
        Response<Long> response2 = transaction.incrBy(key, 2);
        transaction.exec();
        assertEquals(10L, (long) response1.get());
        assertEquals(12L, (long) response2.get());
      }
    }
  }

  @Test
  public void testCustomJedisPool() {
    String key = "custom.key";
    try (CustomJedisPool customPool = new CustomJedisPool(NODE,
        DefaultJedisClientConfig.builder().password("foobared").build())) {

      try (CustomJedis customJedis = customPool.getResource()) {

        assertEquals(1L, (long) customJedis.incr(key, 1));
        assertEquals(3L, (long) customJedis.incr(key, 2));

        try (Pipeline pipeline = customJedis.beginPipelining()) {
          Response<Long> response1 = pipeline.incr(key);
          Response<Long> response2 = pipeline.incrBy(key, 5);
          pipeline.sync();
          assertEquals(4L, (long) response1.get());
          assertEquals(9L, (long) response2.get());
        }

        try (Transaction transaction = customJedis.beginTransaction()) {
          Response<Long> response1 = transaction.incr(key);
          Response<Long> response2 = transaction.incrBy(key, 2);
          transaction.exec();
          assertEquals(10L, (long) response1.get());
          assertEquals(12L, (long) response2.get());
        }
      }
    }
  }
}
