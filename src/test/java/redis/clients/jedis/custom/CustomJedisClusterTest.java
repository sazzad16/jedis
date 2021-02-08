package redis.clients.jedis.custom;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import org.junit.Test;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.tests.HostAndPortUtil;
import redis.clients.jedis.tests.commands.ClusterJedisCommandsTestBase;
import redis.clients.jedis.custom.CustomJedisExample.CustomJedisCluster;

import static org.junit.Assert.assertEquals;

public class CustomJedisClusterTest extends ClusterJedisCommandsTestBase {

  private static final HostAndPort SEED_NODE = HostAndPortUtil.getClusterServers().get(0);

  @Test
  public void testCustomJedisCluster() {
    String key = "{custom}.key";
    try (CustomJedisCluster customCluster = new CustomJedisCluster(Collections.singleton(SEED_NODE),
        DefaultJedisClientConfig.builder().password("cluster").build())) {

      assertEquals(1L, (long) customCluster.incr(key, 1));
      assertEquals(3L, (long) customCluster.incr(key, 2));

      try (Pipeline pipeline = customCluster.beginPipelining(key)) {
        Response<Long> response1 = pipeline.incr(key);
        Response<Long> response2 = pipeline.incrBy(key, 5);
        pipeline.sync();
        assertEquals(4L, (long) response1.get());
        assertEquals(9L, (long) response2.get());
      }

      try (Transaction transaction = customCluster.beginTransaction(key)) {
        Response<Long> response1 = transaction.incr(key);
        Response<Long> response2 = transaction.incrBy(key, 2);
        transaction.exec();
        assertEquals(10L, (long) response1.get());
        assertEquals(12L, (long) response2.get());
      }
    }
  }
}
