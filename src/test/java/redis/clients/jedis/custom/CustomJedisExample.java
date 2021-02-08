package redis.clients.jedis.custom;

import static org.junit.Assert.assertEquals;

import java.util.Set;
import org.junit.Test;

import redis.clients.jedis.AbstractJedisClusterConnectionHandler;
import redis.clients.jedis.AbstractJedisFactory;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisBase;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterBase;
import redis.clients.jedis.JedisPoolBase;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;

public class CustomJedisExample {

  public interface CustomInterface {

    Long incr(String key, long value);
  }

  public static class CustomJedis extends JedisBase implements CustomInterface {

    public CustomJedis(HostAndPort hp, JedisClientConfig clientConfig) {
      super(hp, clientConfig);
    }

    @Override
    public Long incr(String key, long value) {
      checkIsInMultiOrPipeline();
      client.sendCommand(Protocol.Command.INCRBY, key, Long.toString(value));
      return BuilderFactory.LONG.build(client.getOne());
    }
  }

  public static class CustomJedisFactory extends AbstractJedisFactory<CustomJedis> {

    private final JedisClientConfig clientConfig;

    public CustomJedisFactory(HostAndPort hp, JedisClientConfig clientConfig) {
      super(hp);
      this.clientConfig = clientConfig;
    }

    @Override
    protected CustomJedis createObject(HostAndPort hostPort) {
      return new CustomJedis(hostPort, clientConfig);
    }
  }

  public static class CustomJedisPool extends JedisPoolBase<CustomJedis> {

    public CustomJedisPool(HostAndPort hp, JedisClientConfig clientConfig) {
      super(new JedisPoolConfig(), new CustomJedisFactory(hp, clientConfig));
    }
  }

  public static class CustomClusterConnectionHandler
      extends AbstractJedisClusterConnectionHandler<CustomJedis, CustomJedisPool> {

    private final JedisClientConfig clientConfig;

    public CustomClusterConnectionHandler(Set<HostAndPort> nodes, JedisClientConfig clientConfig) {
      super(nodes);
      this.clientConfig = clientConfig;
      initializeSlotsCache();
    }

    @Override
    protected CustomJedis createSeedConnection(HostAndPort hostAndPort) throws JedisException {
      return new CustomJedis(hostAndPort, clientConfig);
    }

    @Override
    public Object generateClusterConfigResponse(CustomJedis jedis) {
      jedis.getClient().clusterSlots();
      return jedis.getClient().getObjectMultiBulkReply();
    }

    @Override
    protected CustomJedisPool createPool(HostAndPort node) {
      return new CustomJedisPool(node, clientConfig);
    }
  }

  public static class CustomJedisCluster extends JedisClusterBase<CustomJedis, CustomJedisPool, CustomClusterConnectionHandler> implements CustomInterface {

    public CustomJedisCluster(Set<HostAndPort> nodes, JedisClientConfig clientConfig) {
      super(new CustomClusterConnectionHandler(nodes, clientConfig), JedisCluster.DEFAULT_MAX_ATTEMPTS);
    }

    @Override
    public Long incr(String key, long value) {
      return new JedisClusterCommand<Long>() {
        @Override
        public Long execute(CustomJedis connection) {
          return connection.incr(key, value);
        }
      }.run(key);
    }
  }
}
