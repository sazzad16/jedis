package redis.clients.jedis;

import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class JedisClusterConnectionHandler extends AbstractJedisClusterConnectionHandler<Jedis, JedisPool> {

  private final GenericObjectPoolConfig poolConfig;
  private final JedisClientConfig clusterNodesConfig;
  private final JedisClientConfig seedNodesConfig;

  public JedisClusterConnectionHandler(Set<HostAndPort> nodes,
      GenericObjectPoolConfig<Jedis> poolConfig, int connectionTimeout, int soTimeout,
      String password) {
    this(nodes, poolConfig, connectionTimeout, soTimeout, password, null);
  }

  public JedisClusterConnectionHandler(Set<HostAndPort> nodes,
      GenericObjectPoolConfig<Jedis> poolConfig, int connectionTimeout, int soTimeout,
      String password, String clientName) {
    this(nodes, poolConfig, connectionTimeout, soTimeout, null, password, clientName);
  }

  public JedisClusterConnectionHandler(Set<HostAndPort> nodes,
      final GenericObjectPoolConfig<Jedis> poolConfig, int connectionTimeout, int soTimeout,
      String user, String password, String clientName) {
    this(nodes, poolConfig, connectionTimeout, soTimeout, 0, user, password, clientName);
  }

  public JedisClusterConnectionHandler(Set<HostAndPort> nodes,
      final GenericObjectPoolConfig<Jedis> poolConfig, int connectionTimeout, int soTimeout,
      int infiniteSoTimeout, String user, String password, String clientName) {
    this(nodes, poolConfig,
        DefaultJedisClientConfig.builder().connectionTimeoutMillis(connectionTimeout)
            .socketTimeoutMillis(soTimeout).blockingSocketTimeoutMillis(infiniteSoTimeout)
            .user(user).password(password).clientName(clientName).build());
  }

  public JedisClusterConnectionHandler(Set<HostAndPort> nodes,
      final GenericObjectPoolConfig poolConfig, final JedisClientConfig clientConfig) {
    super(nodes);
    this.poolConfig = poolConfig;
    this.clusterNodesConfig = clientConfig;
    this.seedNodesConfig = clientConfig;
    initializeSlotsCache();
  }

  @Override
  protected Jedis createSeedConnection(HostAndPort hostAndPort) {
    JedisClientConfig clientConfig = seedNodesConfig;
    Jedis jedis = new Jedis(hostAndPort, clientConfig);
    return jedis;
  }

  @Override
  protected JedisPool createPool(HostAndPort node) {
    return new JedisPool(poolConfig, node, clusterNodesConfig);
  }

  @Override
  public Object generateClusterConfigResponse(Jedis jedis) {
    return jedis.clusterSlots();
  }

}
