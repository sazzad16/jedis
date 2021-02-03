package redis.clients.jedis;

import java.util.Set;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class JedisClusterConnectionHandler extends AbstractJedisClusterConnectionHandler<Jedis, JedisPool> {

  private final GenericObjectPoolConfig poolConfig;
  private final JedisClientConfig clusterNodesConfig;
  private final JedisClientConfig seedNodesConfig;

  public JedisClusterConnectionHandler(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig,
      int connectionTimeout, int soTimeout, String password) {
    this(nodes, poolConfig, connectionTimeout, soTimeout, password, null);
  }

  public JedisClusterConnectionHandler(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig,
      int connectionTimeout, int soTimeout, String password, String clientName) {
    this(nodes, poolConfig, connectionTimeout, soTimeout, null, password, clientName);
  }

  public JedisClusterConnectionHandler(Set<HostAndPort> nodes, final GenericObjectPoolConfig poolConfig,
      int connectionTimeout, int soTimeout, String user, String password, String clientName) {
    this(nodes, poolConfig, connectionTimeout, soTimeout, 0, user, password, clientName);
  }

  public JedisClusterConnectionHandler(Set<HostAndPort> nodes, final GenericObjectPoolConfig poolConfig,
      int connectionTimeout, int soTimeout, int infiniteSoTimeout, String user, String password, String clientName) {
    this(nodes, poolConfig, connectionTimeout, soTimeout, infiniteSoTimeout, user, password, clientName, false, null, null, null, null);
  }

  /**
   * @deprecated This constructor will be removed in future.
   */
  @Deprecated
  public JedisClusterConnectionHandler(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig,
      int connectionTimeout, int soTimeout, String password, String clientName,
      boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap portMap) {
    this(nodes, poolConfig, connectionTimeout, soTimeout, null, password, clientName, ssl, sslSocketFactory, sslParameters, hostnameVerifier, portMap);
  }

  /**
   * @deprecated This constructor will be removed in future.
   */
  @Deprecated
  public JedisClusterConnectionHandler(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig,
      int connectionTimeout, int soTimeout, String user, String password, String clientName,
      boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap portMap) {
    this(nodes, poolConfig, connectionTimeout, soTimeout, 0, user, password, clientName, ssl, sslSocketFactory, sslParameters, hostnameVerifier, portMap);
  }

  /**
   * @deprecated This constructor will be removed in future.
   */
  @Deprecated
  public JedisClusterConnectionHandler(Set<HostAndPort> nodes, final GenericObjectPoolConfig poolConfig,
      int connectionTimeout, int soTimeout, int infiniteSoTimeout, String user, String password, String clientName,
      boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters,
      HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap portMap) {
    this(nodes,
        DefaultJedisClientConfig.builder().withConnectionTimeout(connectionTimeout)
            .withSoTimeout(soTimeout).withInfiniteSoTimeout(infiniteSoTimeout)
            .withUser(user).withPassword(password).withClientName(clientName)
            .withSsl(ssl).withSslSocketFactory(sslSocketFactory).withSslParameters(sslParameters)
            .withHostnameVerifier(hostnameVerifier).build(),
        poolConfig,
        DefaultJedisClientConfig.builder().withConnectionTimeout(connectionTimeout)
            .withSoTimeout(soTimeout).withInfiniteSoTimeout(infiniteSoTimeout)
            .withUser(user).withPassword(password).withClientName(clientName)
            .withSsl(ssl).withSslSocketFactory(sslSocketFactory).withSslParameters(sslParameters)
            .withHostnameVerifier(hostnameVerifier).withHostAndPortMapper(portMap).build());
  }

  /**
   * @deprecated This constructor will be removed in future.
   */
  @Deprecated
  public JedisClusterConnectionHandler(Set<HostAndPort> nodes, final JedisClientConfig seedNodesClientConfig,
      final GenericObjectPoolConfig poolConfig, final JedisClientConfig clusterNodesClientConfig) {
    super(nodes);
    this.poolConfig = poolConfig;
    this.clusterNodesConfig = clusterNodesClientConfig;
    this.seedNodesConfig = seedNodesClientConfig;
    initializeSlotsCache();
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
    Jedis jedis = new Jedis(hostAndPort, (JedisSocketConfig) clientConfig, clientConfig.getInfiniteSoTimeout());
    if (clientConfig.getUser() != null) {
      jedis.auth(clientConfig.getUser(), clientConfig.getPassword());
    } else if (clientConfig.getPassword() != null) {
      jedis.auth(clientConfig.getPassword());
    }
    if (clientConfig.getClientName() != null) {
      jedis.clientSetname(clientConfig.getClientName());
    }
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
