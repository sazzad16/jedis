package redis.clients.jedis;

import java.net.URI;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import org.apache.commons.pool2.PooledObject;

import org.apache.commons.pool2.PooledObjectFactory;

import redis.clients.jedis.exceptions.InvalidURIException;
import redis.clients.jedis.util.JedisURIHelper;

/**
 * PoolableObjectFactory custom impl.
 */
public class JedisFactory extends AbstractJedisFactory<Jedis> implements PooledObjectFactory<Jedis> {

  private final JedisClientConfig config;

  protected JedisFactory(final String host, final int port, final int connectionTimeout,
      final int soTimeout, final String password, final int database, final String clientName) {
    this(host, port, connectionTimeout, soTimeout, password, database, clientName, false, null, null, null);
  }

  protected JedisFactory(final String host, final int port, final int connectionTimeout,
               final int soTimeout, final String user, final String password, final int database, final String clientName) {
    this(host, port, connectionTimeout, soTimeout, 0, user, password, database, clientName);
  }

  protected JedisFactory(final String host, final int port, final int connectionTimeout, final int soTimeout,
      final int infiniteSoTimeout, final String user, final String password, final int database, final String clientName) {
    this(host, port, connectionTimeout, soTimeout, infiniteSoTimeout, user, password, database, clientName, false, null, null, null);
  }

  protected JedisFactory(final String host, final int port, final int connectionTimeout,
      final int soTimeout, final String password, final int database, final String clientName,
      final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {
    this(host, port, connectionTimeout, soTimeout, null, password, database, clientName, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
  }

  protected JedisFactory(final String host, final int port, final int connectionTimeout,
               final int soTimeout, final String user, final String password, final int database, final String clientName,
               final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
               final HostnameVerifier hostnameVerifier) {
    this(host, port, connectionTimeout, soTimeout, 0, user, password, database, clientName, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
  }

  protected JedisFactory(final HostAndPort hostAndPort, final JedisClientConfig clientConfig) {
    super(hostAndPort);
    this.config = DefaultJedisClientConfig.copyConfig(clientConfig);
  }

  protected JedisFactory(final String host, final int port, final int connectionTimeout, final int soTimeout,
      final int infiniteSoTimeout, final String user, final String password, final int database,
      final String clientName, final boolean ssl, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
    super(new HostAndPort(host, port));
    this.config = DefaultJedisClientConfig.builder().withConnectionTimeout(connectionTimeout)
        .withSoTimeout(soTimeout).withInfiniteSoTimeout(infiniteSoTimeout).withUser(user)
        .withPassword(password).withDatabse(database).withClientName(clientName)
        .withSsl(ssl).withSslSocketFactory(sslSocketFactory)
        .withSslParameters(sslParameters).withHostnameVerifier(hostnameVerifier).build();
  }

  protected JedisFactory(final URI uri, final int connectionTimeout, final int soTimeout,
      final String clientName) {
    this(uri, connectionTimeout, soTimeout, clientName, null, null, null);
  }

  protected JedisFactory(final URI uri, final int connectionTimeout, final int soTimeout,
      final String clientName, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
    this(uri, connectionTimeout, soTimeout, 0, clientName, sslSocketFactory, sslParameters, hostnameVerifier);
  }

  protected JedisFactory(final URI uri, final int connectionTimeout, final int soTimeout,
      final int infiniteSoTimeout, final String clientName, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
    super(new HostAndPort(uri.getHost(), uri.getPort()));
    if (!JedisURIHelper.isValid(uri)) {
      throw new InvalidURIException(String.format(
          "Cannot open Redis connection due invalid URI. %s", uri.toString()));
    }
    this.config = DefaultJedisClientConfig.builder().withConnectionTimeout(connectionTimeout)
        .withSoTimeout(soTimeout).withInfiniteSoTimeout(infiniteSoTimeout)
        .withUser(JedisURIHelper.getUser(uri)).withPassword(JedisURIHelper.getPassword(uri))
        .withDatabse(JedisURIHelper.getDBIndex(uri)).withClientName(clientName)
        .withSsl(JedisURIHelper.isRedisSSLScheme(uri)).withSslSocketFactory(sslSocketFactory)
        .withSslParameters(sslParameters).withHostnameVerifier(hostnameVerifier).build();
  }

  protected JedisFactory(final JedisClientConfig clientConfig) {
    super();
    this.config = clientConfig;
  }

  @Override
  public void activateObject(PooledObject<Jedis> pooledJedis) throws Exception {
    final BinaryJedis jedis = pooledJedis.getObject();
    if (jedis.getDB() != config.getDatabase()) {
      jedis.select(config.getDatabase());
    }
  }

  @Override
  protected Jedis createObject(HostAndPort hostPort) {
    return new Jedis(hostPort, config);
  }
}