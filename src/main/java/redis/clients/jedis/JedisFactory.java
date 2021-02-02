package redis.clients.jedis;

import java.net.URI;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.pool2.PooledObjectFactory;
import redis.clients.jedis.util.JedisURIHelper;

/**
 * PoolableObjectFactory custom impl.
 */
public class JedisFactory extends AbstractJedisFactory<Jedis> implements PooledObjectFactory<Jedis> {

  private final JedisSocketConfig socketConfig;
  private final int infiniteSoTimeout;

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
    super(hostAndPort, clientConfig);
    this.socketConfig = DefaultJedisSocketConfig.copyConfig(clientConfig);
    this.infiniteSoTimeout = clientConfig.getInfiniteSoTimeout();
  }

  protected JedisFactory(final String host, final int port, final int connectionTimeout, final int soTimeout,
      final int infiniteSoTimeout, final String user, final String password, final int database,
      final String clientName, final boolean ssl, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
    super(host, port, user, password, database, clientName);
    this.socketConfig = DefaultJedisSocketConfig.builder().withConnectionTimeout(connectionTimeout)
        .withSoTimeout(soTimeout).withSsl(ssl).withSslSocketFactory(sslSocketFactory)
        .withSslParameters(sslParameters).withHostnameVerifier(hostnameVerifier).build();
    this.infiniteSoTimeout = infiniteSoTimeout;
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
    super(uri, clientName);
    this.socketConfig = DefaultJedisSocketConfig.builder().withConnectionTimeout(connectionTimeout)
        .withSoTimeout(soTimeout).withSsl(JedisURIHelper.isRedisSSLScheme(uri))
        .withSslSocketFactory(sslSocketFactory).withSslParameters(sslParameters)
        .withHostnameVerifier(hostnameVerifier).build();
    this.infiniteSoTimeout = infiniteSoTimeout;
  }

  protected JedisFactory(final JedisClientConfig clientConfig) {
    super(clientConfig);
    this.socketConfig = DefaultJedisSocketConfig.copyConfig(clientConfig);
    this.infiniteSoTimeout = clientConfig.getInfiniteSoTimeout();
  }

  @Override
  protected Jedis createObject(HostAndPort hostPort) {
    return new Jedis(hostPort, socketConfig, infiniteSoTimeout);
  }
}