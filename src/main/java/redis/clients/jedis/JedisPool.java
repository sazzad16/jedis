package redis.clients.jedis;

import java.net.URI;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import org.apache.commons.pool2.PooledObjectFactory;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.JedisURIHelper;

public class JedisPool extends JedisPoolBase<Jedis> {

  public JedisPool() {
    this(Protocol.DEFAULT_HOST, Protocol.DEFAULT_PORT);
  }

  public JedisPool(String host, int port) {
    this(new GenericObjectPoolConfig(), host, port);
  }

  public JedisPool(final String uri) {
    this(URI.create(uri));
  }

  public JedisPool(final String uri, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
    this(URI.create(uri), sslSocketFactory, sslParameters, hostnameVerifier);
  }

  public JedisPool(final URI uri) {
    this(new GenericObjectPoolConfig(), uri);
  }

  public JedisPool(final URI uri, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
    this(new GenericObjectPoolConfig(), uri, sslSocketFactory, sslParameters, hostnameVerifier);
  }

  public JedisPool(final URI uri, final int timeout) {
    this(new GenericObjectPoolConfig(), uri, timeout);
  }

  public JedisPool(final URI uri, final int timeout, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
    this(new GenericObjectPoolConfig(), uri, timeout, sslSocketFactory, sslParameters,
        hostnameVerifier);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password) {
    this(poolConfig, host, port, timeout, password, Protocol.DEFAULT_DATABASE);
  }

  public JedisPool(final String host, int port, String user, final String password) {
    this(new GenericObjectPoolConfig(), host, port, user, password);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      String user, final String password) {
    this(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT, user, password, Protocol.DEFAULT_DATABASE);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String user, final String password) {
    this(poolConfig, host, port, timeout, user, password, Protocol.DEFAULT_DATABASE);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password, final boolean ssl) {
    this(poolConfig, host, port, timeout, password, Protocol.DEFAULT_DATABASE, ssl);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String user, final String password, final boolean ssl) {
    this(poolConfig, host, port, timeout, user, password, Protocol.DEFAULT_DATABASE, ssl);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {
    this(poolConfig, host, port, timeout, password, Protocol.DEFAULT_DATABASE, ssl,
        sslSocketFactory, sslParameters, hostnameVerifier);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, final int port) {
    this(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, final int port,
      final boolean ssl) {
    this(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT, ssl);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, final int port,
      final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {
    this(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT, ssl, sslSocketFactory, sslParameters,
        hostnameVerifier);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, final int port,
      final int timeout) {
    this(poolConfig, host, port, timeout, null);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, final int port,
      final int timeout, final boolean ssl) {
    this(poolConfig, host, port, timeout, null, ssl);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, final int port,
      final int timeout, final boolean ssl, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
    this(poolConfig, host, port, timeout, null, ssl, sslSocketFactory, sslParameters,
        hostnameVerifier);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password, final int database) {
    this(poolConfig, host, port, timeout, password, database, null);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String user, final String password, final int database) {
    this(poolConfig, host, port, timeout, user, password, database, null);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password, final int database, final boolean ssl) {
    this(poolConfig, host, port, timeout, password, database, null, ssl);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String user, final String password, final int database, final boolean ssl) {
    this(poolConfig, host, port, timeout, user, password, database, null, ssl);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password, final int database, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {
    this(poolConfig, host, port, timeout, password, database, null, ssl, sslSocketFactory,
        sslParameters, hostnameVerifier);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password, final int database, final String clientName) {
    this(poolConfig, host, port, timeout, timeout, password, database, clientName);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String user, final String password, final int database, final String clientName) {
    this(poolConfig, host, port, timeout, timeout, user, password, database, clientName);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password, final int database, final String clientName,
      final boolean ssl) {
    this(poolConfig, host, port, timeout, timeout, password, database, clientName, ssl);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String user, final String password, final int database, final String clientName,
      final boolean ssl) {
    this(poolConfig, host, port, timeout, timeout, user, password, database, clientName, ssl);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password, final int database, final String clientName,
      final boolean ssl, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
    this(poolConfig, host, port, timeout, timeout, password, database, clientName, ssl,
        sslSocketFactory, sslParameters, hostnameVerifier);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      final int connectionTimeout, final int soTimeout, final String password, final int database,
      final String clientName, final boolean ssl, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
    super(poolConfig, new JedisFactory(host, port, connectionTimeout, soTimeout, password,
        database, clientName, ssl, sslSocketFactory, sslParameters, hostnameVerifier));
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      final int connectionTimeout, final int soTimeout, final int infiniteSoTimeout,
      final String password, final int database, final String clientName, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {
    this(poolConfig, host, port, connectionTimeout, soTimeout, infiniteSoTimeout, null, password,
        database, clientName, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      final int connectionTimeout, final int soTimeout, final String user, final String password,
      final int database, final String clientName, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {
    this(poolConfig, host, port, connectionTimeout, soTimeout, 0, user, password,
        database, clientName, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      final int connectionTimeout, final int soTimeout, final int infiniteSoTimeout,
      final String user, final String password, final int database, final String clientName, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {
    super(poolConfig, new JedisFactory(host, port, connectionTimeout, soTimeout, infiniteSoTimeout,
        user, password, database, clientName, ssl, sslSocketFactory, sslParameters, hostnameVerifier));
  }

  public JedisPool(final GenericObjectPoolConfig<Jedis> poolConfig, final HostAndPort hostAndPort,
      final JedisClientConfig clientConfig) {
    this(poolConfig, new JedisFactory(hostAndPort, clientConfig));
  }

  public JedisPool(final GenericObjectPoolConfig<Jedis> poolConfig, final PooledObjectFactory<Jedis> factory) {
    super(poolConfig, factory);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig) {
    this(poolConfig, Protocol.DEFAULT_HOST, Protocol.DEFAULT_PORT);
  }

  public JedisPool(final String host, final int port, final boolean ssl) {
    this(new GenericObjectPoolConfig(), host, port, ssl);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      final int connectionTimeout, final int soTimeout, final String password, final int database,
      final String clientName) {
    super(poolConfig, new JedisFactory(host, port, connectionTimeout, soTimeout, password,
        database, clientName));
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      final int connectionTimeout, final int soTimeout, final String user, final String password,
      final int database, final String clientName) {
    super(poolConfig, new JedisFactory(host, port, connectionTimeout, soTimeout, user, password,
        database, clientName));
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      final int connectionTimeout, final int soTimeout, final int infiniteSoTimeout,
      final String user, final String password, final int database, final String clientName) {
    super(poolConfig, new JedisFactory(host, port, connectionTimeout, soTimeout, infiniteSoTimeout,
        user, password, database, clientName));
  }

  public JedisPool(final String host, final int port, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {
    this(new GenericObjectPoolConfig(), host, port, ssl, sslSocketFactory, sslParameters,
        hostnameVerifier);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, final int port,
      final int connectionTimeout, final int soTimeout, final String password, final int database,
      final String clientName, final boolean ssl) {
    this(poolConfig, host, port, connectionTimeout, soTimeout, password, database, clientName, ssl,
        null, null, null);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, final int port,
      final int connectionTimeout, final int soTimeout, final String user, final String password,
      final int database, final String clientName, final boolean ssl) {
    this(poolConfig, host, port, connectionTimeout, soTimeout, user, password, database, clientName, ssl,
        null, null, null);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final URI uri) {
    this(poolConfig, uri, Protocol.DEFAULT_TIMEOUT);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {
    this(poolConfig, uri, Protocol.DEFAULT_TIMEOUT, sslSocketFactory, sslParameters,
        hostnameVerifier);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final URI uri, final int timeout) {
    this(poolConfig, uri, timeout, timeout);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final URI uri, final int timeout,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {
    this(poolConfig, uri, timeout, timeout, sslSocketFactory, sslParameters, hostnameVerifier);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int connectionTimeout, final int soTimeout) {
    this(poolConfig, uri, connectionTimeout, soTimeout, null, null, null);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int connectionTimeout, final int soTimeout, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
    super(poolConfig, new JedisFactory(uri, connectionTimeout, soTimeout, null, sslSocketFactory,
        sslParameters, hostnameVerifier));
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final URI uri,
      final int connectionTimeout, final int soTimeout, final int infiniteSoTimeout,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {
    super(poolConfig, new JedisFactory(uri, connectionTimeout, soTimeout, infiniteSoTimeout, null,
        sslSocketFactory, sslParameters, hostnameVerifier));
  }
}
