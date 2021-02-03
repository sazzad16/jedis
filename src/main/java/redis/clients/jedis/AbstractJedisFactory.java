package redis.clients.jedis;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import redis.clients.jedis.exceptions.InvalidURIException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.JedisURIHelper;

/**
 * PoolableObjectFactory custom impl.
 * @param <J>
 */
public abstract class AbstractJedisFactory<J extends JedisBase> implements PooledObjectFactory<J> {

  private final AtomicReference<HostAndPort> hostAndPort = new AtomicReference<>();

  private final String user;
  private final String password;
  private final int database;
  private final String clientName;

  protected AbstractJedisFactory() {
    this(null, null, Protocol.DEFAULT_DATABASE, null);
  }

  protected AbstractJedisFactory(final HostAndPort hostAndPort, final JedisClientConfig clientConfig) {
    this.hostAndPort.set(hostAndPort);
    this.user = clientConfig.getUser();
    this.password = clientConfig.getPassword();
    this.database = clientConfig.getDatabase();
    this.clientName = clientConfig.getClientName();
  }

  protected AbstractJedisFactory(final String host, final int port, final String user, final String password,
      final int database, final String clientName) {
    this.hostAndPort.set(new HostAndPort(host, port));
    this.user = user;
    this.password = password;
    this.database = database;
    this.clientName = clientName;
  }

  protected AbstractJedisFactory(final URI uri, final String clientName) {
    if (!JedisURIHelper.isValid(uri)) {
      throw new InvalidURIException(String.format(
          "Cannot open Redis connection due invalid URI. %s", uri.toString()));
    }
    this.hostAndPort.set(new HostAndPort(uri.getHost(), uri.getPort()));
    this.user = JedisURIHelper.getUser(uri);
    this.password = JedisURIHelper.getPassword(uri);
    this.database = JedisURIHelper.getDBIndex(uri);
    this.clientName = clientName;
  }

  protected AbstractJedisFactory(final JedisClientConfig clientConfig) {
    this.user = clientConfig.getUser();
    this.password = clientConfig.getPassword();
    this.database = clientConfig.getDatabase();
    this.clientName = clientConfig.getClientName();
  }

  protected AbstractJedisFactory(final String user, final String password, final int database, final String clientName) {
    this.user = user;
    this.password = password;
    this.database = database;
    this.clientName = clientName;
  }

  public void setHostAndPort(final HostAndPort hostAndPort) {
    this.hostAndPort.set(hostAndPort);
  }

  @Override
  public void activateObject(PooledObject<J> pooledJedis) throws Exception {
    final J jedis = pooledJedis.getObject();
    if (jedis.getDB() != database) {
      jedis.select(database);
    }
  }

  @Override
  public void destroyObject(PooledObject<J> pooledJedis) throws Exception {
    final J jedis = pooledJedis.getObject();
    if (jedis.isConnected() && !jedis.isBroken()) {
      try {
        jedis.quit();
      } catch (Exception e) {
      }
    }
    try {
      jedis.disconnect();
    } catch (Exception e) {
    }
  }

  protected abstract J createObject(HostAndPort hostPort);

  @Override
  public PooledObject<J> makeObject() throws Exception {
    final HostAndPort hostPort = this.hostAndPort.get();
    if (hostPort == null) {
      throw new JedisException("No HostAndPort is set yet.");
    }
    final J jedis = createObject(hostPort);

    try {
      jedis.connect();
      if (user != null) {
        jedis.auth(user, password);
      } else if (password != null) {
        jedis.auth(password);
      }
      if (database != 0) {
        jedis.select(database);
      }
      if (clientName != null) {
        jedis.clientSetname(clientName);
      }
    } catch (JedisException je) {
      try {
        if (jedis.isConnected() && !jedis.isBroken()) {
          jedis.quit();
        }
      } catch(Exception e) {
        // swallow
      }
      jedis.close();
      throw je;
    }
    return new DefaultPooledObject<>(jedis);
  }

  @Override
  public void passivateObject(PooledObject<J> pooledJedis) throws Exception {
    // TODO maybe should select db 0? Not sure right now.
  }

  @Override
  public boolean validateObject(PooledObject<J> pooledJedis) {
    final J jedis = pooledJedis.getObject();
    try {
      HostAndPort hostPort = this.hostAndPort.get();

      String connectionHost = jedis.getClient().getHost();
      int connectionPort = jedis.getClient().getPort();

      return hostPort.getHost().equals(connectionHost)
          && hostPort.getPort() == connectionPort && jedis.isConnected()
          && jedis.ping().equals("PONG");
    } catch (final Exception e) {
      return false;
    }
  }
}