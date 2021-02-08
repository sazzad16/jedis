package redis.clients.jedis;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.exceptions.InvalidURIException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.JedisURIHelper;

/**
 * PoolableObjectFactory custom impl.
 * @param <J>
 */
public abstract class AbstractJedisFactory<J extends JedisBase> implements PooledObjectFactory<J> {

  private static final Logger logger = LoggerFactory.getLogger(JedisFactory.class);

  private final AtomicReference<HostAndPort> hostAndPort = new AtomicReference<>();

  protected AbstractJedisFactory() {
  }

  protected AbstractJedisFactory(final HostAndPort hostAndPort) {
    this.hostAndPort.set(hostAndPort);
  }

  public void setHostAndPort(final HostAndPort hostAndPort) {
    this.hostAndPort.set(hostAndPort);
  }

  @Override
  public void activateObject(PooledObject<J> pooledJedis) throws Exception {
    // select db ?
  }

  @Override
  public void destroyObject(PooledObject<J> pooledJedis) throws Exception {
    final J jedis = pooledJedis.getObject();
    if (jedis.isConnected()) {
      try {
        // need a proper test, probably with mock
        if (!jedis.isBroken()) {
          jedis.quit();
        }
      } catch (Exception e) {
        logger.warn("Error while QUIT", e);
      }
      try {
        jedis.close();
      } catch (Exception e) {
        logger.warn("Error while close", e);
      }
    }
  }

  protected abstract J createObject(HostAndPort hostPort);

  @Override
  public PooledObject<J> makeObject() throws Exception {
    final HostAndPort hostPort = this.hostAndPort.get();
    if (hostPort == null) {
      throw new JedisException("No HostAndPort is set yet.");
    }
    J jedis = null;

    try {
      jedis = createObject(hostPort);
      jedis.connect();
      return new DefaultPooledObject<>(jedis);
    } catch (JedisException je) {
      if (jedis != null) {
        try {
          jedis.quit();
        } catch (Exception e) {
          logger.warn("Error while QUIT", e);
        }
        try {
          jedis.close();
        } catch (Exception e) {
          logger.warn("Error while close", e);
        }
      }
      throw je;
    }
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