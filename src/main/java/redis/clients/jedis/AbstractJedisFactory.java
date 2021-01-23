package redis.clients.jedis;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import redis.clients.jedis.exceptions.JedisException;

/**
 * PoolableObjectFactory custom impl.
 * @param <J>
 */
public abstract class AbstractJedisFactory<J extends JedisBase> implements PooledObjectFactory<J> {

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
  public void destroyObject(PooledObject<J> pooledJedis) throws Exception {
    final J jedis = pooledJedis.getObject();
    if (jedis.isConnected() && !jedis.isBroken()) {
      try {
        jedis.quit();
      } catch (Exception e) {
      }
    }
    try {
      jedis.close();
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
    } catch (JedisException je) {
      try {
        if (jedis.isConnected() && !jedis.isBroken()) {
          jedis.quit();
        }
      } catch (Exception e) {
      }
      try {
        jedis.close();
      } catch (Exception e) {
      }
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