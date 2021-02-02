package redis.clients.jedis;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.Pool;

public abstract class AbstractJedisPool<J extends AbstractJedis> extends Pool<J> {

  public AbstractJedisPool(GenericObjectPoolConfig<J> poolConfig, PooledObjectFactory<J> factory) {
    super(poolConfig, factory);
  }

  @Override
  public J getResource() {
    J jedis = super.getResource();
    jedis.setDataSource(this);
    return jedis;
  }

  @Override
  public void returnResource(final J resource) {
    if (resource != null) {
      try {
        resource.resetState();
        returnResourceObject(resource);
      } catch (Exception e) {
        returnBrokenResource(resource);
        throw new JedisException("Resource is returned to the pool as broken", e);
      }
    }
  }
}
