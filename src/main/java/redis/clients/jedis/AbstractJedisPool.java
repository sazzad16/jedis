package redis.clients.jedis;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.Pool;

public abstract class AbstractJedisPool<J extends AbstractJedis> extends Pool<J> {

  private static final Logger log = LoggerFactory.getLogger(JedisPool.class);

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
        log.warn("Resource is returned to the pool as broken", e);
      }
    }
  }
}
