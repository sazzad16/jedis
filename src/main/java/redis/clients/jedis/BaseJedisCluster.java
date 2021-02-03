package redis.clients.jedis;

public class BaseJedisCluster extends JedisClusterBase<Jedis, JedisPool> {

  protected final JedisClusterConnectionHandler connectionHandler;
  protected final int maxAttempts;

  public BaseJedisCluster(int maxAttempts, JedisClusterConnectionHandler connectionHandler) {
    super(maxAttempts, connectionHandler);
    this.connectionHandler = connectionHandler;
    this.maxAttempts = maxAttempts;
  }

  public abstract class JedisClusterCommand<T> extends AbstractJedisClusterCommand<T, Jedis> {

    public JedisClusterCommand(JedisClusterConnectionHandler connectionHandler, int maxAttempts) {
      super(connectionHandler, maxAttempts);
    }
  }
}
