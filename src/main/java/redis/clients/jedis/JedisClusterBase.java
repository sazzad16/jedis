package redis.clients.jedis;

import java.io.Closeable;
import java.util.Map;
import redis.clients.jedis.commands.ProtocolCommand;

public class JedisClusterBase<J extends JedisBase, P extends JedisPoolBase<J>> implements Closeable {

  public static final int HASHSLOTS = 16384;
  protected static final int DEFAULT_TIMEOUT = 2000;
  protected static final int DEFAULT_MAX_ATTEMPTS = 5;

  private final int maxAttempts;

  private final AbstractJedisClusterConnectionHandler<J, P> connectionHandler;

  public JedisClusterBase(int maxAttempts, AbstractJedisClusterConnectionHandler<J, P> connectionHandler) {
    this.maxAttempts = maxAttempts;
    this.connectionHandler = connectionHandler;
  }

  @Override
  public void close() {
    connectionHandler.close();
  }

  public Map<String, P> getClusterNodes() {
    return connectionHandler.getNodes();
  }

  public J getConnectionFromSlot(int slot) {
    return this.connectionHandler.getConnectionFromSlot(slot);
  }

  public Object sendCommand(final byte[] sampleKey, final ProtocolCommand cmd, final byte[]... args) {
    return new AbstractJedisClusterCommand<Object, J>(connectionHandler, maxAttempts) {
      @Override
      public Object execute(J connection) {
        return connection.sendCommand(cmd, args);
      }
    }.runBinary(sampleKey);
  }

  public Object sendCommand(final String sampleKey, final ProtocolCommand cmd, final String... args) {
    return new AbstractJedisClusterCommand<Object, J>(connectionHandler, maxAttempts) {
      @Override
      public Object execute(J connection) {
        return connection.sendCommand(cmd, args);
      }
    }.run(sampleKey);
  }

  public Object sendBlockingCommand(final byte[] sampleKey, final ProtocolCommand cmd, final byte[]... args) {
    return new AbstractJedisClusterCommand<Object, J>(connectionHandler, maxAttempts) {
      @Override
      public Object execute(J connection) {
        return connection.sendBlockingCommand(cmd, args);
      }
    }.runBinary(sampleKey);
  }

  public Object sendBlockingCommand(final String sampleKey, final ProtocolCommand cmd, final String... args) {
    return new AbstractJedisClusterCommand<Object, J>(connectionHandler, maxAttempts) {
      @Override
      public Object execute(J connection) {
        return connection.sendBlockingCommand(cmd, args);
      }
    }.run(sampleKey);
  }
}
