package redis.clients.jedis;

import java.io.Closeable;
import java.util.Map;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.JedisClusterCRC16;

public class JedisClusterBase<J extends JedisBase, P extends JedisPoolBase<J>,
    H extends AbstractJedisClusterConnectionHandler<J, P>> implements Closeable {

  public static final int HASHSLOTS = 16384;
  protected static final int DEFAULT_TIMEOUT = 2000;
  protected static final int DEFAULT_MAX_ATTEMPTS = 5;

  protected final int maxAttempts;

  protected final H connectionHandler;

  public JedisClusterBase(H connectionHandler, int maxAttempts) {
    this.maxAttempts = maxAttempts;
    this.connectionHandler = connectionHandler;
  }

  public abstract class JedisClusterCommand<T> extends AbstractJedisClusterCommand<T, J> {

    public JedisClusterCommand(H connectionHandler, int maxAttempts) {
      super(connectionHandler, maxAttempts);
    }

    public JedisClusterCommand() {
      super(connectionHandler, maxAttempts);
    }
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

  public Pipeline<J> beginPipelining(int hashSlot) {
    return this.connectionHandler.getConnectionFromSlot(hashSlot).beginPipelining();
  }

  public Pipeline<J> beginPipelining(byte[] sampleKey) {
    return beginPipelining(JedisClusterCRC16.getSlot(sampleKey));
  }

  public Pipeline<J> beginPipelining(String sampleKey) {
    return beginPipelining(JedisClusterCRC16.getSlot(sampleKey));
  }

  public Transaction<J> beginTransaction(int hashSlot) {
    return this.connectionHandler.getConnectionFromSlot(hashSlot).beginTransaction();
  }

  public Transaction<J> beginTransaction(byte[] sampleKey) {
    return beginTransaction(JedisClusterCRC16.getSlot(sampleKey));
  }

  public Transaction<J> beginTransaction(String sampleKey) {
    return beginTransaction(JedisClusterCRC16.getSlot(sampleKey));
  }

  public Object sendCommand(final byte[] sampleKey, final ProtocolCommand cmd, final byte[]... args) {
    return new JedisClusterCommand<Object>(connectionHandler, maxAttempts) {
      @Override
      public Object execute(J connection) {
        return connection.sendCommand(cmd, args);
      }
    }.runBinary(sampleKey);
  }

  public Object sendCommand(final String sampleKey, final ProtocolCommand cmd, final String... args) {
    return new JedisClusterCommand<Object>(connectionHandler, maxAttempts) {
      @Override
      public Object execute(J connection) {
        return connection.sendCommand(cmd, args);
      }
    }.run(sampleKey);
  }

  public Object sendBlockingCommand(final byte[] sampleKey, final ProtocolCommand cmd, final byte[]... args) {
    return new JedisClusterCommand<Object>(connectionHandler, maxAttempts) {
      @Override
      public Object execute(J connection) {
        return connection.sendBlockingCommand(cmd, args);
      }
    }.runBinary(sampleKey);
  }

  public Object sendBlockingCommand(final String sampleKey, final ProtocolCommand cmd, final String... args) {
    return new JedisClusterCommand<Object>(connectionHandler, maxAttempts) {
      @Override
      public Object execute(J connection) {
        return connection.sendBlockingCommand(cmd, args);
      }
    }.run(sampleKey);
  }
}
