package redis.clients.jedis;

public interface ConnectionBrokenPattern {
  boolean determine(Connection connection, RuntimeException throwable);
}
