package redis.clients.jedis.example;

import redis.clients.jedis.JedisBase;
import redis.clients.jedis.BuilderFactory;

public class CustomJedisExample {

  public static class CustomJedis extends JedisBase {

    public CustomJedis() {
    }

    public long sum(int x, int y) {
      checkIsInMultiOrPipeline();
      client.eval("return tonumber(ARGV[1]) + tonumber(ARGV[2])", 0, String.valueOf(x), String.valueOf(y));
      return BuilderFactory.LONG.build(client.getOne());
    }
  }

  public static void main(String[] args) {
    try (CustomJedis jedis = new CustomJedis()) {
      System.out.println(jedis.sum(4, 7));
    }
  }
}
