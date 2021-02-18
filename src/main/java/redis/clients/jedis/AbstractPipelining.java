package redis.clients.jedis;

import java.io.Closeable;
import java.util.function.Consumer;

public abstract class AbstractPipelining<J extends JedisBase> extends Queable implements Closeable {

  private final J resource;

  @Deprecated
  private Client client = null;

  public AbstractPipelining(J resource) {
    this.resource = resource;
  }

  protected final <T> Response<T> addBatch(Consumer<Client> operator, Builder<T> builder) {
    operator.accept(getClient());
    return enqueResponse(builder);
  }

  protected final Client getClient() {
    if (resource == null) {
      return client;
    }
    return resource.getClient();
  }

  protected Client getClient(String key) {
    return getClient();
  }

  protected Client getClient(byte[] key) {
    return getClient();
  }

  /**
   * @deprecated This will be removed in future.
   * @param client
   */
  @Deprecated
  public void setClient(Client client) {
    if (this.resource == null) {
      this.client = client;
    }
  }

  protected abstract void clear();

  @Override
  public void close() {
    clear();
    if (this.resource != null) {
      this.resource.unsetDataSource();
    }
  }
}
