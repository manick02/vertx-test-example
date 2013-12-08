package muraken720.vertx.mod.testexample.integration.java;

import io.vertx.rxcore.java.eventbus.RxEventBus;
import io.vertx.rxcore.java.eventbus.RxMessage;
import org.junit.Test;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import rx.Observable;

import java.util.concurrent.ConcurrentMap;

import static org.vertx.testtools.VertxAssert.*;

public class RxJ8BusModVerticleTest extends TestVerticle {

  @Test
  public void testAddAction() {
    final RxEventBus rxEventBus = new RxEventBus(vertx.eventBus());
    final ConcurrentMap<String, String> map = vertx.sharedData().getMap("muraken720.testexample");

    JsonObject request = createAddRequest("@muraken720");

    Observable<RxMessage<JsonObject>> obs = rxEventBus.send("muraken720.vertx.mod.testexample", request);

    obs.subscribe(reply -> {
      JsonObject json = reply.body();

      assertEquals("ok", json.getString("status"));

      assertEquals("@muraken720", map.get("name"));

      testComplete();
    });
  }

  @Test
  public void testUnknownAction() {
    JsonObject request = new JsonObject()
        .putString("action", "unknown")
        .putString("key", "name")
        .putString("value", "@muraken720");

    container.logger().info("request message: " + request);

    RxEventBus rxEventBus = new RxEventBus(vertx.eventBus());

    Observable<RxMessage<JsonObject>> obs = rxEventBus.send("muraken720.vertx.mod.testexample", request);

    obs.subscribe(reply -> {
      JsonObject json = reply.body();
      container.logger().info("response message: " + json);

      assertEquals("error", json.getString("status"));
      assertEquals("unknown action.", json.getString("message"));

      testComplete();
    });
  }

  @Test
  public void testSerialAction() {
    final RxEventBus rxEventBus = new RxEventBus(vertx.eventBus());
    final ConcurrentMap<String, String> map = vertx.sharedData().getMap("muraken720.testexample");

    JsonObject req1 = createAddRequest("@muraken720");

    // リクエスト1
    Observable<RxMessage<JsonObject>> obs1 = rxEventBus.send("muraken720.vertx.mod.testexample", req1);

    Observable<RxMessage<JsonObject>> obs2 = obs1.flatMap(reply -> {
      assertEquals("ok", reply.body().getString("status"));
      assertEquals("@muraken720", map.get("name"));

      JsonObject req2 = createAddRequest("Kenichiro");

      return rxEventBus.send("muraken720.vertx.mod.testexample", req2);
    });

    // リクエスト2
    Observable<RxMessage<JsonObject>> obs3 = obs2.flatMap(reply -> {
      assertEquals("ok", reply.body().getString("status"));
      assertEquals("Kenichiro", map.get("name"));

      JsonObject req3 = createAddRequest("Murata");

      return rxEventBus.send("muraken720.vertx.mod.testexample", req3);
    });

    // リクエスト3
    obs3.subscribe(reply -> {
      assertEquals("ok", reply.body().getString("status"));
      assertEquals("Murata", map.get("name"));

      testComplete();
    });
  }

  private JsonObject createAddRequest(String value) {
    return new JsonObject()
        .putString("action", "add")
        .putString("key", "name")
        .putString("value", value);
  }

  @Override
  public void start() {
    // Make sure we call initialize() - this sets up the assert stuff so assert
    // functionality works correctly
    initialize();
    // Deploy the module - the System property `vertx.modulename` will contain
    // the name of the module so you
    // don't have to hardecode it in your tests
    container.deployModule(System.getProperty("vertx.modulename"),
        asyncResult -> {
          // Deployment is asynchronous and this this handler will be called
          // when it's complete (or failed)
          if (asyncResult.failed()) {
            container.logger().error(asyncResult.cause());
          }
          assertTrue(asyncResult.succeeded());
          assertNotNull("deploymentID should not be null",
              asyncResult.result());
          // If deployed correctly then start the tests!
          startTests();
        });
  }
}
