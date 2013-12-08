package muraken720.vertx.mod.testexample.integration.java;

import org.junit.Test;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import java.util.concurrent.ConcurrentMap;

import static org.vertx.testtools.VertxAssert.*;

public class J8BusModVerticleTest extends TestVerticle {

  @Test
  public void testAddAction() {
    final EventBus eventBus = vertx.eventBus();
    final ConcurrentMap<String, String> map = vertx.sharedData().getMap("muraken720.testexample");

    JsonObject request = createAddRequest("@muraken720");

    eventBus.send("muraken720.vertx.mod.testexample", request,
        (Handler<Message<JsonObject>>) reply -> {
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

    vertx.eventBus().send("muraken720.vertx.mod.testexample", request,
        (Handler<Message<JsonObject>>) reply -> {
          JsonObject json = reply.body();
          container.logger().info("response message: " + json);

          assertEquals("error", json.getString("status"));
          assertEquals("unknown action.", json.getString("message"));

          testComplete();
        });
  }

  @Test
  public void testSerialAction() {
    final EventBus eventBus = vertx.eventBus();
    final ConcurrentMap<String, String> map = vertx.sharedData().getMap("muraken720.testexample");

    JsonObject req1 = createAddRequest("@muraken720");

    // リクエスト1
    eventBus.send("muraken720.vertx.mod.testexample", req1,
        (Handler<Message<JsonObject>>) reply -> {
          assertEquals("ok", reply.body().getString("status"));
          assertEquals("@muraken720", map.get("name"));

          JsonObject req2 = createAddRequest("Kenichiro");

          // リクエスト2
          eventBus.send("muraken720.vertx.mod.testexample", req2,
              (Handler<Message<JsonObject>>) reply1 -> {
                assertEquals("ok", reply.body().getString("status"));
                assertEquals("Kenichiro", map.get("name"));

                JsonObject req3 = createAddRequest("Murata");

                // リクエスト3
                eventBus.send("muraken720.vertx.mod.testexample", req3,
                    (Handler<Message<JsonObject>>) reply2 -> {
                      assertEquals("ok", reply.body().getString("status"));
                      assertEquals("Murata", map.get("name"));

                      testComplete();
                    });
              });
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
