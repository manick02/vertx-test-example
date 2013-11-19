package muraken720.vertx.mod.testexample.integration.java;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.assertNotNull;
import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.testComplete;

import java.util.concurrent.ConcurrentMap;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

public class BusModVerticleTest extends TestVerticle {

  @Test
  public void testAddAction() {
    container.logger().info("in testAddAction()");

    JsonObject request = new JsonObject().putString("action", "add")
        .putString("key", "name").putString("value", "@muraken720");

    container.logger().info("request message: " + request);

    vertx.eventBus().send("muraken720.vertx.mod.testexample", request,
        new Handler<Message<JsonObject>>() {
          @Override
          public void handle(Message<JsonObject> reply) {
            JsonObject json = reply.body();
            container.logger().info("response message: " + json);

            assertEquals("ok", json.getString("status"));

            ConcurrentMap<String, String> map = vertx.sharedData().getMap(
                "muraken720.testexample");

            assertEquals("@muraken720", map.get("name"));

            testComplete();
          }
        });
  }

  @Test
  public void testUnknownAction() {
    container.logger().info("in testUnknownAction()");

    JsonObject request = new JsonObject().putString("action", "unknown")
        .putString("key", "name").putString("value", "@muraken720");

    container.logger().info("request message: " + request);

    vertx.eventBus().send("muraken720.vertx.mod.testexample", request,
        new Handler<Message<JsonObject>>() {
          @Override
          public void handle(Message<JsonObject> reply) {
            JsonObject json = reply.body();
            container.logger().info("response message: " + json);

            assertEquals("error", json.getString("status"));
            assertEquals("unknown action.", json.getString("message"));

            testComplete();
          }
        });
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
        new AsyncResultHandler<String>() {
          @Override
          public void handle(AsyncResult<String> asyncResult) {
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
          }
        });
  }
}
