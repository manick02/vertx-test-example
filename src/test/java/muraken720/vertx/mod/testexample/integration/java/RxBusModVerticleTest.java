package muraken720.vertx.mod.testexample.integration.java;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.assertNotNull;
import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.testComplete;
import io.vertx.rxcore.java.eventbus.RxEventBus;
import io.vertx.rxcore.java.eventbus.RxMessage;

import java.util.concurrent.ConcurrentMap;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import rx.Observable;
import rx.util.functions.Action1;
import rx.util.functions.Func1;

public class RxBusModVerticleTest extends TestVerticle {

  @Test
  public void testAddAction() {
    container.logger().info("in testAddAction()");

    JsonObject request = new JsonObject().putString("action", "add")
        .putString("key", "name").putString("value", "@muraken720");

    container.logger().info("request message: " + request);

    RxEventBus rxEventBus = new RxEventBus(vertx.eventBus());

    Observable<RxMessage<JsonObject>> obs = rxEventBus.send(
        "muraken720.vertx.mod.testexample", request);

    obs.subscribe(new Action1<RxMessage<JsonObject>>() {
      @Override
      public void call(RxMessage<JsonObject> reply) {
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

    RxEventBus rxEventBus = new RxEventBus(vertx.eventBus());

    Observable<RxMessage<JsonObject>> obs = rxEventBus.send(
        "muraken720.vertx.mod.testexample", request);

    obs.subscribe(new Action1<RxMessage<JsonObject>>() {
      @Override
      public void call(RxMessage<JsonObject> reply) {
        JsonObject json = reply.body();
        container.logger().info("response message: " + json);

        assertEquals("error", json.getString("status"));
        assertEquals("unknown action.", json.getString("message"));

        testComplete();
      }
    });
  }

  @Test
  public void testSerialAction() {
    container.logger().info("in testSerialAction()");

    final RxEventBus rxEventBus = new RxEventBus(vertx.eventBus());
    final ConcurrentMap<String, String> map = vertx.sharedData().getMap(
        "muraken720.testexample");

    Observable<RxMessage<JsonObject>> obs1 = rxEventBus.send(
        "muraken720.vertx.mod.testexample",
        new JsonObject().putString("action", "add").putString("key", "name")
            .putString("value", "@muraken720"));

    Observable<RxMessage<JsonObject>> obs2 = obs1
        .flatMap(new Func1<RxMessage<JsonObject>, Observable<RxMessage<JsonObject>>>() {
          @Override
          public Observable<RxMessage<JsonObject>> call(
              RxMessage<JsonObject> reply) {
            assertEquals("ok", reply.body().getString("status"));
            assertEquals("@muraken720", map.get("name"));

            return rxEventBus.send(
                "muraken720.vertx.mod.testexample",
                new JsonObject().putString("action", "add")
                    .putString("key", "name").putString("value", "Kenichiro"));
          }
        });

    Observable<RxMessage<JsonObject>> obs3 = obs2
        .flatMap(new Func1<RxMessage<JsonObject>, Observable<RxMessage<JsonObject>>>() {
          @Override
          public Observable<RxMessage<JsonObject>> call(
              RxMessage<JsonObject> reply) {
            assertEquals("ok", reply.body().getString("status"));
            assertEquals("Kenichiro", map.get("name"));

            return rxEventBus.send(
                "muraken720.vertx.mod.testexample",
                new JsonObject().putString("action", "add")
                    .putString("key", "name").putString("value", "Murata"));
          }
        });

    obs3.subscribe(new Action1<RxMessage<JsonObject>>() {
      @Override
      public void call(RxMessage<JsonObject> reply) {
        assertEquals("ok", reply.body().getString("status"));
        assertEquals("Murata", map.get("name"));

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
