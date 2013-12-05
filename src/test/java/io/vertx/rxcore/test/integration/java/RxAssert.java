package io.vertx.rxcore.test.integration.java;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.rxcore.java.eventbus.RxMessage;
import rx.Observable;
import rx.Subscription;
import rx.plugins.RxJavaErrorHandler;
import rx.plugins.RxJavaPlugins;
import rx.util.functions.Action0;
import rx.util.functions.Action1;
import static org.vertx.testtools.VertxAssert.*;


/** Assertion utilities for Rx Observables
 * @author <a href="http://github.com/petermd">Peter McDonnell</a>
 **/
public class RxAssert {
  
  static {
    try {
      RxJavaPlugins.getInstance().registerErrorHandler(new RxJavaErrorHandler() {
        @Override 
        public void handleError(Throwable t) {
          System.err.println("RxJava-error: "+t);
        }
      });
      
      System.out.println("INFO Registered rxjava plugin error handler");
    }
    catch(Throwable t) {
      System.err.println("FATAL Unable to register rxjava plugin error handler (t="+t+")");
      t.printStackTrace(System.err);
    }
  }
  
  /** Assert a message */
  public static <T> void assertMessageThenComplete(Observable<RxMessage<T>> in, final T exp) {
    final AtomicInteger count=new AtomicInteger(1);
    in.subscribe(
      new Action1<RxMessage<T>>() {
        public void call(RxMessage<T> value) {
          assertEquals(exp,value.body());
          assertEquals(0,count.decrementAndGet());
          System.out.println("got:"+value.body());
        }
      },
      new Action1<Throwable>() {
        public void call(Throwable t) {
          fail("Error while mapping message (t="+t+")");
        }
      },
      new Action0() {
        public void call() {
          assertEquals(0,count.get());
          testComplete();
        }
      });
  }

  /** Assert a single value */
  public static <T> void assertSingle(Observable<T> in, final T value) {
    assertSequence(in,value);
  }

  /** Assert a sequence */
  public static <T> void assertSequence(Observable<T> in, final T... exp) {
    assertSequenceThen(in, new Action0() {
      @Override public void call() {
        // Do nothing
      }
    }, exp);
  }

  /** Assert that we receive N values */
  public static <T> Subscription assertCount(Observable<T> in, final int max) {
    return assertCountThen(in,new Action0() {
      @Override public void call() {
      }
    },max);
  }

  /** Assert that we receive N values then complete test */
  public static <T> Subscription assertCountThenComplete(Observable<T> in, final int max) {
    return assertCountThen(in,new Action0() {
      @Override public void call() {
        testComplete();
      }
    },max);
  }

  /** Assert that we receive N values then complete test */
  public static <T> Subscription assertCountThen(Observable<T> in, final Action0 thenAction, final int max) {
    final AtomicInteger count=new AtomicInteger(0);
    return in.subscribe(
      new Action1<T>() {
        public void call(T value) {
          assertTrue(count.incrementAndGet()<=max);
          System.out.println("sequence["+count+"]="+value);
        }
      },
      new Action1<Throwable>() {
        public void call(Throwable t) {
          fail("Error while counting sequence (t="+t+")");
        }
      },
      new Action0() {
        public void call() {
          assertTrue(count.get()==max);
          System.out.println("sequence-complete");
          thenAction.call();
        }
      });
  }

  /** Assert a single value then complete test */
  public static <T> void assertSingleThenComplete(Observable<T> in, final T value) {
    assertSequenceThenComplete(in,value);
  }

  /** Assert a sequence then complete test */
  public static <T> void assertSequenceThenComplete(Observable<T> in, final T... exp) {
    assertSequenceThen(in, new Action0() {
      @Override public void call() {
        testComplete();
      }
    }, exp);
  }
  
  /** Assert a sequence then call an Action0 when complete */
  public static <T> void assertSequenceThen(Observable<T> in, final Action0 thenAction, final T... exp) {
    final List<T> expList=new ArrayList(Arrays.asList(exp));
    in.subscribe(
      new Action1<T>() {
        public void call(T value) {
          assertEquals(expList.remove(0),value);
          System.out.println("sequence-next:"+value);
        }
      },
      new Action1<Throwable>() {
        public void call(Throwable t) {
          fail("Error while mapping sequence (t="+t+")");
        }
      },
      new Action0() {
        public void call() {
          assertTrue(expList.isEmpty());
          System.out.println("sequence-complete");
          thenAction.call();
        }
      });
  }

  /** Assert an expected error */
  public static <T> void assertError(Observable<T> in, final Class errClass) {
    assertError(in,errClass,null);
  }

  /** Assert an expected error */
  public static <T> void assertError(Observable<T> in, final Class errClass, final String errMsg) {
    in.subscribe(
      new Action1<T>() {
        public void call(T value) {
          System.out.println("error-next:"+value);
        }
      },
      new Action1<Throwable>() {
        public void call(Throwable t) {
          System.out.println("error-caught:"+t);
          assertEquals(errClass,t.getClass());
          if (errMsg!=null)
            assertEquals(errMsg,t.getMessage());  
          testComplete();
        }
      },
      new Action0() {
        public void call() {
          fail("unexpected-complete: failure expected");
        }
      });
  }
}
