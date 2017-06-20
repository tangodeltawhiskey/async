package tangodeltawhiskey.async;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;
import static tangodeltawhiskey.async.AbstractEventServerTest.Callbacks.*;

public class AbstractEventServerTest {

    enum Callbacks {
        DO_START, AFTER_RUNNING, GET_EVENT, PROCESS_EVENT, DO_INTERRUPT,
        DO_STOP, AFTER_DONE, ON_FAILURE
    }

    private class VoidEventServer extends AbstractEventServer<Void> {

        @Override
        protected void doStart() throws Exception {
        }

        @Override
        protected void afterRunning() {
        }

        @Override
        protected Void getEvent() throws Exception {
            return null;
        }

        @Override
        protected void processEvent(Void event) throws Exception {
        }

        @Override
        protected void doInterrupt() {
        }

        @Override
        protected void doStop() throws Exception {
        }

        @Override
        protected void afterDone() {
        }

        @Override
        protected void onFailure(State state, Exception e) {
        }
    }

    private class VoidEventServerWithCallOrder extends VoidEventServer {

        protected final List<Callbacks> _callbackCallOrder;

        public VoidEventServerWithCallOrder(List<Callbacks> callbackCallOrder) {
            _callbackCallOrder = callbackCallOrder;
        }

        @Override
        protected void doStart() throws Exception {
            _callbackCallOrder.add(DO_START);
        }

        @Override
        protected void afterRunning() {
            _callbackCallOrder.add(AFTER_RUNNING);
        }

        @Override
        protected Void getEvent() throws Exception {
            _callbackCallOrder.add(GET_EVENT);
            return null;
        }

        @Override
        protected void processEvent(Void event) throws Exception {
            _callbackCallOrder.add(PROCESS_EVENT);
            stop();
        }

        @Override
        protected void doInterrupt() {
            _callbackCallOrder.add(DO_INTERRUPT);
        }

        @Override
        protected void doStop() throws Exception {
            _callbackCallOrder.add(DO_STOP);
        }

        @Override
        protected void afterDone() {
            _callbackCallOrder.add(AFTER_DONE);
        }

        @Override
        protected void onFailure(State state, Exception e) {
            _callbackCallOrder.add(ON_FAILURE);
        }

    }

    @Test
    public void callbacksAreCalledInCorrectOrder() throws Exception {

        /**
         * Scenario 1: Server starts normally and processes one message before
         * being stopped.
         *
         * Expected callback order is:
         *  1. doStart
         *  2. afterRunning
         *  3. getEvent
         *  4. processEvent
         *  5. doInterrupt
         *  6. getEvent
         *  7. doStop
         *  8. afterDone
         */
        List<Callbacks> callbacks1 = new ArrayList<>();

        AbstractEventServer server1
                = new VoidEventServerWithCallOrder(callbacks1) {

            @Override
            protected void runAsync() {
                this.run();
            }
        };

        server1.start();

        List<Callbacks> expected1 =
                Arrays.asList(DO_START, AFTER_RUNNING, GET_EVENT, PROCESS_EVENT,
                        DO_INTERRUPT, GET_EVENT, DO_STOP, AFTER_DONE);

        assertEquals(expected1, callbacks1);

        /**
         * Scenario 2: Server throws exception during start.
         *
         * Expected callback order is:
         *  1. doStart
         *  2. onFailure
         *  3. doStop
         *  4. afterDone
         */
        List<Callbacks> callbacks2 = new ArrayList<>();

        AbstractEventServer server2
                = new VoidEventServerWithCallOrder(callbacks2) {

            @Override
            protected void doStart() throws Exception {
                _callbackCallOrder.add(DO_START);
                throw new Exception();
            }

            @Override
            protected void runAsync() {
                this.run();
            }
        };

        server2.start();

        List<Callbacks> expected2 =
                Arrays.asList(DO_START, ON_FAILURE, DO_STOP, AFTER_DONE);

        assertEquals(expected2, callbacks2);

        /**
         * Scenario 3: Server throws exception during first getEvent.
         *
         * Expected callback order is:
         *  1. doStart
         *  2. afterRunning
         *  3. getEvent
         *  4. onFailure
         *  5. doStop
         *  6. afterDone
         */
        List<Callbacks> callbacks3 = new ArrayList<>();

        AbstractEventServer server3
                = new VoidEventServerWithCallOrder(callbacks3) {

            @Override
            protected Void getEvent() throws Exception {
                _callbackCallOrder.add(GET_EVENT);
                throw new Exception();
            }

            @Override
            protected void runAsync() {
                this.run();
            }
        };

        server3.start();

        List<Callbacks> expected3 =
                Arrays.asList(DO_START, AFTER_RUNNING, GET_EVENT, ON_FAILURE, DO_STOP, AFTER_DONE);

        assertEquals(expected3, callbacks3);

        /**
         * Scenario 4: Server throws exception during first getProcess.
         *
         * Expected callback order is:
         *  1. doStart
         *  2. afterRunning
         *  3. getEvent
         *  4. onFailure
         *  5. doStop
         *  6. afterDone
         */
        List<Callbacks> callbacks4 = new ArrayList<>();

        AbstractEventServer server4
                = new VoidEventServerWithCallOrder(callbacks4) {

            @Override
            protected void processEvent(Void event) throws Exception {
                _callbackCallOrder.add(PROCESS_EVENT);
                throw new Exception();
            }

            @Override
            protected void runAsync() {
                this.run();
            }
        };

        server4.start();

        List<Callbacks> expected4 =
                Arrays.asList(DO_START, AFTER_RUNNING, GET_EVENT, PROCESS_EVENT,
                        ON_FAILURE, DO_STOP, AFTER_DONE);

        assertEquals(expected4, callbacks4);

        /**
         * Scenario 5: Server starts normally and processes one message before
         * being stopped, but an exception is thrown while stopping.
         *
         * Expected callback order is:
         *  1. doStart
         *  2. onFailure
         *  3. getEvent
         *  4. processEvent
         *  5. doInterrupt
         *  6. getEvent
         *  7. doStop
         *  8. onFailure
         *  9. afterDone
         */
        List<Callbacks> callbacks5 = new ArrayList<>();

        AbstractEventServer server5
                = new VoidEventServerWithCallOrder(callbacks5) {

            @Override
            protected void doStop() throws Exception {
                _callbackCallOrder.add(DO_STOP);
                throw new Exception();
            }

            @Override
            protected void runAsync() {
                this.run();
            }
        };

        server5.start();

        List<Callbacks> expected5 =
        Arrays.asList(DO_START, AFTER_RUNNING, GET_EVENT, PROCESS_EVENT,
                DO_INTERRUPT, GET_EVENT, DO_STOP, ON_FAILURE, AFTER_DONE);

        assertEquals(expected5, callbacks5);
    }

    @Test
    public void serverIsInCorrectState() throws Exception {

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch latch3 = new CountDownLatch(1);
        CountDownLatch latch4 = new CountDownLatch(1);
        CountDownLatch latch5 = new CountDownLatch(1);
        CountDownLatch latch6 = new CountDownLatch(1);
        CountDownLatch latch7 = new CountDownLatch(1);
        CountDownLatch latch8 = new CountDownLatch(1);

        AbstractEventServer server1 = new VoidEventServer() {

            @Override
            protected void doStart() throws Exception {
                latch1.countDown();
                latch2.await();
            }

            @Override
            protected void afterRunning() {
                latch3.countDown();
                try {
                    latch4.await();
                } catch (InterruptedException e) {}
            }

            @Override
            protected Void getEvent() throws Exception {
                latch5.await();
                return null;
            }

            @Override
            protected void doInterrupt() {
                latch5.countDown();
            }

            @Override
            protected void doStop() throws Exception {
                latch6.countDown();
                latch7.wait();
            }

            @Override
            protected void afterDone() {
                latch8.countDown();
            }

        };

        assertTrue(server1.isStopped());

        server1.start();

        latch1.await();
        assertTrue(server1.isStarting());
        latch2.countDown();

        latch3.await();
        assertTrue(server1.isRunning());
        latch4.countDown();

        server1.stop();

        latch6.await();
        assertTrue(server1.isStopping());
        latch7.countDown();

        latch8.await();
        assertTrue(server1.isDone());
    }
}