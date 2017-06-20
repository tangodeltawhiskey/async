package tangodeltawhiskey.async;

import java.util.concurrent.Executors;

public abstract class AbstractEventServer<T> implements Runnable {

    public enum State {
        RUNNING, STOPPED, STARTING, STOPPING, DONE
    }

    private volatile State _state = State.STOPPED;

    @Override
    public void run() {

        try {
            doStart();
            setState(State.RUNNING);
            afterRunning();
        } catch (Exception e) {
            onFailure(_state, e);
        }

        if (isRunning()) {

            while (true) {

                T event;
                try {
                    event = getEvent();
                } catch (Exception e) {
                    onFailure(_state, e);
                    break;
                }

                if (isRunning()) {
                    try {
                        processEvent(event);
                    } catch (Exception e) {
                        onFailure(_state, e);
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        setState(State.STOPPING);

        try {
            doStop();
        } catch (Exception e) {
            onFailure(_state, e);
        }

        setState(State.DONE);
        afterDone();
    }

    protected void runAsync() {
        Executors.newSingleThreadExecutor().submit(this);
    }

    public boolean isRunning() {
        return _state.equals(State.RUNNING);
    }

    public boolean isDone() {
        return _state.equals(State.DONE);
    }

    public boolean isStarting() {
        return _state.equals(State.STARTING);
    }

    public boolean isStopping() {
        return _state.equals(State.STOPPING);
    }

    public boolean isStopped() {
        return _state.equals(State.STOPPED);
    }

    public synchronized void start() {
        if (_state.equals(State.STOPPED)) {
            _state = State.STARTING;
            runAsync();
        }
    }

    public synchronized void stop() {
        if (_state.equals(State.RUNNING)) {
            _state = State.STOPPING;
            doInterrupt();
        }
    }

    private synchronized void setState(State state) {
        _state = state;
    }

    protected abstract T getEvent() throws Exception;

    protected abstract void processEvent(T event) throws Exception;

    protected abstract void doStart() throws Exception;

    protected abstract void doStop() throws Exception;

    protected abstract void doInterrupt();

    protected abstract void afterRunning();

    protected abstract void afterDone();

    protected abstract void onFailure(State state, Exception e);
 }
