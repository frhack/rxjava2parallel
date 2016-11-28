package org.teepee.parallelobservable;

import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import javafx.util.Pair;
import org.teepee.parallelobservable.operators.Take;
import org.teepee.parallelobservable.operators.TakeWhile;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

import static java.lang.Thread.sleep;

/**
 * Created by francesco on 13/11/2016.
 */
public class ParallelObservable<T> {
    private ExecutorService executorService;
    private Observable<T> observable;
    private Integer bufferSize;
    private Integer threadsPoolSize;
    BlockingQueue<Runnable> tqueue;

    public ParallelObservable(Observable<T> observable) {
        this.observable = observable;
    }


    public Observable<T> getObservable() {
        return observable;
    }

    public Integer getBufferSize() {
        return bufferSize;
    }

    public ParallelObservable<T> withBufferSize(Integer bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    public Integer getThreadsPoolSize() {
        return threadsPoolSize;
    }

    public ParallelObservable<T> withThreadsPoolSize(Integer threadsPoolSize) {
        this.threadsPoolSize = threadsPoolSize;
        return this;
    }

    public static <T> ParallelObservable<T> fromIterable(Iterable<? extends T> source) {
        return new ParallelObservable<T>(Observable.fromIterable(source));
    }

    public static <T> ParallelObservable<T> fromObservable(Observable<T> source) {
        return new ParallelObservable<T>(source);
    }


    public static ParallelObservable<Integer> range(int start, int count) {
        return new ParallelObservable<>(Observable.range(start, count));
    }

    public ParallelObservable<T> doOnNext(Consumer<? super T> fun) {
        ParallelObservable<T> p = null;

        if (bufferSize == null) {
            p = getPipeObservable(fun);
        } else {
            p = getPipeObservableBuffered(fun);
        }
        return p;
    }

    public ParallelObservable<T> take(long n) {
        return new Take(this, n);
    }

    public ParallelObservable<T> takeWhile(Predicate<? super T> predicate) {
        return new TakeWhile<T>(this, predicate);
    }


    public Observable<T> toObservable() {
        return getObservable().serialize();
        //return new ToObservable<T>(this).getObservable();
    }


    public ParallelObservable<T> filter(Function<? super T, Boolean> fun) {
        ParallelObservable<T> o;

        if (bufferSize == null) {
            o = getFilterParallelPipe(fun);
        } else {
            o = getFilterParallelPipeBuffered(fun);
        }
        return o;
    }

    public final <R> ParallelObservable<R> map(Function<? super T, ? extends R> fun) {
        ParallelObservable<R> o;

        if (bufferSize == null) {
            o = getPipeMapObservable(fun);
        } else {
            o = getPipeMapObservableBuffered(fun);
        }
        return o;
    }


    private final <R> ParallelObservable<R>


    getPipeMapObservable(Function<? super T, ? extends R> fun) {
        ParallelObservable<T> p = this;

        Observable<R> o = Observable.create(new ObservableOnSubscribe<R>() {
            final Queue<R> queue = new ConcurrentLinkedQueue<>();


            @Override
            public void subscribe(ObservableEmitter<R> e) {

                initExecutorService();
                observable.forEachWhile((T t) -> {
                            p.submitMap(t, fun, e, queue);
                            return !e.isDisposed();
                        }
                );
                executorService.shutdown();
                try {
                    executorService.awaitTermination(3600, TimeUnit.SECONDS);
                } catch (Exception ee) {
                    e.onError(ee);
                }
                e.onComplete();
            }
        });
        return new ParallelObservable<R>(o);
    }


    private ParallelObservable<T> getPipeObservable(Consumer<? super T> fun) {
        ParallelObservable<T> p = this;
        final Queue<T> queue = new ConcurrentLinkedQueue<>();

        Observable<T> o = Observable.create(new ObservableOnSubscribe<T>() {


            @Override
            public void subscribe(ObservableEmitter<T> e) {

                initExecutorService();
                observable.forEachWhile((T t) -> {
                            p.submit(t, fun, e, queue);
                            return !e.isDisposed();
                        }
                );
                executorService.shutdown();
                try {
                    executorService.awaitTermination(3600, TimeUnit.SECONDS);
                } catch (Exception ee) {
                    e.onError(ee);
                }
                e.onComplete();
            }
        });
        return new ParallelObservable<T>(o);
    }

    private Observable<T> getFilterObservable(Function<? super T, Boolean> fun) {
        ParallelObservable<T> parallelObservable = this;

        return Observable.create(new ObservableOnSubscribe<T>() {


            @Override
            public void subscribe(ObservableEmitter<T> e) {
                initExecutorService();
                observable.forEachWhile((T t) -> {
                            parallelObservable.submitFilter(t, fun, e);
                            return !e.isDisposed();
                        }
                );
                executorService.shutdown();
                try {
                    executorService.awaitTermination(3600, TimeUnit.SECONDS);
                } catch (Exception ee) {
                    e.onError(ee);
                }
                e.onComplete();
            }
        });
    }


    private final <R> ParallelObservable<R> getPipeMapObservableBuffered(Function<? super T, ? extends R> fun) {
        ParallelObservable<T> p = this;
        Observable<R> o = Observable.create(new ObservableOnSubscribe<R>() {
            int bufferIndex = 0;


            @Override
            public void subscribe(ObservableEmitter<R> e) {
                initExecutorService();
                final List<Future<R>> buffer = new ArrayList<Future<R>>(bufferSize);
                observable.forEachWhile((T t) -> {
                            p.submitMapBuffered(t, fun, e, buffer, bufferIndex);
                            bufferIndex++;
                            if (bufferIndex == bufferSize) {
                                executorService.shutdown();
                                try {
                                    executorService.awaitTermination(3600, TimeUnit.SECONDS);
                                } catch (Exception ee) {
                                    e.onError(ee);
                                }
                                for (int i = 0; i < bufferIndex; i++) {
                                    e.onNext(buffer.get(i).get());
                                }
                                bufferIndex = 0;
                                initExecutorService();
                            }
                            return !e.isDisposed();
                        }
                );

                executorService.shutdown();
                try {
                    executorService.awaitTermination(3600, TimeUnit.SECONDS);
                } catch (Exception ee) {
                    e.onError(ee);
                }
                for (int i = 0; i < bufferIndex; i++) {
                    try {
                        e.onNext(buffer.get(i).get());
                    } catch (Exception ex) {
                        e.onError(ex);
                    }
                }
                e.onComplete();
            }
        });
        return new ParallelObservable<R>(o);
    }


    private ParallelObservable<T> getPipeObservableBuffered(Consumer<? super T> fun) {
        ParallelObservable<T> p = this;
        Observable<T> o = Observable.create(new ObservableOnSubscribe<T>() {
            int bufferIndex = 0;

            @Override
            public void subscribe(ObservableEmitter<T> e) {

                initExecutorService();
                final List<T> buffer = new ArrayList<T>(bufferSize);
                observable.forEachWhile((T t) -> {
                            p.submitBuffered(t, fun, e, buffer, bufferIndex);
                            bufferIndex++;
                            if (bufferIndex == bufferSize) {
                                executorService.shutdown();
                                try {
                                    executorService.awaitTermination(3600, TimeUnit.SECONDS);
                                } catch (Exception ee) {
                                    e.onError(ee);
                                }
                                for (int i = 0; i < bufferIndex; i++) {
                                    e.onNext(buffer.get(i));
                                }
                                bufferIndex = 0;
                                initExecutorService();
                            }
                            return !e.isDisposed();
                        }
                );

                executorService.shutdown();
                try {
                    executorService.awaitTermination(3600, TimeUnit.SECONDS);
                } catch (Exception ee) {
                    e.onError(ee);
                }
                for (int i = 0; i < bufferIndex; i++) {
                    e.onNext(buffer.get(i));
                }
                e.onComplete();
                bufferIndex = 0;
            }
        });
        return new ParallelObservable<T>(o);
    }


    private void initExecutorService() {
        tqueue = new ArrayBlockingQueue<>(1000);
        Integer size = threadsPoolSize;
        //FIXME TODO
        RejectedExecutionHandler rejectedExecutionHandler = (r, executor) -> {
            try {
                sleep(0, 1000);
                tqueue.put(r);
            } catch (Exception e) {
                System.err.println(e);
            }
        };

        if (size == null) {
            size = Runtime.getRuntime().availableProcessors() + 1;
        }
        //executorService = Executors.newFixedThreadPool(threadsPoolSize);
        //System.out.println(parallelObservable + " " + size);
        executorService = new ThreadPoolExecutor(size, size,
                0L, TimeUnit.MILLISECONDS,
                tqueue);

    }


    private void submit(T element, Consumer<? super T> fun, ObservableEmitter<T> oe, Queue<T> queue) {
        ParallelObservable<T> parallelObservable = this;
        submitWaitIfNeeded();
        executorService.submit(() -> {
            try {
                fun.accept(element);
                oe.onNext(element);
            } catch (Exception e) {
                oe.onError(e);
            }
        });
    }


    private void submitFilter(T element, Function<? super T, Boolean> fun, ObservableEmitter<T> oe) {
        submitWaitIfNeeded();
        executorService.submit(() -> {
            try {
                if (fun.apply(element)) {
                    oe.onNext(element);
                }
            } catch (Exception e) {
                oe.onError(e);
            }
        });
    }



    private void submitWaitIfNeeded() {
        while (tqueue.size() > 500) {
            try {
                sleep(0, 100);
            } catch (Exception e) {
            }
        }
    }

    private <R> void submitMap(T element, Function<? super T, ? extends R> fun, ObservableEmitter<R> oe, Queue<R> queue) throws Exception {

        ParallelObservable<T> parallelObservable = this;
        submitWaitIfNeeded();
        executorService.submit(() -> {
            try {
                R r = fun.apply(element);
                oe.onNext(r);
            } catch (Exception e) {
                oe.onError(e);
            }
        });
    }

    private <R> void submitMapBuffered(T element, Function<? super T, ? extends R> fun, ObservableEmitter<R> oe, List<Future<R>> buffer, int bufferIndex) throws Exception {
        Future<R> future;
        submitWaitIfNeeded();
        future = executorService.submit(() -> {
            R r = null;
            try {
                r = fun.apply(element);

            } catch (Exception e) {
                oe.onError(e);
            }
            return r;
        });
        if (buffer.size() <= bufferIndex) {
            buffer.add(future);
        } else {
            buffer.set(bufferIndex, future);
        }
    }


    private void submitBuffered(T element, Consumer<? super T> fun, ObservableEmitter<T> oe, List<T> buffer, int bufferIndex) throws Exception {
        submitWaitIfNeeded();
        executorService.submit(() -> {
            try {
                fun.accept(element);
            } catch (Exception e) {
                oe.onError(e);
            }
        });
        if (buffer.size() <= bufferIndex) {
            buffer.add(element);
        } else {
            buffer.set(bufferIndex, element);
        }
    }

    private ParallelObservable<T> getFilterParallelPipe(Function<? super T, Boolean> fun) {
        ParallelObservable<T> parallelObservable = this;

        Observable<T> o = Observable.create(new ObservableOnSubscribe<T>() {


            @Override
            public void subscribe(ObservableEmitter<T> e) {
                initExecutorService();
                observable.forEachWhile((T t) -> {
                            parallelObservable.submitFilter(t, fun, e);
                            return !e.isDisposed();
                        }
                );
                executorService.shutdown();
                try {
                    executorService.awaitTermination(3600, TimeUnit.SECONDS);
                } catch (Exception ee) {
                    e.onError(ee);
                }
                e.onComplete();
            }
        });
        return new ParallelObservable<T>(o);
    }


    private ParallelObservable<T> getFilterParallelPipeBuffered(Function<? super T, Boolean> fun) {
        ParallelObservable<T> p = this;
        Observable<T> o = Observable.create(new ObservableOnSubscribe<T>() {
            int bufferIndex = 0;


            @Override
            public void subscribe(ObservableEmitter<T> e) {
                initExecutorService();
                final List<Future<Pair<T, Boolean>>> buffer = new ArrayList<>(bufferSize);
                observable.forEachWhile((T t) -> {
                            p.submitFilterBuffered(t, fun, e, buffer, bufferIndex);
                            bufferIndex++;
                            if (bufferIndex == bufferSize) {
                                executorService.shutdown();
                                try {
                                    executorService.awaitTermination(3600, TimeUnit.SECONDS);
                                } catch (Exception ee) {
                                    e.onError(ee);
                                }
                                for (int i = 0; i < bufferIndex; i++) {
                                    if (buffer.get(i).get().getValue())
                                        e.onNext(buffer.get(i).get().getKey());
                                }
                                bufferIndex = 0;
                                initExecutorService();
                            }
                            return !e.isDisposed();
                        }
                );

                executorService.shutdown();
                try {
                    executorService.awaitTermination(3600, TimeUnit.SECONDS);
                } catch (Exception ee) {
                    e.onError(ee);
                }
                for (int i = 0; i < bufferIndex; i++) {
                    try {
                        if (buffer.get(i).get().getValue())
                            e.onNext(buffer.get(i).get().getKey());
                    } catch (Exception ex) {
                        e.onError(ex);
                    }
                }
                e.onComplete();
            }
        });
        return new ParallelObservable<T>(o);
    }


    private void submitFilterBuffered(T element, Function<? super T, Boolean> fun, ObservableEmitter<T> oe, List<Future<Pair<T, Boolean>>> buffer, int bufferIndex) throws Exception {
        submitWaitIfNeeded();
        Future<Pair<T, Boolean>> future = executorService.submit(() -> {
            Boolean r = false;
            try {
                r = fun.apply(element);
            } catch (Exception e) {
                oe.onError(e);
            }
            return new Pair<>(element, r);
        });

        if (buffer.size() <= bufferIndex) {
            buffer.add(future);
        } else {
            buffer.set(bufferIndex, future);
        }
    }


}