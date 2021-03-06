package org.teepee.parallelobservable;

import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import javafx.util.Pair;
import org.teepee.parallelobservable.operators.Take;

import java.util.ArrayList;
import java.util.List;
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
    private boolean serialized = false;
    BlockingQueue<Runnable> tqueue;

    public ParallelObservable(Observable<T> observable) {
        this.observable = observable;
        this.threadsPoolSize = Runtime.getRuntime().availableProcessors() + 1;
    }


    public Observable<T> observable() {
        return observable;
    }


    public ParallelObservable<T> withBuffer(Integer bufferSize) {
        ParallelObservable<T> p = getNew(observable);
        p.bufferSize = bufferSize;
        return p;
    }

    public ParallelObservable<T> unparallel() {
        return withThreads(0);
    }


    public ParallelObservable<T> withThreads(Integer threadsPoolSize) {
        ParallelObservable<T> p = getNew(observable);
        p.threadsPoolSize = threadsPoolSize;
        return p;
    }


    public static <T> ParallelObservable<T> fromIterable(Iterable<? extends T> source) {
        return new ParallelObservable<T>(Observable.fromIterable(source));
    }

    public static <T> ParallelObservable<T> fromObservable(Observable<T> source) {
        return new ParallelObservable<>(source);
    }


    public static ParallelObservable<Integer> range(int start, int count) {
        return new ParallelObservable<>(Observable.range(start, count));
    }

    public ParallelObservable<T> doOnNext(Consumer<? super T> fun) {
        if (threadsPoolSize == 0) return getNewBufferedIfNeeded(observable.doOnNext(fun));
        ParallelObservable<T> p;
        if (bufferSize == null) {
            p = getDoOnNextObservable(fun);
        } else {
            p = getDoOnNextObservableBuffered(fun);
        }
        return p;
    }

    public ParallelObservable<T> take(long n) {
        return new Take<>(this, n);
    }

    public ParallelObservable<T> takeWhile(Predicate<? super T> predicate) {
        return this.map((T t) -> {
            Pair<T, Boolean> pair = new Pair(t, predicate.test(t));
            return pair;
        }).unparallelTtakeWhile((Pair<T, Boolean> pair) -> pair.getValue()).map((Pair<T, Boolean> pair) -> pair.getKey());
        //return  new ParallelObservable<>(new ObservableTakeWhile<T>(observable(),predicate));
    }


    public ParallelObservable<T> unparallelTtakeWhile(Predicate<? super T> fun) {
        return getNew(observable.takeWhile(fun));
    }


    public ParallelObservable<T> takeUntil(Predicate<? super T> predicate) {
        return this.map((T t) -> {
            Pair<T, Boolean> pair = new Pair(t, predicate.test(t));
            return pair;
        }).unparallelTtakeUntil((Pair<T, Boolean> pair) -> pair.getValue()).map((Pair<T, Boolean> pair) -> pair.getKey());
    }

    public ParallelObservable<T> unparallelTtakeUntil(Predicate<? super T> fun) {
        return getNew(observable.takeUntil(fun));
    }


    public Observable<T> serialObservable() {
        if (serialized) return observable;
        return observable.serialize();
        //return new ToObservable<T>(this).observable();
    }


    public ParallelObservable<T> serialize() {
        if (serialized) return this;
        ParallelObservable<T> po = getNew(observable.serialize());
        po.serialized = true;
        return po;
    }


    public ParallelObservable<T> filter(Predicate<? super T> fun) {
        ParallelObservable<T> o;
        if (threadsPoolSize == 0) return getNewBufferedIfNeeded(observable.filter(fun));
        if (bufferSize == null) {
            o = getFilterParallelObservable(fun);
        } else {
            o = getFilterParallelObservableBuffered(fun);
        }
        return o;
    }


    public final <R> ParallelObservable<R> map(Function<? super T, ? extends R> fun) {
        ParallelObservable<R> o;
        if (threadsPoolSize == 0) {
            return getNewBufferedIfNeeded(observable.map(fun));
        }
        if (bufferSize == null) {
            o = getMapParallelObservable(fun);
        } else {
            o = getMapParallelObservableBuffered(fun);
        }
        return o;
    }


    private <R> ParallelObservable<R> getMapParallelObservable(Function<? super T, ? extends R> fun) {
        Observable<R> o = Observable.create(e -> {

            initExecutorService();
            observable.forEachWhile((T t) -> {
                        submitMap(t, fun, e);
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
        });
        return new ParallelObservable<R>(o);
    }


    private ParallelObservable<T> getDoOnNextObservableNEW(Consumer<? super T> fun) {
        Observable<T> o = Observable.create(e -> {
                    initExecutorService();
                    System.out.println("XXX LLL" + e.isDisposed());
                    observable.doOnNext((T t) -> {
                        System.out.println("XXX AAAA LLL" + e.isDisposed());
                        submit(t, fun, e);
                        ;
                    });
                    observable.doOnComplete(() -> {
                        System.out.println("XXX COMPLETE");
                        e.onComplete();
                        executorService.shutdown();
                        try {
                            executorService.awaitTermination(3600, TimeUnit.SECONDS);
                        } catch (Exception ee) {
                            e.onError(ee);
                        }
                    });
                    //e.onComplete();
                }


        );
//        o.doOnComplete(()->e.onComplete());
        return getNew(o);
    }


    private ParallelObservable<T> getDoOnNextObservable(Consumer<? super T> fun) {
        Observable<T> o = Observable.create(e -> {

            initExecutorService();
            observable.forEachWhile((T t) -> {
                        submit(t, fun, e);
                        return !e.isDisposed();
                    }
            );
            observable.doOnComplete(() -> {
                executorService.shutdown();
                try {
                    executorService.awaitTermination(3600, TimeUnit.SECONDS);
                } catch (Exception ee) {
                    e.onError(ee);
                }
                e.onComplete();
            });

        });
        return getNew(o);
    }


    private <R> ParallelObservable<R> getMapParallelObservableBuffered(Function<? super T, ? extends R> fun) {
        Observable<R> o = Observable.create(new ObservableOnSubscribe<R>() {
            int bufferIndex = 0;

            @Override
            public void subscribe(ObservableEmitter<R> e) {
                initExecutorService();
                final List<Future<R>> buffer = new ArrayList<Future<R>>(bufferSize);
                observable.forEachWhile((T t) -> {
                            submitMapBuffered(t, fun, e, buffer, bufferIndex);
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
        return new ParallelObservable<>(o);
    }


    private ParallelObservable<T> getDoOnNextObservableBuffered(Consumer<? super T> fun) {
        Observable<T> o = Observable.create(new ObservableOnSubscribe<T>() {
            int bufferIndex = 0;

            @Override
            public void subscribe(ObservableEmitter<T> e) {

                initExecutorService();
                final List<T> buffer = new ArrayList<T>(bufferSize);
                observable.forEachWhile((T t) -> {
                            submitBuffered(t, fun, e, buffer, bufferIndex);
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





                observable.doOnComplete(() -> {
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
                    e.onComplete();
                });


            }
        });
        return getNew(o);
    }


    public void initExecutorService() {
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


    private void submit(T element, Consumer<? super T> fun, ObservableEmitter<T> oe) {
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


    private void submitFilter(T element, Predicate<? super T> fun, ObservableEmitter<T> oe) {
        submitWaitIfNeeded();
        executorService.submit(() -> {
            try {
                if (fun.test(element)) {
                    oe.onNext(element);
                }
            } catch (Exception e) {
                oe.onError(e);
            }
        });
    }


    private void submitWaitIfNeeded() {
        while (tqueue.size() > 900) {
            try {
                sleep(0, 100);
            } catch (Exception e) {
            }
        }
    }

    private <R> void submitMap(T element, Function<? super T, ? extends R> fun, ObservableEmitter<R> oe) throws Exception {
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

    private ParallelObservable<T> getFilterParallelObservable(Predicate<? super T> fun) {
        Observable<T> o = Observable.create(e -> {
            initExecutorService();
            observable.forEachWhile((T t) -> {
                        submitFilter(t, fun, e);
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
        });
        return new ParallelObservable<>(o);
    }


    private ParallelObservable<T> getFilterParallelObservableBuffered(Predicate<? super T> fun) {
        Observable<T> o = Observable.create(new ObservableOnSubscribe<T>() {
            int bufferIndex = 0;


            @Override
            public void subscribe(ObservableEmitter<T> e) {
                initExecutorService();
                final List<Future<Pair<T, Boolean>>> buffer = new ArrayList<>(bufferSize);
                observable.forEachWhile((T t) -> {
                            submitFilterBuffered(t, fun, e, buffer, bufferIndex);
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
        return new ParallelObservable<>(o);
    }


    public static <T> Predicate<? super T> not(Predicate<? super T> predicate) {
        return (T t) -> !predicate.test(t);
    }

    private void submitFilterBuffered(T element, Predicate<? super T> fun, ObservableEmitter<T> oe, List<Future<Pair<T, Boolean>>> buffer, int bufferIndex) throws Exception {
        submitWaitIfNeeded();
        Future<Pair<T, Boolean>> future = executorService.submit(() -> {
            Boolean r = false;
            try {
                r = fun.test(element);
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

    private <S> ParallelObservable<S> getNew(Observable<S> observable) {
        ParallelObservable<S> parallelObservable = new ParallelObservable<S>(observable);
        return parallelObservable;
    }

    private <S> ParallelObservable<S> getNewBufferedIfNeeded(Observable<S> observable) {
        if (bufferSize != null) {
            return getNew(observable.buffer(bufferSize).flatMap(l -> Observable.fromIterable(l)));
        } else {
            return getNew(observable);
        }
    }


    public boolean isSerialized() {
        return serialized;
    }

}