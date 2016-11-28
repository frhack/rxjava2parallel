package org.teepee.parallelobservable.operators;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.DisposableHelper;
import io.reactivex.internal.disposables.EmptyDisposable;
import io.reactivex.plugins.RxJavaPlugins;
import org.teepee.parallelobservable.ParallelObservable;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by francesco on 20/11/2016.
 */
public class Take<T> extends ParallelObservable<T> {


    public Take(ParallelObservable<T> source, long n) {
        super(
                new Observable<T>() {
                    @Override
                    protected void subscribeActual(Observer<? super T> observer) {
                        source.getObservable().subscribe(new Take.TakeObserver(observer, n));
                    }
                });
    }


    static final class TakeObserver<T> implements Observer<T>, Disposable {
        final Observer<? super T> actual;
        boolean done;

        Disposable subscription;
        final AtomicLong remaining;
        final AtomicInteger counter = new AtomicInteger();
        final Queue<T> queue = new ConcurrentLinkedQueue<>();


        TakeObserver(Observer<? super T> actual, long limit) {
            remaining = new AtomicLong(limit);
            this.actual = actual;
        }

        public void onSubscribe(Disposable s) {
            if (DisposableHelper.validate(this.subscription, s)) {
                this.subscription = s;
                if (this.remaining.get() == 0L) {
                    this.done = true;
                    s.dispose();
                    EmptyDisposable.complete(this.actual);
                } else {
                    this.actual.onSubscribe(this);
                }
            }

        }

        private <T> void onNext(Observer<? super T> oe, T t, Queue<T> queue) {
            queue.offer(t);
            drain(oe, queue);
        }





        private <T> void drain(Observer<? super T>  oe, Queue<T> queue) {
            // R t;
            if (counter.getAndIncrement() == 0) {
                do {
                    //t =;


                    if (!this.done && remaining.getAndDecrement() > 0L) {
                        boolean stop = this.remaining.get() == 0;
                        //this.actual.onNext(t);
                        oe.onNext(queue.poll());
                        //this.onNext(actual,t,queue);
                        if (stop) {
                            this.onComplete();
                        }
                    }
                } while (counter.decrementAndGet() != 0);
            }
        }


        public void onNext(T t) {
            this.onNext(actual,t,queue);
        }

        public void onError(Throwable t) {
            if (this.done) {
                RxJavaPlugins.onError(t);
            } else {
                this.done = true;
                this.subscription.dispose();
                this.actual.onError(t);
            }
        }

        public void onComplete() {
            if (!this.done) {
                this.done = true;
                this.subscription.dispose();
                this.actual.onComplete();
            }

        }

        public void dispose() {
            this.subscription.dispose();
        }

        public boolean isDisposed() {
            return this.subscription.isDisposed();
        }
    }


}
