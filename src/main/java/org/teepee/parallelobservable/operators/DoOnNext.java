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
public class DoOnNext<T> extends ParallelObservable<T> {


    public DoOnNext(ParallelObservable<T> source, long n) {
        super(
                new Observable<T>() {
                    @Override
                    protected void subscribeActual(Observer<? super T> observer) {
                        source.observable().subscribe(new DoOnNext.DoOnNextObserver<>(observer));
                        source.initExecutorService();
                    }
                });
    }

    @Override
    public boolean isSerialized() {
        return true;
    }

    static final class DoOnNextObserver<T> implements Observer<T>, Disposable {
        final Observer<? super T> actual;
        boolean done;

        Disposable subscription;


        DoOnNextObserver(Observer<? super T> actual) {
            this.actual = actual;
        }

        public void onSubscribe(Disposable s) {
            if (DisposableHelper.validate(this.subscription, s)) {
                this.subscription = s;
                this.actual.onSubscribe(this);
            }

        }



        public void onNext(T t) {

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