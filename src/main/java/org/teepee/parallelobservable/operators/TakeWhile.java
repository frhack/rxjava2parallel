package org.teepee.parallelobservable.operators;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Predicate;
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
public class TakeWhile<T> extends ParallelObservable<T> {


    public TakeWhile(ParallelObservable<T> source, Predicate<? super T> predicate) {
        super(
                new Observable<T>() {
                    @Override
                    protected void subscribeActual(Observer<? super T> observer) {
                        source.getObservable().subscribe(new TakeWhile.TakeWhileObserver<T>(observer, predicate));
                    }
                });
    }


    static final class TakeWhileObserver<T> implements Observer<T>, Disposable {
        final Observer<? super T> actual;
        final Predicate<? super T> predicate;
        Disposable s;
        boolean done;

        TakeWhileObserver(Observer<? super T> actual, Predicate<? super T> predicate) {
            this.actual = actual;
            this.predicate = predicate;
        }

        public void onSubscribe(Disposable s) {
            if(DisposableHelper.validate(this.s, s)) {
                this.s = s;
                this.actual.onSubscribe(this);
            }

        }

        public void dispose() {
            this.s.dispose();
        }

        public boolean isDisposed() {
            return this.s.isDisposed();
        }

        public void onNext(T t) {
            if(!this.done) {
                boolean b;
                try {
                    b = this.predicate.test(t);
                } catch (Throwable var4) {
                    Exceptions.throwIfFatal(var4);
                    this.s.dispose();
                    this.onError(var4);
                    return;
                }

                if(!b) {
                    this.done = true;
                    this.s.dispose();
                    this.actual.onComplete();
                } else {
                    this.actual.onNext(t);
                }
            }
        }

        public void onError(Throwable t) {
            if(this.done) {
                RxJavaPlugins.onError(t);
            } else {
                this.done = true;
                this.actual.onError(t);
            }
        }

        public void onComplete() {
            if(!this.done) {
                this.done = true;
                this.actual.onComplete();
            }
        }
    }


}
