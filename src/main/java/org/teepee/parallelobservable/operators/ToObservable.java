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

/**
 * Created by francesco on 20/11/2016.
 */
public class ToObservable<T> extends ParallelObservable<T> {


    public ToObservable(ParallelObservable<T> source) {
        super(
                new Observable<T>() {
                    @Override
                    protected void subscribeActual(Observer<? super T> observer) {
                        source.observable().subscribe(new ToObservable.TakeObserver(observer));
                    }
                });
    }


    static final class TakeObserver<T> implements Observer<T>, Disposable {
        final Observer<? super T> actual;
        boolean done;
        long count = 0;

        Disposable subscription;
        final AtomicInteger counter = new AtomicInteger();
        final Queue<T> queue = new ConcurrentLinkedQueue<>();


        TakeObserver(Observer<? super T> actual) {
            this.actual = actual;
        }

        public void onSubscribe(Disposable s) {
            if (DisposableHelper.validate(this.subscription, s)) {
                this.subscription = s;
                if (this.isDisposed()) {
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
// DONT TOUCH
                    //FIXME LOOK AT ObservableBlockingSubscribe
try {
    if(count++ %100000 == 0)
    System.out.println(".");
    oe.onNext(queue.poll());

}catch (Exception e){
    if(!this.isDisposed())
    System.out.println("ERROR "+this.isDisposed()+e);
}

                        //this.onNext(actual,t,queue);
                } while (counter.decrementAndGet() != 0);
            }
        }








        public void onNext(T t) {
            //System.out.println(".");
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
