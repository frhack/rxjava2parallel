package org.teepee.parallelobservable;

import io.reactivex.Observable;
import io.reactivex.Single;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;

/**
 * Created by francesco on 20/11/2016.
 */


public class ParallelObservableTests {


    @org.testng.annotations.Test
    public void costructor() throws Exception {

        IntStream integerStream = IntStream.range(999999999, 1999999999);
        Observable<Integer> integerObservable = Observable.fromIterable(integerStream::iterator);
        ParallelObservable<Integer> integerParallelObservable = new ParallelObservable<>(integerObservable);
        assert integerParallelObservable.getClass() == ParallelObservable.class;
    }

    @org.testng.annotations.Test
    public void costructorFromObservable() throws Exception {

        IntStream integerStream = IntStream.range(999999999, 1999999999);
        Observable<Integer> integerObservable = Observable.fromIterable(integerStream::iterator);
        ParallelObservable<Integer> integerParallelObservable = ParallelObservable.fromObservable(integerObservable);
        assert integerParallelObservable.getClass() == ParallelObservable.class;
    }

    @org.testng.annotations.Test
    public void costructorFromIterable() throws Exception {
        IntStream integerStream = IntStream.range(999999999, 1999999999);
        ParallelObservable<Integer> integerParallelObservable = ParallelObservable.fromIterable(integerStream::iterator);
        assert integerParallelObservable.getClass() == ParallelObservable.class;
    }

    @org.testng.annotations.Test
    public void costructorRange() throws Exception {
        ParallelObservable<Integer> integerParallelObservable = ParallelObservable.range(999999999, 1000000000);
        assert integerParallelObservable.getClass() == ParallelObservable.class;
    }


    @org.testng.annotations.Test
    public void getObservable() throws Exception {
        Observable<Integer> integerObservable = Observable.range(999999999, 1000000000);
        ParallelObservable<Integer> integerParallelObservable = new ParallelObservable<Integer>(integerObservable);
        assert integerParallelObservable.getObservable() == integerObservable; //get the source observable, no thread safe
        assert integerParallelObservable.toObservable() != integerObservable; // trasfrom parallel observable back to sequenzial observable

    }


    @org.testng.annotations.Test
    public void testFilter1() throws Exception {

        Observable<Integer> integerObservable = Observable.range(999999999, 1000000000);
        ParallelObservable<Integer> integerParallelObservable = new ParallelObservable<Integer>(integerObservable);
        ParallelObservable<Integer> parallelObservablePrimes = integerParallelObservable.withThreadsPoolSize(9).filter((Integer c) -> {
            return BigInteger.valueOf(c).isProbablePrime(100000);
        });

        assert parallelObservablePrimes.toObservable().take(100000).toList().blockingGet().size() == 100000;

    }

    // test ParallelObservable.filter( lambda ) .filter(lambda)
    @org.testng.annotations.Test
    public void testFilter2() throws Exception {

        Observable<Integer> integerObservable = Observable.range(999999999, 1000000000);
        ParallelObservable<Integer> integerParallelObservable = new ParallelObservable<>(integerObservable);

        ParallelObservable<Integer> parallelObservablePrimes = integerParallelObservable.withThreadsPoolSize(4).filter((Integer c) -> {
            return BigInteger.valueOf(c).isProbablePrime(100000);
        })
                .withThreadsPoolSize(4).filter((Integer i) -> {
                    String s = i.toString();
                    return s.charAt(s.length() - 1) == '3';
                });
        assert parallelObservablePrimes.toObservable().take(10000).toList().blockingGet().size() == 10000;
    }


    // check that parallel is faster
    @org.testng.annotations.Test
    public void testFilterSpeed() throws Exception {

        Observable<Integer> integerObservable = Observable.range(999999999, 1000000000);
        ParallelObservable<Integer> integerParallelObservable = new ParallelObservable<>(integerObservable);

        ParallelObservable<Integer> parallelObservablePrimes = integerParallelObservable.withThreadsPoolSize(4).filter((Integer c) -> {
            return BigInteger.valueOf(c).isProbablePrime(100000);
        })
                .withThreadsPoolSize(4).filter((Integer i) -> {
                    String s = i.toString();
                    return s.charAt(s.length() - 1) == '3';
                });
        Long time0 = System.currentTimeMillis();
        assert parallelObservablePrimes.take(10000).getObservable().toList().blockingGet().size() == 10000;
        Long time1 = System.currentTimeMillis();

        Long parallelElapse = time1 - time0;

        Observable<Integer> observablePrimes = integerObservable.filter((Integer c) -> {
            return BigInteger.valueOf(c).isProbablePrime(100000);
        })
                .filter((Integer i) -> {
                    String s = i.toString();
                    return s.charAt(s.length() - 1) == '3';
                });
        time0 = System.currentTimeMillis();
        assert observablePrimes.take(10000).toList().blockingGet().size() == 10000;
        time1 = System.currentTimeMillis();
        Long nonParallelElapse = time1 - time0;
        assert parallelElapse<nonParallelElapse;
    }



    // test
    @org.testng.annotations.Test
    public void testFilterOutput() throws Exception {

        IntStream integerStream = IntStream.range(1, 1999999999);
        Observable<Integer> integerObservable = Observable.fromIterable(() -> integerStream.iterator()).take(1000000);
        ParallelObservable<Integer> integerParallelObservable = new ParallelObservable<>(integerObservable);

        ParallelObservable<Integer> parallelObservablePrimes = integerParallelObservable.filter((Integer c) -> {
            return c % 2 == 0;
        });

        assert parallelObservablePrimes.toObservable().toList().blockingGet().size() == 500000;
    }




    @org.testng.annotations.Test
    public void testFilterOutputAndSpeed() throws Exception {

        ParallelObservable<Integer> integerParallelObservable = ParallelObservable.range(999999999, 2000);

        ParallelObservable<Integer> parallelObservablePrimes = integerParallelObservable
                .withThreadsPoolSize(4).filter((Integer c) -> {
                    try{ sleep(1); }catch(Exception e){};
                    String s = new Integer(c).toString();
                    return s.charAt(s.length() - 1) != '3';
                })
                .withThreadsPoolSize(4).filter((Integer c) -> {
                    try{ sleep(1); }catch(Exception e){};
                    String s = new Integer(c).toString();
                    return s.charAt(s.length() - 1) != '2';
                });
        long time0 = System.currentTimeMillis();
        assert parallelObservablePrimes.toObservable().toList().blockingGet().size() == 1600;
        long parallelElapse = System.currentTimeMillis() - time0;


        IntStream integerStream = IntStream.range(999999999, 999999999 + 2000);

        integerStream = integerStream
                .filter((int c) -> {
                    try{ sleep(1); }catch(Exception e){};
                    String s = new Integer(c).toString();
                    return s.charAt(s.length() - 1) != '3';
                })
                .filter((int c) -> {
                    try{ sleep(1); }catch(Exception e){};
                    String s = new Integer(c).toString();
                    return s.charAt(s.length() - 1) != '2';
                });
        ;


        time0 = System.currentTimeMillis();
        assert integerStream.toArray().length == 1600;
        long nonParallelElapse = System.currentTimeMillis() - time0;

        //System.out.println(parallelElapse);
        //System.out.println(nonParallelElapse);
        assert parallelElapse < nonParallelElapse;

    }





    @org.testng.annotations.Test
    public void testFilterBuffered() throws Exception {

        ParallelObservable<Integer> integerParallelObservable = ParallelObservable.range(999999999, 1000000000);
        integerParallelObservable.setBufferSize(100);
        ParallelObservable<Integer> evens = integerParallelObservable.withThreadsPoolSize(4).filter((Integer i) -> i % 2 == 0);

        assert evens.toObservable().take(10000).toList().blockingGet().size() == 10000;

    }


    @org.testng.annotations.Test
    public void testMap() throws Exception {

        ParallelObservable<Integer> integerParallelObservable = ParallelObservable.range(999999999, 1000000000);

        ParallelObservable<String> parallelObservablePrimes = integerParallelObservable.withThreadsPoolSize(4).map(Object::toString);

        assert parallelObservablePrimes.toObservable().take(10000).toList().blockingGet().size() == 10000;

    }


    @org.testng.annotations.Test
    public void testMapBuffered() throws Exception {

        ParallelObservable<Integer> integerParallelObservable = ParallelObservable.range(999999999, 1000000000);
        integerParallelObservable.setBufferSize(100);
        ParallelObservable<String> parallelObservablePrimes = integerParallelObservable.withThreadsPoolSize(4).map(Object::toString);


        assert parallelObservablePrimes.toObservable().take(10000).toList().blockingGet().size() == 10000;

    }


    @org.testng.annotations.Test
    public void testDoOnNext() throws Exception {

        ParallelObservable<Integer> integerParallelObservable = ParallelObservable.range(999999999, 1000000000);

        ParallelObservable<Integer> parallelObservablePrimes = integerParallelObservable.withThreadsPoolSize(4).doOnNext(System.out::println);

        assert parallelObservablePrimes.toObservable().take(10).toList().blockingGet().size() == 10;

    }

    @org.testng.annotations.Test
    public void testTakeWhile() throws Exception {
    ParallelObservable<Integer> po = ParallelObservable.range(0, 10000000);

    assert   po.withThreadsPoolSize(4).doOnNext((Integer i) -> i++).withThreadsPoolSize(4).doOnNext((Integer i) -> i++).takeWhile((Integer i)->i < 5000000)
            .getObservable().serialize().toList().blockingGet().size() <= 5000000;
    }



    @org.testng.annotations.Test
    public void testDoOnNextBuffered() throws Exception {

        ParallelObservable<Integer> integerParallelObservable = ParallelObservable.range(999999999, 1000000000);
        integerParallelObservable.setBufferSize(100);
        ParallelObservable<Integer> parallelObservablePrimes = integerParallelObservable.withThreadsPoolSize(4).doOnNext(System.out::println);

        Single<List<Integer>> s =  parallelObservablePrimes.toObservable().take(10000).toList();
               assert s.blockingGet().size() == 10000;

    }


}