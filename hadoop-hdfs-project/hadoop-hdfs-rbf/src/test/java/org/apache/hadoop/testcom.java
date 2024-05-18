package org.apache.hadoop;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;


@SuppressWarnings("checkstyle:TypeName")
public class testcom {

  @Test
  public void test1() throws InterruptedException, ExecutionException {
    CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
      int i = 0;
      if (i == 1) {
        return "zj";
      }
      throw new RuntimeException(new IOException("asdasdsadsadasd"));
      // 可能抛出异常的代码
    }).thenApplyAsync(res -> {
      // 使用结果的代码
      System.out.println("[adasd]");
      return res;
    });
        //.exceptionally(ex -> {
//      // 处理异常
//      throw new CompletionException(ex);
//    }).thenApplyAsync(s -> {
//      System.out.println("[zz]"+s);
//
//      return s;
//    }).thenCompose(s -> CompletableFuture.supplyAsync(() -> "[asdasdasdsadsa]"))
//        .exceptionally(throwable -> {
//          System.out.println(throwable);
//          throwable.printStackTrace();
//          return "aa";
//        });


    System.out.println(future.get());
  }

  @Test
  public void test2() throws ExecutionException, InterruptedException {
    CompletableFuture<String> completableFuture = CompletableFuture.completedFuture("0");
    List<String> list = new ArrayList<>();
    list.add("1");
    list.add("2");
    list.add("3");

    for (String s : list) {
      completableFuture = completableFuture.thenCompose(o -> {
        System.out.println(s);
        if (s.equals("2")) {
          throw new CompletionException(new IOException("asdasdasd"));
        }
        return CompletableFuture.completedFuture(o + s);
      });
    }

    System.out.println(completableFuture.get());
  }
}
