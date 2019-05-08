package com.easou.example;

import scala.Serializable;
import scala.math.Ordered;

/**
 * ClassName SecondSortKey
 * 功能: 自定义二次排序key
 * Author yangweifeng
 * Date 2018/11/25 18:31
 * Version 1.0
 **/
public class SecondSortKey implements Ordered<SecondSortKey>, Serializable{
    private static final long serialVersionUID = 1l;
    // 自定义key，需要排序的列
    private Integer first;
    private Integer second;
    public SecondSortKey(Integer first, Integer second) {
        this.first = first;
        this.second = second;
    }
    @Override
    public boolean $less(SecondSortKey other) {
        if (this.first < other.getFirst()){
            return true;
        }else if (this.first == other.getFirst() && this.second < other.getSecond()){
            return true;
        }
        return false;
    }

    /**
     * 大于判断
     * @param other
     * @return
     */
    @Override
    public boolean $greater(SecondSortKey other) {
        if (this.first > other.getFirst()){
            return true;
        }else if (this.first == other.getFirst() && this.second > other.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondSortKey other) {
        if (this.$less(other)){
            return true;
        }else if (this.first == other.getFirst() && this.second == other.getSecond()){
            return true;
        }
        return false;
    }

    /**
     * 大于等于
     * @param other
     * @return
     */
    @Override
    public boolean $greater$eq(SecondSortKey other) {
      if (this.$greater(other)){
         return true;
      }else if (this.first == other.getFirst() && this.second == other.getSecond()){
          return true;
      }
      return false;
    }
    /*
        1.compareTo（Object obj）方法是java.lang.Comparable接口中的方法， 当需要对类的对象进行排序时，该类需要实Comparable接口，必须重写public int compareTo(T)方法，
        String类等一些类默认实现了该接口；
        String类默认实现了该接口 compareTo（）方法的返回值 s1.compareTo(s2)—》s1 与 s2 的ASC码 的差值，其实就是字典排序；
        2.compare（Object o1, Object o2）方法是java.util.Comparator接口的方法, 它实际上用的是待比较对象的compareTo（Object obj）方法；
        3.arrayList排序时
        1.Collections.sort(list) 会自动调用User实现的Comparable的compareTo()方法
        Comparator comparator = new Comparator() {
        @Override
        public int compare(User user0, User user1) {
        return user0.id.compareTo(user1.id);}};
        2.Collections.sort(list, comparator)//也可以重新实现排序的方法
     */
    @Override
    public int compare(SecondSortKey other) {
        if (this.getFirst() != other.getFirst()) {
            return this.getFirst() - other.getFirst();
        }else {
            return this.getSecond() - other.getSecond();
        }
    }
    @Override
    public int compareTo(SecondSortKey other) {
        if (this.getFirst() != other.getFirst()) {
            return this.getFirst() - other.getFirst();
        }else {
            return this.getSecond() - other.getSecond();
        }
    }

    public Integer getFirst() {
        return first;
    }

    public void setFirst(Integer first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }
    //为要进行排序的多个列，提供get和set方法，hashcode、equal方法

}
