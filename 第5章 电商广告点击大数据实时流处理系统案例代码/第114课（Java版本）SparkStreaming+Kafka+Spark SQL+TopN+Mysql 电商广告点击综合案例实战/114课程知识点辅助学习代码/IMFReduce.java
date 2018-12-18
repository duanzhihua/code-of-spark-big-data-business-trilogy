package com.dt.spark.IMF114;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.derby.tools.sysinfo;

public class IMFReduce {

	public static <MYF, MYT> MYT myIMFreduce(final Iterable<MYF> iterable, final Func<MYF, MYT> func, MYT origin) {

		MYT result = null;
		for (Iterator iterator = iterable.iterator(); iterator.hasNext();) {
			result = func.IMFapply((MYF) (iterator.next()), origin);
		}
		return result;
	}

	public static void main(String[] args) {
		List<Person> people = new ArrayList<Person>();
		people.add(new Person("张三", 90));
		people.add(new Person("李四", 100));
		people.add(new Person("王五", 90));
		people.add(new Person("小李", 60));

		/*
		 * for ( Person row:people) { System.out.println(row.getScore()); }
		 */

		Integer maxScore = IMFReduce.myIMFreduce(people, new Func<Person, Integer>() {
			int countScore = 0;

			public Integer IMFapply(Person person, Integer origin) {

				int getScoreResult = (person.getScore() > origin ? person.getScore() : origin);
				countScore += getScoreResult;
				// System.out.println(countScore + "person.getScore() " +
				// person.getScore() + " origin "+ origin );
				return countScore;
			}
		}, 80); //80分初始分

		System.out.println("=======IMF reduce ===========!");
		System.out.println("myIMFreduce 汇总分数(分数低于80分按80分统计)累计： " + maxScore);

	}

	public interface Func<F, T> {

		T IMFapply(F currentElement, T origin);
	}

}

class Person {
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getScore() {
		return score;
	}

	public void setScore(int score) {
		this.score = score;
	}

	private String name;
	private int score;

	public Person(String name, int score) {
		this.name = name;
		this.score = score;
	}
}
