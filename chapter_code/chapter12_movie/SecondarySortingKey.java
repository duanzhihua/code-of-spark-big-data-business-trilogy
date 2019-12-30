	package com.dt.spark.cores;
	import scala.Serializable;
	import scala.math.Ordered;
	public class SecondarySortingKey implements Ordered<SecondarySortingKey>, 	Serializable{
	    private int first;
	    private int second;
	    public int getFirst() {
	        return first;
	    }
	    public void setFirst(int first) {
	        this.first = first;
	    }
	    public int getSecond() {
	        return second;
	    }
	    public void setSecond(int second) {
	        this.second = second;
	    }
	    public SecondarySortingKey(int first, int second){
	        this.first = first;
	        this.second = second;
	    }
	    @Override
	    public String toString() {
	        return super.toString();
	    }
	    @Override
	    public boolean equals(Object o) {
	        if (this == o) return true;
	        if (o == null || getClass() != o.getClass()) return false;
	        SecondarySortingKey that = (SecondarySortingKey) o;
	        if (first != that.first) return false;
	        return second == that.second;
	    }
	    @Override
	    public int hashCode() {
	        int result = first;
	        result = 31 * result + second;
	        return result;
	    }
	    @Override
	    public int compare(SecondarySortingKey that) {
	        if(this.first - that.getFirst() != 0){
	            return this.first - that.getFirst();
	        } else {
	            return this.second - that.getSecond();
	        }
	    }
	    @Override
	    public boolean $less(SecondarySortingKey that) {
	        if(this.first < that.getFirst()) {
	            return  true;
	        } else if (this.first == that.getFirst() &&this.second < that.getSecond()){
	            return true;
	        }
	        return false;
	    }
	    @Override
	    public boolean $less$eq(SecondarySortingKey other) {
	        if(SecondarySortingKey.this.$less(other)){
	            return true;
	        } else if (this.first == other.getFirst() && this.second == other.getSecond()){
	            return true;
	        }
	        return false;
	    }
	    @Override
	    public boolean $greater(SecondarySortingKey that) {
	        if(this.first > that.getFirst()){
	            return true;
	        }else if(this.first == that.getFirst() && this.second > that.getSecond()) {
	            return true;
	        }
	        return false;
	    }
	    @Override
	    public boolean $greater$eq(SecondarySortingKey that) {
	        if (SecondarySortingKey.this.$greater(that)){
	            return true;
	        } else if (this.first == that.getFirst() && this.second == that.getSecond()){
	            return true;
	        }
	        return false;
	    }
	    @Override
	    public int compareTo(SecondarySortingKey that) {
	        if(this.first - that.getFirst() != 0){
	            return this.first - that.getFirst();
	        } else {
	            return this.second - that.getSecond();
	        }
	    }
	}

