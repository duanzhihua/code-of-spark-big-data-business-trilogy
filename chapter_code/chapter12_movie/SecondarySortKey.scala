	class SecondarySortKey(val first:Double,val second:Double) extends 	Ordered [SecondarySortKey] with Serializable {
	      def compare(other:SecondarySortKey):Int = {
	        if (this.first - other.first !=0) {
	          (this.first - other.first).toInt
	        } else {
	          if (this.second - other.second > 0){
	              Math.ceil(this.second - other.second).toInt
	          } else if (this.second - other.second < 0){
	            Math.floor(this.second - other.second).toInt
	          } else {
	            (this.second - other.second).toInt
	          }
	        }
