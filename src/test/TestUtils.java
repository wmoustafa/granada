package test;


public class TestUtils {

	public static class LongPair implements Comparable<LongPair>
	{
		long first,second;
		
		public LongPair(long fst, long snd)
		{
			this.first = fst;
			this.second = snd;
		}

		public long getFirst() {
			return first;
		}

		public void setFirst(long first) {
			this.first = first;
		}

		public long getSecond() {
			return second;
		}

		public void setSecond(long second) {
			this.second = second;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (first ^ (first >>> 32));
			result = prime * result + (int) (second ^ (second >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			LongPair other = (LongPair) obj;
			if (first != other.first)
				return false;
			if (second != other.second)
				return false;
			return true;
		}


		@Override
		public int compareTo(LongPair o) {
			assert(o != null);
			
			if (this == o)
				return 0;
			LongPair other = (LongPair) o;
			if(this.first == other.first)
			{
				return Long.compare(second, other.second);
			}
			else
				return Long.compare(first, other.first);
		}
		
		@Override
		public String toString(){
			return new String("["+ first + ", " + second +"]");
		}
					
	}	
}
