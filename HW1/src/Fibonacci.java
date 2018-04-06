// A class to implement Fibonacci(17)
public class Fibonacci {

	int N;
	public Fibonacci() {
		N = 17;
	}
	public static int fibonacci(int n) {
		if (n==0)
			return 0;
		else if (n==1)
			return 1;
		else
			return fibonacci(n-1)+fibonacci(n-2);
	}
	
	public int f() {
		return fibonacci(N);
	}

}
