package open.data.lv.spark;

public class Fib {
    public static void main(String[] args) {
        //0,1,1,2,3,5,8,13,21
        for (int i = 0; i < 20; i++) {
            System.out.print(fib(i) + ", ");
        }

    }

    private static int fib(int i) {
        if (i == 0) {
            return 0;
        }
        if (i <= 2) {
            return 1;
        }
        int fib = 3;
        int current = 2;
        for (int j = 3; j < i; j++) {
            int temp = fib;
            fib = fib + current;
            current = temp;
        }
        return fib;
    }
}
