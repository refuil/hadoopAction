

public class Test {

    public static void main(String[] args) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < 101; i++) {
            sb.append("hadoop demo " + i + "\r\n");
        }
        System.out.println(sb.toString());
    }

}
