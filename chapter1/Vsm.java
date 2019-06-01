import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Vsm {
    public static double calCosSim(Map<String, Double> v1, 
Map<String, Double> v2){
        double sclar = 0.0,norm1=0.0,norm2=0.0,similarity=0.0;
        Set<String> v1Keys = v1.keySet();
        Set<String> v2Keys = v2.keySet();
        Set<String> both= new HashSet<>();
        both.addAll(v1Keys);
        both.retainAll(v2Keys);
        System.out.println(both);
        for (String str1 : both) {
            sclar += v1.get(str1) * v2.get(str1);
        }
        for (String str1:v1.keySet()){
            norm1+=Math.pow(v1.get(str1),2);
        }
        for (String str2:v2.keySet()){
            norm2+=Math.pow(v2.get(str2),2);
        }
        similarity=sclar/Math.sqrt(norm1*norm2);
        System.out.println("sclar:"+sclar);
        System.out.println("norm1:"+norm1);
        System.out.println("norm2:"+norm2);
        System.out.println("similarity:"+similarity);
        return similarity;
    }
    public static void main(String[] args) {
        Map<String, Double> m1 = new HashMap<>();
        m1.put("Hello", 1.0);
        m1.put("css", 2.0);
        m1.put("Lucene", 3.0);

        Map<String, Double> m2 = new HashMap<>();
        m2.put("Hello", 1.0);
        m2.put("Word", 2.0);
        m2.put("Hadoop", 3.0);
        m2.put("java", 4.0);
        m2.put("html", 1.0);
        m2.put("css", 2.0);
        calCosSim(m1, m2);
    }
}
