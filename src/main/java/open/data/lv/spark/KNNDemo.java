package open.data.lv.spark;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.neighboursearch.KDTree;

import java.util.ArrayList;

/**
 * @author Gowtham Girithar Srirangasamy
 */
public class KNNDemo {

    public KNNDemo() {
        ArrayList<Double> d1 = new ArrayList<Double>();
        d1.add(1.0);
        d1.add(1.0);

        ArrayList<Double> d2 = new ArrayList<Double>();
        d2.add(4.0);
        d2.add(4.0);

        ArrayList<Double> d3 = new ArrayList<Double>();
        d3.add(10.0);
        d3.add(1.0);

        ArrayList<ArrayList<Double>> ar = new ArrayList<ArrayList<Double>>();
        ar.add(d1);
        ar.add(d2);
        ar.add(d3);

        // attributes
        Attribute a1 = new Attribute("x-attr", 0);
        Attribute a2 = new Attribute("y-attr", 0);
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(a1);
        attrs.add(a2);

        // instances
        Instances ds = new Instances("ds", attrs, 10);
        for (ArrayList<Double> d : ar) { // iterate thro points
            Instance i = new DenseInstance(2);
            i.setValue(a1, d.get(0)); // x
            i.setValue(a2, d.get(1)); // y
            ds.add(i);
        }

        Instance target = new DenseInstance(2);
        target.setValue(a1, 3);
        target.setValue(a2, 1);
        //KDTree knn = new KDTree(ds); // causes a runtime exception; requires setInstances() instead
        KDTree knn = new KDTree();
        try {
            knn.setInstances(ds);
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        Instances targetDs = new Instances("target", attrs, 1);
        targetDs.add(target);

        Instances nearestInstances;
        try {
            nearestInstances = knn.kNearestNeighbours(targetDs.firstInstance(), 3);
            for (int i = 0; i < nearestInstances.numInstances(); i++) {

                System.out.println(nearestInstances.instance(i).value(a1) + ", " + nearestInstances.instance(i).value(a2));
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        KNNDemo kdTreeTest = new KNNDemo();
    }


}

