package open.data.lv.spark.utils;

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
        ArrayList<Double> stop1 = new ArrayList<Double>();
        stop1.add(1.0);
        stop1.add(1.0);

        ArrayList<Double> stop2 = new ArrayList<Double>();
        stop2.add(4.0);
        stop2.add(4.0);

        ArrayList<Double> stop3 = new ArrayList<Double>();
        stop3.add(10.0);
        stop3.add(1.0);

        ArrayList<ArrayList<Double>> ar = new ArrayList<ArrayList<Double>>();
        ar.add(stop1);
        ar.add(stop2);
        ar.add(stop3);

        // attributes
        Attribute lat = new Attribute("lat", 0);
        Attribute lon = new Attribute("lon", 1);
        ArrayList<Attribute> attrs = new ArrayList<>();
        attrs.add(lat);
        attrs.add(lon);

        // instances
        Instances ds = new Instances("ds", attrs, 10);
        for (ArrayList<Double> d : ar) {
            Instance i = new DenseInstance(2);
            i.setValue(lat, d.get(0)); // x
            i.setValue(lon, d.get(1)); // y
            ds.add(i);
        }


        //KDTree knn = new KDTree(ds); // causes a runtime exception; requires setInstances() instead
        KDTree knn = new KDTree();
        try {
            knn.setInstances(ds);
            //knn.getDistanceFunction();
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }


        Instance target = new DenseInstance(2);
        target.setValue(lat, 5);
        target.setValue(lon, 2);
        Instances targetDs = new Instances("target", attrs, 1);
        targetDs.add(target);

        Instances nearestInstances;
        try {
            nearestInstances = knn.kNearestNeighbours(targetDs.firstInstance(), 2);
            //knn.setDistanceFunction();
            //Instance i = knn.nearestNeighbour(target);
            //System.out.println(i.value(a1));

            for (int i = 0; i < nearestInstances.numInstances(); i++) {

                System.out.println(nearestInstances.instance(i).value(lat) + ", " + nearestInstances.instance(i).value(lon));
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

