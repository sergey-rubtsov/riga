package open.data.lv.spark.utils;

import open.data.lv.spark.Stop;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.neighboursearch.KDTree;

import java.util.ArrayList;
import java.util.List;

public class KNNClassifier {

    private KDTree knn;

    private Attribute lat = new Attribute("lat");
    private Attribute lon = new Attribute("lon");
    ArrayList<Attribute> coordinates;


    public KNNClassifier(List<Stop> points) {
        ArrayList<String> my_nominal_values = new ArrayList<>(points.size());
        points.forEach(p -> {
            my_nominal_values.add(p.getId());
        });

        Attribute id = new Attribute("id",  my_nominal_values);

        coordinates = new ArrayList<>(3);
        coordinates.add(lat);
        coordinates.add(lon);
        coordinates.add(id);

        Instances instances = new Instances("coordinates", coordinates, points.size());
        instances.setClassIndex(id.index());


        points.forEach(p -> {
            Instance inst = new DenseInstance(3);
            inst.setValue(lat, p.getLat());
            inst.setValue(lon, p.getLon());
            inst.setValue(id, p.getId());
            instances.add(inst);
            inst.setDataset(instances);
        });
        this.knn = new KDTree();
        try {
            this.knn.setInstances(instances);
        } catch (Exception e) {
            throw new RuntimeException("unable to create classifier");
        }
    }

    public String findNearestNeighbourId(float lat, float lon) {
        Instance inst = new DenseInstance(2);
        inst.setValue(this.lat, lat);
        inst.setValue(this.lon, lon);
        Instances targetDs = new Instances("target", coordinates, 1);
        targetDs.add(inst);
        try {
            Instance i = knn.nearestNeighbour(targetDs.firstInstance());
            return i.stringValue(2);
        } catch (Exception e) {
            throw new RuntimeException("unable to find nearest neighbour");
        }
    }

}
