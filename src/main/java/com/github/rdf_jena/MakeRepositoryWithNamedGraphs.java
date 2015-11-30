package com.github.rdf_jena;

import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.tdb.TDBFactory;

import java.util.Iterator;

public class MakeRepositoryWithNamedGraphs {

    public static void main(String[] args) {
        Dataset ds = TDBFactory.createDataset("data2");

        Model m1 = ModelFactory.createMemModelMaker().createModel("http://www.named-graphs.org/1");
        m1.add(m1.createResource("http://www.people.org/tony"), m1.createProperty("http://actions.org/likes"), "burritos");
        ds.addNamedModel("http://www.named-graphs.org/1", m1);

        Model m2 = ModelFactory.createMemModelMaker().createModel("http://www.named-graphs.org/2");
        m2.add(m2.createResource("http://www.people.org/kate"), m2.createProperty("http://actions.org/likes"), "gadgets");
        ds.addNamedModel("http://www.named-graphs.org/2", m2);

        Iterator<String> names = ds.listNames();
        while (names.hasNext()) {
            System.out.println(names.next());
        }

        ds.close();
    }
}
