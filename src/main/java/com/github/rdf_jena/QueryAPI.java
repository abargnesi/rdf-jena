package com.github.rdf_jena;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.*;

import java.util.Iterator;

public interface QueryAPI {

    StmtIterator            iterate(Resource subject, Property predicate, RDFNode object);

    Iterator<Triple>        describe(String query);

    boolean                 ask(String query);

    Iterable<QuerySolution> select(String query);

    Model                   construct(String query);
}
