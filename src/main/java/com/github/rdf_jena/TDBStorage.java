package com.github.rdf_jena;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.util.iterator.ClosableIterator;

import java.io.InputStream;
import java.util.Iterator;
import java.util.function.Function;

public class TDBStorage implements QueryAPI, StorageAPI {

    private final Dataset dataset;

    public TDBStorage(String dir) {
        dataset = TDBFactory.createDataset(dir);
    }

    public Dataset getDataset() {
        return dataset;
    }

    @Override
    public void load(InputStream stream, Lang langHint) {
        Model model = dataset.getDefaultModel();
        RDFDataMgr.read(model, stream, langHint);
    }

    @Override
    public StmtIterator iterate(Resource subject, Property predicate, RDFNode object) {
        Model model = dataset.getDefaultModel();
        return model.listStatements(subject, predicate, object);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Triple> describe(String query) {
        return query(query, QueryExecution::execDescribeTriples);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean ask(String query) {
        return query(query, QueryExecution::execAsk);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<QuerySolution> select(String query) {
        return () -> new SelectIterator(query, dataset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Model construct(String query) {
        return query(query, QueryExecution::execConstruct);
    }

    /**
     * Executes the query, returning the desired interpretation.
     */
    protected <R> R query(String query, Function<QueryExecution, R> resultsFunction) {
        Model model = dataset.getDefaultModel();
        try(QueryExecution queryExec = QueryExecutionFactory.create(query, model)) {
            return resultsFunction.apply(queryExec);
        }
    }

    private class SelectIterator implements ClosableIterator<QuerySolution> {

        private final QueryExecution queryExecution;
        private ResultSet resultSet;
        private boolean ended = false;

        public SelectIterator(String query, Dataset dataset) {
            this.queryExecution = QueryExecutionFactory.create(query, dataset.getDefaultModel());
        }

        @Override
        public boolean hasNext() {
            if (resultSet == null) {
                resultSet = queryExecution.execSelect();
            }

            boolean more = resultSet.hasNext();
            if (!more) {
                close();
                ended = true;
            }
            return more;
        }

        @Override
        public QuerySolution next() {
            if (resultSet == null) {
                resultSet = queryExecution.execSelect();
            }

            QuerySolution solution = resultSet.next();
            if (solution == null) {
                close();
                ended = true;
            }
            return solution;
        }

        @Override
        public void close() {
            if (!ended) {
                queryExecution.close();
            }
        }
    }
}
