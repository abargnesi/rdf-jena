package com.github.rdf_jena;

import org.apache.jena.query.*;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.tdb.TDB;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class Main {

    public static void main(String[] args) throws IOException {
        TDBStorage tdb = new TDBStorage("data");
        //tdb.load(new FileInputStream("namespaces-inferred.nq"), Lang.NQ);

        //runSelect("SELECT (COUNT(*) as ?no) WHERE { ?s ?p ?o .}", null, tdb.getDataset());

        String eqSparql = Files.readAllLines(new File("equivalence.sparql").toPath()).stream().collect(Collectors.joining("\n"));
        List<String> affyProbesetIds = Files.readAllLines(new File("affy-probeset-ids.belns").toPath());

        final AtomicInteger count = new AtomicInteger(0);
        long start = System.currentTimeMillis();
        affyProbesetIds.forEach(id -> {
            QuerySolutionMap solMap = new QuerySolutionMap();
            solMap.add("prefix",   tdb.getDataset().getDefaultModel().createLiteral("affx"));
            solMap.add("id",       tdb.getDataset().getDefaultModel().createLiteral(id));
            solMap.add("e_prefix", tdb.getDataset().getDefaultModel().createLiteral("egid"));
            runSelect(eqSparql, solMap, tdb.getDataset());

            count.incrementAndGet();
        });
        long end = System.currentTimeMillis();

        System.out.println("Average time per query: " + ((end - start) / (float) count.get()));
    }

    public static void runSelect(String sparql, QuerySolution initialSolution, Dataset dataset) {
        Prologue queryPrologue = new Prologue();
        queryPrologue.setPrefix("belv", "http://www.openbel.org/vocabulary/");
        queryPrologue.setPrefix("dct",  "http://purl.org/dc/terms/");
        queryPrologue.setPrefix("skos", "http://www.w3.org/2004/02/skos/core#");
        queryPrologue.setPrefix("xsd",  "http://www.w3.org/2001/XMLSchema#");
        queryPrologue.setPrefix("ns",   "http://www.openbel.org/bel/namespace/");

        Query query = QueryFactory.parse(new Query(queryPrologue), sparql, null, null);

//        System.out.println(format("\nRunning query:\n%s", query));

        try(QueryExecution queryExec = QueryExecutionFactory.create(query, dataset)) {
            // NB: when tdb:unionDefaultGraph is set to true, the default graph is the union of all named graphs
            // NB: the union of all named graphs excludes the default graph
            queryExec.getContext().set(TDB.symUnionDefaultGraph, false);
            Optional.ofNullable(initialSolution).ifPresent(querySolution -> queryExec.setInitialBinding(initialSolution));

            ResultSet rs = queryExec.execSelect();
            if (rs.hasNext()) {
                String header = rs.getResultVars().stream().collect(Collectors.joining("\t\t"));
                //System.out.println(header);
                while (rs.hasNext()) {
                    QuerySolution solution = rs.next();
                    List<String> names = new ArrayList<>();
                    solution.varNames().forEachRemaining(names::add);
                    String row = names.stream().
                            map(var -> solution.get(var).toString()).
                            collect(Collectors.joining("\t\t"));
                    //System.out.println(row);
                }
            }
        }

    }
}
