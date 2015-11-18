package com.github.rdf_jena;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.StmtIterator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

import static java.util.stream.StreamSupport.stream;
import static java.util.Spliterators.spliteratorUnknownSize;

public class TestStorage {

    @Test
    public void iteration() {
        TDBStorage storage = new TDBStorage("data");
        Model model = storage.getDataset().getDefaultModel();

        Resource subject = model.createResource("http://www.openbel.org/bel/namespace/hgnc-human-genes/391");
        StmtIterator statementIterator = storage.iterate(subject, null, null);
        long count = stream(spliteratorUnknownSize(statementIterator, 0), false).count();

        Assert.assertEquals(39, count);
    }

    @Test
    public void ask() {
        // Is there an EntrezGene equivalent for HGNC:391?
        String askQuery =
                "PREFIX hgnc:    <http://www.openbel.org/bel/namespace/hgnc-human-genes/>\n" +
                "PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>\n" +
                "PREFIX belns:   <http://www.openbel.org/bel/namespace/>\n" +
                "ASK {\n" +
                    "hgnc:391 skos:exactMatch ?concept .\n" +
                    "?concept skos:inScheme   belns:entrez-gene .\n" +
                "}\n" ;
        TDBStorage storage = new TDBStorage("data");
        Assert.assertTrue(storage.ask(askQuery));
    }

    @Test
    public void describe() {
        // Describe the hgnc-human-genes ConceptScheme.
        TDBStorage storage = new TDBStorage("data");

        // single URI
        String describeQuery =
                "PREFIX hgnc:    <http://www.openbel.org/bel/namespace/hgnc-human-genes/>\n" +
                "PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>\n" +
                "PREFIX belns:   <http://www.openbel.org/bel/namespace/>\n" +
                "DESCRIBE belns:hgnc-human-genes\n";
        Iterator<Triple> triples = storage.describe(describeQuery);
        long count = stream(spliteratorUnknownSize(triples, 0), false).count();
        Assert.assertEquals(5, count);

        // where clause
        describeQuery =
                "PREFIX belv:    <http://www.openbel.org/vocabulary/>\n" +
                "PREFIX hgnc:    <http://www.openbel.org/bel/namespace/hgnc-human-genes/>\n" +
                "PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>\n" +
                "PREFIX belns:   <http://www.openbel.org/bel/namespace/>\n" +
                "DESCRIBE ?concept\n" +
                "WHERE {\n" +
                "    ?scheme   belv:prefix   \"taxon\" .\n" +
                "    ?concept  skos:inScheme ?scheme .\n"  +
                "}\n";
        triples = storage.describe(describeQuery);
        count = stream(spliteratorUnknownSize(triples, 0), false).count();
        Assert.assertEquals(29, count);
    }

    @Test
    public void select() {
        // Select the EntrezGene equivalent for HGNC:391?
        TDBStorage storage = new TDBStorage("data");

        String selectQuery =
                "PREFIX hgnc:    <http://www.openbel.org/bel/namespace/hgnc-human-genes/>\n" +
                "PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>\n" +
                "PREFIX belns:   <http://www.openbel.org/bel/namespace/>\n" +
                "SELECT ?equiv_pref_label\n" +
                "WHERE {\n" +
                "    hgnc:391       skos:exactMatch ?concept_equiv    .\n" +
                "    ?concept_equiv skos:prefLabel  ?equiv_pref_label .\n"  +
                "}\n";
        Iterable<QuerySolution> solutions = storage.select(selectQuery);
        long count = stream(spliteratorUnknownSize(solutions.iterator(), 0), false).count();
        //storage.select(selectQuery).forEach(System.out::println);
        Assert.assertEquals(9, count);
    }

    @Test
    public void construct() {
        // Construct the transitive closure of exactMatch / orthologousMatch for HGNC:391.
        TDBStorage storage = new TDBStorage("data");

        String constructQuery =
                "PREFIX hgnc:    <http://www.openbel.org/bel/namespace/hgnc-human-genes/>\n" +
                "PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>\n" +
                "PREFIX belv:   <http://www.openbel.org/vocabulary/>\n" +
                "CONSTRUCT {\n" +
                "    hgnc:391       skos:exactMatch        ?concept_equiv .\n" +
                "    hgnc:391       belv:orthologousMatch  ?concept_ortho .\n" +
                "    ?concept_equiv skos:exactMatch       ?concept_equiv .\n" +
                "    ?concept_ortho skos:orthologousMatch ?concept_ortho .\n" +
                "    ?concept_equiv skos:prefLabel        ?equiv_pref_label .\n" +
                "    ?concept_ortho skos:prefLabel        ?ortho_pref_label .\n" +
                "}\n" +
                "WHERE {\n" +
                "    hgnc:391       skos:exactMatch        ?concept_equiv .\n" +
                "    hgnc:391       belv:orthologousMatch  ?concept_ortho .\n" +
                "    ?concept_equiv skos:exactMatch*       ?concept_equiv .\n" +
                "    ?concept_ortho skos:orthologousMatch* ?concept_ortho .\n" +
                "    ?concept_equiv skos:prefLabel         ?equiv_pref_label .\n" +
                "    ?concept_ortho skos:prefLabel         ?ortho_pref_label .\n" +
                "}\n";
        Model model = storage.construct(constructQuery);
        //model.listStatements().forEachRemaining(System.out::println);
        Assert.assertEquals(71, model.size());
    }
}
