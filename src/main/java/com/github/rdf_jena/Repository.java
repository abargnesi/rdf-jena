package com.github.rdf_jena;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.tdb.TDBFactory;
import org.jruby.*;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.exceptions.RaiseException;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static com.github.rdf_jena.JenaConverters.*;
import static com.github.rdf_jena.JenaRepositoryService.findClass;
import static com.github.rdf_jena.RubyRDFConverters.convertNode;
import static com.github.rdf_jena.RubyRDFConverters.convertQuad;
import static com.github.rdf_jena.TransactionUtil.executeInTransaction;
import static org.jruby.RubyBoolean.newBoolean;
import static org.jruby.RubyFixnum.newFixnum;
import static org.jruby.RubyString.newString;
import static org.jruby.RubySymbol.newSymbol;

@JRubyClass(name = "Repository")
public class Repository extends RubyObject {

    public static ObjectAllocator Allocator = Repository::new;

    /**
     * Jena {@link Dataset} backed by a TDB database. This state is should be
     * used as final within this Ruby Object.
     */
    protected Dataset ds;

    public Repository(Ruby runtime, RubyClass metaclass) {
        super(runtime, metaclass);
    }

    @JRubyMethod(name = "initialize", required = 1, optional = 1)
    public IRubyObject initialize(
            ThreadContext ctx,
            IRubyObject[] args
    ) {
        String datasetDirectory = args[0].asJavaString();
        ds                      = TDBFactory.createDataset(datasetDirectory);

        //Ruby ruby                      = ctx.runtime;
        //RubyHash options               = (args.length <= 1) ? RubyHash.newHash(ruby) : args[1].convertToHash();
        //Boolean someOption             = (Boolean) options.get(newSymbol(ruby, "some_option"));

        return ctx.nil;
    }

    @JRubyMethod(name = "durable?")
    public RubyBoolean isDurable(ThreadContext ctx) {
        return newBoolean(ctx.runtime, true);
    }

    @JRubyMethod(name = "empty?")
    public RubyBoolean isEmpty(ThreadContext ctx) {
        return executeInTransaction(ds, ReadWrite.READ, ds -> newBoolean(ctx.runtime, ds.asDatasetGraph().isEmpty()));
    }

    @JRubyMethod(name = {"count", "size"})
    public RubyFixnum size(ThreadContext ctx) {
        return executeInTransaction(ds, ReadWrite.READ, ds -> {
            Iterator<Quad> quads = ds.asDatasetGraph().find();
            long size = 0;
            while (quads.hasNext()) {
                quads.next(); size += 1;
            }
            return newFixnum(ctx.runtime, size);
        });
    }

    @JRubyMethod(name = {"graph_count", "graph_size"})
    public RubyFixnum graphSize(ThreadContext ctx) {
        return executeInTransaction(ds, ReadWrite.READ, ds -> newFixnum(ctx.runtime, ds.asDatasetGraph().size()));
    }

    @JRubyMethod(name = {"each_statement", "each"})
    public IRubyObject iterateStatements(ThreadContext ctx, Block block) {
        if (block != Block.NULL_BLOCK) {
            executeInTransaction(ds, ReadWrite.READ, ds -> {
                Iterator<Quad> quads = ds.asDatasetGraph().find();
                while (quads.hasNext()) {
                    block.call(ctx, convertQuad(ctx, quads.next()));
                }
                return null;
            });
            return ctx.nil;
        } else {
            return callMethod(ctx, "enum_statement");
        }
    }

    @JRubyMethod(name = {"query_pattern"}, required = 1, optional = 1)
    public IRubyObject queryPattern(ThreadContext ctx, IRubyObject[] args, Block block) {
        if (block != Block.NULL_BLOCK) {
            IRubyObject statements = args[0];

            // argument 1; unused options

            executeInTransaction(ds, ReadWrite.READ, ds -> {
                Node[] nodePattern = convertRDFPattern(ctx, statements);
                DatasetGraph dg    = ds.asDatasetGraph();
                Iterator<Quad> quads;
                if (nodePattern == null) {
                    quads = dg.find();
                } else {
                    if (nodePattern[0] == null) {
                        nodePattern[0] = NodeFactory.createURI("urn:x-arq:DefaultGraph");
                    }
                    quads = dg.find(nodePattern[0], nodePattern[1], nodePattern[2], nodePattern[3]);
                }
                while (quads.hasNext()) {
                    block.call(ctx, convertQuad(ctx, quads.next()));
                }
                return null;
            });
            return ctx.nil;
        } else {
            IRubyObject[] enumArgs = new IRubyObject[args.length+1];
            enumArgs[0] = newSymbol(ctx.runtime, "query_pattern");
            System.arraycopy(args, 0, enumArgs, 1, args.length);
            return callMethod(ctx, "enum_for", enumArgs);
        }
    }

    @JRubyMethod(name = {"query_execute"}, required = 1, argTypes = {RubyString.class})
    public IRubyObject queryExecute(ThreadContext ctx, RubyString sparqlQuery, Block block) {
        if (block != Block.NULL_BLOCK) {
            executeInTransaction(ds, ReadWrite.READ, ds -> {
                Query query   = QueryFactory.create(sparqlQuery.asString().asJavaString());

                Model defaultModel = ds.getDefaultModel();
                try (QueryExecution qexec = QueryExecutionFactory.create(query, defaultModel)) {
                    ResultSet resultSet = qexec.execSelect();
                    while (resultSet.hasNext()) {
                        QuerySolution solution = resultSet.next();
                        Map<RubyString, IRubyObject> solutionVars = new HashMap<>();
                        Iterator<String> variableIterator = solution.varNames();
                        while (variableIterator.hasNext()) {
                            String variable = variableIterator.next();
                            solutionVars.put(ctx.runtime.newString(variable), convertNode(ctx, solution.get(variable)));
                        }
                        block.call(ctx, RubyHash.newHash(ctx.runtime, solutionVars, ctx.nil));
                    }
                }
                return null;
            });
            return ctx.nil;
        } else {
            IRubyObject[] enumArgs = new IRubyObject[2];
            enumArgs[0] = newSymbol(ctx.runtime, "query_execute");
            enumArgs[1] = sparqlQuery;
            return callMethod(ctx, "enum_for", enumArgs);
        }
    }

    @JRubyMethod(name = "has_statement?", required = 1)
    public RubyBoolean hasStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == null) {
            return newBoolean(ctx.runtime, false);
        }

        return executeInTransaction(ds, ReadWrite.READ, ds -> {
            Node[] nodePattern = convertRDFPattern(ctx, rdfStatement);
            if (nodePattern == null) {
                return newBoolean(ctx.runtime, false);
            }

            if (nodePattern[0] == null) {
                nodePattern[0] = NodeFactory.createURI("urn:x-arq:DefaultGraph");
            }
            return newBoolean(ctx.runtime, ds.asDatasetGraph().contains(
                    nodePattern[0], //graph
                    nodePattern[1], //subject
                    nodePattern[2], //predicate
                    nodePattern[3]  //object
            ));
        });
    }

    @JRubyMethod(name = "insert_statement", required = 1)
    public IRubyObject insertStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == null) {
            return newBoolean(ctx.runtime, false);
        }

        return executeInTransaction(ds, ReadWrite.WRITE, ds -> {
            Quad quad = convertRDFQuad(ctx, rdfStatement);

            if (quad == null) {
                return newBoolean(ctx.runtime, false);
            }

            DatasetGraph dg = ds.asDatasetGraph();
            if (quad.getGraph() == null) {
                Node defaultGraphNode = NodeFactory.createURI("urn:x-arq:DefaultGraph");
                Graph defaultGraph    = dg.getGraph(defaultGraphNode);
                Triple triple         = quad.asTriple();
                if (defaultGraph.contains(triple)) {
                    return newBoolean(ctx.runtime, false);
                }
                defaultGraph.add(quad.asTriple());
            } else {
                if (dg.contains(quad)) {
                    return newBoolean(ctx.runtime, false);
                }
                dg.add(quad);
            }

            return newBoolean(ctx.runtime, true);
        });
    }

    @JRubyMethod(name = "insert_statements", required = 1)
    public IRubyObject insertStatements(ThreadContext ctx, IRubyObject rdfStatements) {
        if (rdfStatements == null) {
            return newBoolean(ctx.runtime, false);
        }

        return executeInTransaction(ds, ReadWrite.WRITE, ds -> {
            DatasetGraph   dg                  = ds.asDatasetGraph();
            RubyEnumerator statementEnumerator = (RubyEnumerator) rdfStatements.callMethod(ctx, "each");
            try {
                while (true) {
                    RubyArray array = statementEnumerator.next(ctx).convertToArray();
                    Node subject   = Optional.ofNullable(convertRDFResource(ctx, array.entry(0))).map(RDFNode::asNode).orElse(null);
                    Node predicate = Optional.ofNullable(convertRDFResource(ctx, array.entry(1))).map(RDFNode::asNode).orElse(null);
                    Node object    = Optional.ofNullable(convertRDFTerm(ctx, array.entry(2))).map(RDFNode::asNode).orElse(null);
                    Node graph     = Optional.ofNullable(convertRDFResource(ctx, array.entry(3))).map(RDFNode::asNode).orElse(null);

                    if (graph == null) {
                        graph = NodeFactory.createURI("urn:x-arq:DefaultGraph");
                    }
                    dg.add(new Quad(graph, subject, predicate, object));
                }
            } catch (RaiseException ex) {
                // Handle StopIteration as a terminator for iterating a Ruby Enumerator.
                // All other exceptions are rethrown.
                if (ex.getException().getMetaClass() == ctx.runtime.getStopIteration()) {
                    // safe to skip
                } else {
                    // need to rethrow, this is truly an exception
                    throw ex;
                }
            }
            return newBoolean(ctx.runtime, true);
        });
    }

    @JRubyMethod(name = "insert_file", required = 1)
    public IRubyObject insertFile(ThreadContext ctx, IRubyObject path) {
        String filePath = path.asString().asJavaString();
        File file       = new File(filePath);

        if (!file.exists() || !file.canRead()) {
            throw ctx.runtime.newArgumentError("path cannot be read");
        }

        return executeInTransaction(ds, ReadWrite.WRITE, ds -> {
            DatasetGraph graph = ds.asDatasetGraph();

            try (InputStream stream = new FileInputStream(file)) {
                // Map from filename's extension; fallback to NQUADS default.
                Lang langHint = RDFLanguages.filenameToLang(file.getName(), Lang.NQUADS);
                RDFDataMgr.read(graph, stream, langHint);
                return newBoolean(ctx.runtime, true);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                throw ctx.runtime.newIOErrorFromException(e);
            } catch (IOException e) {
                throw ctx.runtime.newIOErrorFromException(e);
            }
        });
    }

    @JRubyMethod(name = "insert_reader", required = 1)
    public IRubyObject insertReader(ThreadContext ctx, IRubyObject reader) {
        RubyFile file      = ((RubyFile) reader);
        InputStream stream = file.getInStream();

        return executeInTransaction(ds, ReadWrite.WRITE, ds -> {
            DatasetGraph graph = ds.asDatasetGraph();
            RDFDataMgr.read(graph, stream, Lang.NQUADS);
            return newBoolean(ctx.runtime, true);
        });
    }

    @JRubyMethod(name = "delete_statement", required = 1)
    public IRubyObject deleteStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == null) {
            return ctx.nil;
        }

        return executeInTransaction(ds, ReadWrite.WRITE, ds -> {
            DatasetGraph dg = ds.asDatasetGraph();
            Node[] nodePattern = convertRDFPattern(ctx, rdfStatement);
            if (nodePattern == null) {
                return newBoolean(ctx.runtime, false);
            }

            if (nodePattern[0] == null) {
                Graph graph = dg.getDefaultGraph();
                graph.remove(
                        nodePattern[1], //subject
                        nodePattern[2], //predicate
                        nodePattern[3]  //object
                );
            } else {
                dg.deleteAny(
                        nodePattern[0], //graph
                        nodePattern[1], //subject
                        nodePattern[2], //predicate
                        nodePattern[3]  //object
                );
            }
            return newBoolean(ctx.runtime, true);
        });
    }

    @JRubyMethod(name = "clear_statements")
    public IRubyObject clearStatements(ThreadContext ctx) {
        executeInTransaction(ds, ReadWrite.WRITE, ds -> {
            ds.asDatasetGraph().clear();
            return null;
        });
        return ctx.nil;
    }

    @JRubyMethod(name = "graph", required = 1)
    public IRubyObject getGraph(ThreadContext ctx, IRubyObject graphName) {
        RubyClass rubyGraphClass = findClass(ctx, "Graph");
        com.github.rdf_jena.Graph.Allocator.allocate(ctx.runtime, rubyGraphClass);

        return executeInTransaction(ds, ReadWrite.READ, ds -> {
            DatasetGraph dg = ds.asDatasetGraph();
            RubySymbol defaultGraph = newSymbol(ctx.runtime, "default");
            if (graphName.isNil() || graphName == defaultGraph) {
                return rubyGraphClass.newInstance(ctx, defaultGraph, this, Block.NULL_BLOCK);
            } else {
                String javaGraphName = graphName.asString().asJavaString();
                if (!dg.containsGraph(NodeFactory.createURI(javaGraphName))) {
                    return ctx.nil;
                }
                return rubyGraphClass.newInstance(ctx, graphName, this, Block.NULL_BLOCK);
            }
        });
    }

    @JRubyMethod(name = "each_graph")
    public IRubyObject iterateGraphs(ThreadContext ctx, Block block) {
        if (block != Block.NULL_BLOCK) {
            RubyClass rubyGraphClass = findClass(ctx, "Graph");
            com.github.rdf_jena.Graph.Allocator.allocate(ctx.runtime, rubyGraphClass);

            executeInTransaction(ds, ReadWrite.READ, ds -> {
                DatasetGraph dg = ds.asDatasetGraph();
                Iterator<Node> graphNodes = dg.listGraphNodes();
                while (graphNodes.hasNext()) {
                    // Create new RDF::Jena::Graph ruby object.
                    RubyString rubyGraphName = newString(ctx.runtime, graphNodes.next().getURI());
                    IRubyObject rubyGraph = rubyGraphClass.newInstance(ctx,
                            rubyGraphName,
                            this,
                            Block.NULL_BLOCK);

                    // Yield the RDF::Jena::Graph to the block.
                    block.call(ctx, rubyGraph);
                }
                return null;
            });
            return ctx.nil;
        } else {
            return this.callMethod(ctx, "enum_graph");
        }
    }

    @JRubyMethod(name = "insert_graph", required = 1)
    public IRubyObject insertGraph(ThreadContext ctx, IRubyObject rubyGraph) {
        if (!rubyGraph.respondsTo("graph_name")) {
            throw ctx.runtime.newArgumentError("graph does not provide graph_name");
        }
        if (!rubyGraph.respondsTo("data")) {
            throw ctx.runtime.newArgumentError("graph does not provide data");
        }

        executeInTransaction(ds, ReadWrite.WRITE, (Dataset ds) -> {
            DatasetGraph dg = ds.asDatasetGraph();
            IRubyObject graphName = rubyGraph.callMethod(ctx, "graph_name");
            String javaGraphName  = graphName.isNil() ? null : graphName.asJavaString();

            // Error if graph already exists.
            Node graphNode = NodeFactory.createURI(javaGraphName);
            if (dg.containsGraph(graphNode)) {
                throw ctx.runtime.newRuntimeError("cannot insert because graph already exists for '" + javaGraphName + "'");
            }

            // Insert into new memory graph.
            Graph insertGraph = GraphFactory.createDefaultGraph();
            RubyEnumerator statementEnumerator = (RubyEnumerator) rubyGraph.callMethod(ctx, "data").callMethod(ctx, "each_statement");
            insertIntoGraph(ctx, statementEnumerator, insertGraph);

            // Add memory graph to Dataset Graph (backed by TDB).
            dg.addGraph(graphNode, insertGraph);
            return null;
        });

        return rubyGraph;
    }

    @JRubyMethod(name = "replace_graph", required = 1)
    public IRubyObject replaceGraph(ThreadContext ctx, IRubyObject rubyGraph) {
        if (!rubyGraph.respondsTo("graph_name")) {
            throw ctx.runtime.newArgumentError("graph does not provide graph_name");
        }
        if (!rubyGraph.respondsTo("data")) {
            throw ctx.runtime.newArgumentError("graph does not provide data");
        }

        executeInTransaction(ds, ReadWrite.WRITE, (Dataset ds) -> {
            DatasetGraph dg       = ds.asDatasetGraph();
            IRubyObject graphName = rubyGraph.callMethod(ctx, "graph_name");
            String javaGraphName  = graphName.isNil() ? null : graphName.asJavaString();

            // Error if graph does not exists.
            Node graphNode = NodeFactory.createURI(javaGraphName);
            if (!dg.containsGraph(graphNode)) {
                throw ctx.runtime.newRuntimeError("cannot replace because graph does not exist for '" + javaGraphName + "'");
            }

            Graph graph                        = dg.getGraph(NodeFactory.createURI(javaGraphName));
            RubyEnumerator statementEnumerator = (RubyEnumerator) rubyGraph.callMethod(ctx, "data").callMethod(ctx, "each_statement");
            graph.clear();
            insertIntoGraph(ctx, statementEnumerator, graph);
            return null;
        });

        return rubyGraph;
    }

    @JRubyMethod(name = "delete_graph", required = 1)
    public IRubyObject deleteGraph(ThreadContext ctx, IRubyObject rubyGraph) {
        if (!rubyGraph.respondsTo("graph_name")) {
            throw ctx.runtime.newArgumentError("graph does not provide graph_name");
        }
        if (!rubyGraph.respondsTo("data")) {
            throw ctx.runtime.newArgumentError("graph does not provide data");
        }

        executeInTransaction(ds, ReadWrite.WRITE, (Dataset ds) -> {
            DatasetGraph dg       = ds.asDatasetGraph();
            IRubyObject graphName = rubyGraph.callMethod(ctx, "graph_name");
            String javaGraphName  = graphName.isNil() ? null : graphName.asJavaString();
            Node graphNode        = NodeFactory.createURI(javaGraphName);

            // Error if graph does not exists.
            if (!dg.containsGraph(graphNode)) {
                throw ctx.runtime.newRuntimeError("cannot delete because graph does not exist for '" + javaGraphName + "'");
            }

            dg.removeGraph(graphNode);
            return null;
        });

        return rubyGraph;
    }

    private void insertIntoGraph(ThreadContext ctx, RubyEnumerator statementEnumerator, Graph graph) {
        try {
            while (true) {
                graph.add(convertRDFTriple(ctx, statementEnumerator.next(ctx)));
            }
        } catch (RaiseException ex) {
            // Handle StopIteration as a terminator for iterating a Ruby Enumerator.
            // All other exceptions are rethrown.
            if (ex.getException().getMetaClass() == ctx.runtime.getStopIteration()) {
                // safe to skip
            } else {
                // need to rethrow, this is truly an exception
                throw ex;
            }
        }
    }
}
