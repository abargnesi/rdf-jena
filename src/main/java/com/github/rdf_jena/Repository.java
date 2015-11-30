package com.github.rdf_jena;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.*;
import org.apache.jena.tdb.TDBFactory;
import org.jruby.*;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.exceptions.RaiseException;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import java.util.Iterator;

import static com.github.rdf_jena.JenaConverters.convertRDFStatement;
import static com.github.rdf_jena.JenaRepositoryService.findClass;
import static com.github.rdf_jena.TransactionUtil.executeInTransaction;
import static org.jruby.RubyBoolean.newBoolean;
import static org.jruby.RubyString.newString;
import static org.jruby.RubySymbol.newSymbol;

@JRubyClass(name = "Repository")
public class Repository extends RubyObject {

    public static ObjectAllocator Allocator = Repository::new;

    /**
     * Jena {@link Dataset} backed by a TDB database. This state is should be
     * used as final within this Ruby Object.
     */
    protected Dataset         dataset;
    protected RepositoryModel repositoryModel;
    protected boolean         unionEachNgWithDefault;

    public Repository(Ruby runtime, RubyClass metaclass) {
        super(runtime, metaclass);
    }

    @JRubyMethod(name = "initialize", required = 1, optional = 1)
    public IRubyObject initialize(
            ThreadContext ctx,
            IRubyObject[] args
    ) {
        String datasetDirectory = args[0].asJavaString();
        Ruby ruby = ctx.runtime;
        RubyHash options = (args.length <= 1) ? RubyHash.newHash(ruby) : args[1].convertToHash();
        Boolean unionEachNgWithDefault = (Boolean) options.get(newSymbol(ruby, "union_each_ng_with_default"));
        this.unionEachNgWithDefault = unionEachNgWithDefault != null && unionEachNgWithDefault;

        dataset         = TDBFactory.createDataset(datasetDirectory);

        Model defaultModel = executeInTransaction(dataset, ReadWrite.READ, ds -> dataset.getDefaultModel());
        repositoryModel = new RepositoryModel(this, defaultModel, dataset);
        return ctx.nil;
    }

    /**
     * Delegated. See {@link RepositoryModel#isDurable(ThreadContext)}.
     */
    @JRubyMethod(name = "durable?")
    public RubyBoolean isDurable(ThreadContext ctx) {
        return repositoryModel.isDurable(ctx);
    }

    /**
     * Delegated. See {@link RepositoryModel#isEmpty(ThreadContext)}.
     */
    @JRubyMethod(name = "empty?")
    public RubyBoolean isEmpty(ThreadContext ctx) {
        return repositoryModel.isEmpty(ctx);
    }

    /**
     * Delegated. See {@link RepositoryModel#count(ThreadContext)}.
     */
    @JRubyMethod(name = "count", alias = {"size"})
    public RubyFixnum count(ThreadContext ctx) {
        return repositoryModel.count(ctx);
    }

    /**
     * Delegated. See {@link RepositoryModel#iterateStatements(ThreadContext, Block)}.
     */
    @JRubyMethod(name = "each_statement")
    public IRubyObject iterateStatements(ThreadContext ctx, Block block) {
        return repositoryModel.iterateStatements(ctx, block);
    }

    /**
     * Delegated. See {@link RepositoryModel#hasStatement(ThreadContext, IRubyObject)}.
     */
    @JRubyMethod(name = "has_statement?", required = 1)
    public RubyBoolean hasStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        return repositoryModel.hasStatement(ctx, rdfStatement);
    }

    /**
     * Delegated. See {@link RepositoryModel#insertStatement(ThreadContext, IRubyObject)}.
     */
    @JRubyMethod(name = "insert_statement", required = 1)
    public IRubyObject insertStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        return repositoryModel.insertStatement(ctx, rdfStatement);
    }

    /**
     * Delegated. See {@link RepositoryModel#deleteStatement(ThreadContext, IRubyObject)}.
     */
    @JRubyMethod(name = "delete_statement", required = 1)
    public IRubyObject deleteStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        return repositoryModel.deleteStatement(ctx, rdfStatement);
    }

    /**
     * Delegated. See {@link RepositoryModel#clearStatements(ThreadContext)}.
     */
    @JRubyMethod(name = "clear_statements")
    public IRubyObject clearStatements(ThreadContext ctx) {
        return repositoryModel.clearStatements(ctx);
    }

    @JRubyMethod(name = "each_graph")
    public IRubyObject iterateGraphs(ThreadContext ctx, Block block) {
        if (block != Block.NULL_BLOCK) {
            RubyClass rubyGraphClass = findClass(ctx, "Graph");
            Graph.Allocator.allocate(ctx.runtime, rubyGraphClass);

            dataset.begin(ReadWrite.READ);
            try {
                Iterator<String> names = dataset.listNames();
                while (names.hasNext()) {
                    // Create new RDF::Jena::Graph ruby object.
                    RubyString rubyGraphName = newString(ctx.runtime, names.next());
                    IRubyObject rubyGraph = rubyGraphClass.newInstance(ctx,
                            rubyGraphName,
                            this,
                            newBoolean(ctx.runtime, unionEachNgWithDefault),
                            Block.NULL_BLOCK);

                    // Yield the RDF::Jena::Graph to the block.
                    block.call(ctx, rubyGraph);
                }
            } finally {
                dataset.end();
            }
            return ctx.nil;
        } else {
            return this.callMethod(ctx, "enum_graph");
        }
    }

    @JRubyMethod(name = "insert_graph", required = 1)
    public IRubyObject insertGraph(ThreadContext ctx, IRubyObject graph) {
        if (!graph.respondsTo("graph_name")) {
            throw ctx.runtime.newArgumentError("graph does not provide graph_name");
        }
        if (!graph.respondsTo("data")) {
            throw ctx.runtime.newArgumentError("graph does not provide data");
        }

        executeInTransaction(dataset, ReadWrite.WRITE, (Dataset ds) -> {

            IRubyObject graphName = graph.callMethod(ctx, "graph_name");
            RubyEnumerator statementEnumerator = (RubyEnumerator) graph.callMethod(ctx, "data").callMethod(ctx, "each_statement");
            if (graphName.isNil()) {
                insertIntoDefaultGraph(ctx, statementEnumerator);
            } else {
                String javaGraphName = graphName.asJavaString();
                boolean containsModel = ds.containsNamedModel(javaGraphName);
                if (containsModel) {
                    throw ctx.runtime.newRuntimeError("cannot insert because graph already exists for '" + javaGraphName + "'");
                }
                insertIntoNamedGraph(ctx, statementEnumerator, ds.getNamedModel(javaGraphName));
            }
            return null;
        });

        return graph;
    }

    @JRubyMethod(name = "replace_graph", required = 1)
    public IRubyObject replaceGraph(ThreadContext ctx, IRubyObject graph) {
        if (!graph.respondsTo("graph_name")) {
            throw ctx.runtime.newArgumentError("graph does not provide graph_name");
        }
        if (!graph.respondsTo("data")) {
            throw ctx.runtime.newArgumentError("graph does not provide data");
        }

        executeInTransaction(dataset, ReadWrite.WRITE, (Dataset ds) -> {
            IRubyObject graphName = graph.callMethod(ctx, "graph_name");
            RubyEnumerator statementEnumerator = (RubyEnumerator) graph.callMethod(ctx, "data").callMethod(ctx, "each_statement");
            if (graphName.isNil()) {
                clearStatements(ctx);
                insertIntoDefaultGraph(ctx, statementEnumerator);
            } else {
                String javaGraphName = graphName.asJavaString();
                boolean containsModel = ds.containsNamedModel(javaGraphName);
                if (!containsModel) {
                    throw ctx.runtime.newRuntimeError("cannot replace because graph does not exist for '" + javaGraphName + "'");
                }

                ds.removeNamedModel(javaGraphName);
                insertIntoNamedGraph(ctx, statementEnumerator, ds.getNamedModel(javaGraphName));
            }
            return null;
        });

        return graph;
    }

    @JRubyMethod(name = "delete_graph", required = 1)
    public IRubyObject deleteGraph(ThreadContext ctx, IRubyObject graph) {
        if (!graph.respondsTo("graph_name")) {
            throw ctx.runtime.newArgumentError("graph does not provide graph_name");
        }
        if (!graph.respondsTo("data")) {
            throw ctx.runtime.newArgumentError("graph does not provide data");
        }

        executeInTransaction(dataset, ReadWrite.WRITE, (Dataset ds) -> {
            IRubyObject graphName = graph.callMethod(ctx, "graph_name");
            if (graphName.isNil()) {
                clearStatements(ctx);
            } else {
                String javaGraphName = graphName.asJavaString();
                boolean containsModel = ds.containsNamedModel(javaGraphName);
                if (!containsModel) {
                    throw ctx.runtime.newRuntimeError("cannot delete because graph does not exist for '" + javaGraphName + "'");
                }

                ds.removeNamedModel(javaGraphName);
            }
            return null;
        });

        return graph;
    }

    private void insertIntoNamedGraph(ThreadContext ctx, RubyEnumerator statementEnumerator, Model namedGraph) {
        try {
            while (true) {
                Statement stmt = convertRDFStatement(ctx, statementEnumerator.next(ctx), namedGraph);
                namedGraph.add(stmt);
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

    private void insertIntoDefaultGraph(ThreadContext ctx, RubyEnumerator statementEnumerator) {
        try {
            while (true) {
                insertStatement(ctx, statementEnumerator.next(ctx));
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

    // TODO: Provide replace_graph (Not provided by RDF mixins)
    // TODO: Provide delete_graph (Not provided by RDF mixins)
}
