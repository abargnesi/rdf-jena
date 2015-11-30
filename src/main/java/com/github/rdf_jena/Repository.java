package com.github.rdf_jena;

import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.*;
import org.apache.jena.tdb.TDBFactory;
import org.jruby.*;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import java.util.Iterator;

import static org.jruby.RubyString.newString;

@JRubyClass(name = "Repository")
public class Repository extends RubyObject {

    public static ObjectAllocator Allocator = Repository::new;

    /**
     * Jena {@link Dataset} backed by a TDB database. This state is should be
     * used as final within this Ruby Object.
     */
    protected Dataset         dataset;
    protected RepositoryModel repositoryModel;

    public Repository(Ruby runtime, RubyClass metaclass) {
        super(runtime, metaclass);
    }

    @JRubyMethod(name = "initialize", required = 1)
    public IRubyObject initialize(
            ThreadContext context,
            IRubyObject datasetDirectory
    ) {
        dataset         = TDBFactory.createDataset(datasetDirectory.asJavaString());
        repositoryModel = new RepositoryModel(this, dataset.getDefaultModel());
        return context.nil;
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
            RubyClass rubyGraphClass = JenaRepositoryService.findClass(ctx, "Graph");
            Graph.Allocator.allocate(ctx.runtime, rubyGraphClass);

            Iterator<String> names = dataset.listNames();
            while (names.hasNext()) {
                String graphName         = names.next();
                Model graphModel         = dataset.getNamedModel(graphName);

                // Create new RDF::Jena::Graph ruby object.
                RubyString rubyGraphName = newString(ctx.runtime, graphName);
                IRubyObject rubyGraph    = rubyGraphClass.newInstance(ctx, rubyGraphName, Block.NULL_BLOCK);

                // Set Jena Model for graph by accessing Java object.
                Graph javaGraph          = (Graph) rubyGraph.toJava(Graph.class);
                javaGraph.setRepositoryModel(new RepositoryModel(rubyGraph, graphModel));

                // Yield the RDF::Jena::Graph to the block.
                block.call(ctx, rubyGraph);
            }
            return ctx.nil;
        } else {
            return this.callMethod(ctx, "enum_graph");
        }
    }

    // TODO: Implement insert_graph
}
