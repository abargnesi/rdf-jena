package com.github.rdf_jena;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.jruby.*;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import static com.github.rdf_jena.TransactionUtil.executeInTransaction;

@JRubyClass(name = "Graph")
public class Graph extends RubyObject {

    public static ObjectAllocator Allocator = Graph::new;

    // ruby state
    protected RubyString graphName;

    // java state
    protected Repository repository;
    protected RepositoryModel repositoryModel;

    public Graph(Ruby runtime, RubyClass metaclass) {
        super(runtime, metaclass);
    }

    // Ruby methods
    @JRubyMethod(name = "initialize", required = 2)
    public IRubyObject initialize(
            ThreadContext context,
            IRubyObject graphName,
            IRubyObject repository
    ) {
        this.graphName       = graphName.asString();
        this.repository      = (Repository) repository.toJava(Repository.class);

        // Get/Create named model from the provided dataset.
        Dataset ds = this.repository.dataset;

        Model model = executeInTransaction(ds, ReadWrite.READ, t -> ds.getNamedModel(graphName.asJavaString()));

        // Set up RepositoryModel using model/dataset.
        this.repositoryModel = new RepositoryModel(this, model, ds);

        return context.nil;
    }

    @JRubyMethod(name = "graph_name")
    public IRubyObject getGraphName(ThreadContext ctx) {
        if (graphName == null) {
            return ctx.nil;
        }

        return graphName;
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
}
