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

import static com.github.rdf_jena.JenaConverters.convertRDFStatement;
import static com.github.rdf_jena.RubyRDFConverters.convertStatement;
import static org.jruby.RubyBoolean.newBoolean;
import static org.jruby.RubyFixnum.newFixnum;

@JRubyClass(name = "Repository")
public class Repository extends RubyObject {

    public static ObjectAllocator Allocator = Repository::new;

    /**
     * Jena {@link Dataset} backed by a TDB database. This state is should be
     * used as final within this Ruby Object.
     */
    public Dataset dataset;

    public Repository(Ruby runtime, RubyClass metaclass) {
        super(runtime, metaclass);
    }

    @JRubyMethod(name = "initialize", required = 1)
    public IRubyObject initialize(
            ThreadContext context,
            IRubyObject datasetDirectory
    ) {
        dataset = TDBFactory.createDataset(datasetDirectory.asJavaString());
        return context.nil;
    }

    @JRubyMethod(name = "durable?")
    public RubyBoolean isDurable(ThreadContext ctx) {
        return newBoolean(ctx.runtime, true);
    }

    @JRubyMethod(name = "empty?")
    public RubyBoolean isEmpty(ThreadContext ctx) {
        boolean empty = dataset.getDefaultModel().isEmpty();
        return newBoolean(ctx.runtime, empty);
    }

    @JRubyMethod(name = "count", alias = {"size"})
    public RubyFixnum count(ThreadContext ctx) {
        long size = dataset.getDefaultModel().size();
        return newFixnum(ctx.runtime, size);
    }

    @JRubyMethod(name = "each_statement")
    public IRubyObject iterateStatements(ThreadContext ctx, Block block) {
        if (block != Block.NULL_BLOCK) {
            StmtIterator statements = dataset.getDefaultModel().listStatements();
            while (statements.hasNext()) {
                Statement statement = statements.nextStatement();
                block.call(ctx, convertStatement(ctx, statement));
            }
            return ctx.nil;
        } else {
            return this.callMethod(ctx, "enum_statement");
        }
    }

    @JRubyMethod(name = "has_statement?", required = 1)
    public RubyBoolean hasStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        Model model = dataset.getDefaultModel();
        Statement statement = convertRDFStatement(ctx, rdfStatement, model);
        return newBoolean(ctx.runtime, model.contains(statement));
    }
}
