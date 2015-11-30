package com.github.rdf_jena;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.jruby.*;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.Block;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import static com.github.rdf_jena.JenaConverters.convertRDFStatement;
import static com.github.rdf_jena.RubyRDFConverters.convertStatement;
import static org.jruby.RubyBoolean.newBoolean;
import static org.jruby.RubyFixnum.newFixnum;

/**
 * Collaborator for {@link Repository} and {@link Graph}.
 */
public class RepositoryModel {

    private final IRubyObject self;
    private final Model model;

    public RepositoryModel(IRubyObject self, Model model) {
        this.self  = self;
        this.model = model;
    }

    @JRubyMethod(name = "durable?")
    public RubyBoolean isDurable(ThreadContext ctx) {
        return newBoolean(ctx.runtime, true);
    }

    @JRubyMethod(name = "empty?")
    public RubyBoolean isEmpty(ThreadContext ctx) {
        return newBoolean(ctx.runtime, model.isEmpty());
    }

    @JRubyMethod(name = "count", alias = {"size"})
    public RubyFixnum count(ThreadContext ctx) {
        return newFixnum(ctx.runtime, model.size());
    }

    @JRubyMethod(name = "each_statement")
    public IRubyObject iterateStatements(ThreadContext ctx, Block block) {
        if (block != Block.NULL_BLOCK) {
            StmtIterator statements = model.listStatements();
            while (statements.hasNext()) {
                Statement statement = statements.nextStatement();
                block.call(ctx, convertStatement(ctx, statement));
            }
            return ctx.nil;
        } else {
            return self.callMethod(ctx, "enum_statement");
        }
    }

    @JRubyMethod(name = "has_statement?", required = 1)
    public RubyBoolean hasStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == null) {
            return newBoolean(ctx.runtime, false);
        }

        Statement statement = convertRDFStatement(ctx, rdfStatement, model);
        return newBoolean(ctx.runtime, model.contains(statement));
    }

    @JRubyMethod(name = "insert_statement", required = 1)
    public IRubyObject insertStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == null) {
            return ctx.nil;
        }

        Statement statement = convertRDFStatement(ctx, rdfStatement, model);
        if (!model.contains(statement)) {
            model.begin();
            model.add(statement);
            model.commit();
        }

        return ctx.nil;
    }

    @JRubyMethod(name = "delete_statement", required = 1)
    public IRubyObject deleteStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == null) {
            return ctx.nil;
        }

        model.remove(convertRDFStatement(ctx, rdfStatement, model));
        return ctx.nil;
    }

    @JRubyMethod(name = "clear_statements")
    public IRubyObject clearStatements(ThreadContext ctx) {
        model.removeAll();
        return ctx.nil;
    }
}
