package com.github.rdf_jena;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.jruby.*;
import org.jruby.runtime.Block;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import static com.github.rdf_jena.JenaConverters.convertRDFStatement;
import static com.github.rdf_jena.RubyRDFConverters.convertStatement;
import static com.github.rdf_jena.TransactionUtil.executeInTransaction;
import static org.jruby.RubyBoolean.newBoolean;
import static org.jruby.RubyFixnum.newFixnum;

/**
 * Collaborator for {@link Repository} and {@link Graph}.
 */
public class RepositoryModel {

    protected final IRubyObject self;
    protected final Dataset dataset;
    protected final String namedModelURI;
    protected final boolean unionWithDefault;

    public RepositoryModel(IRubyObject self, Dataset dataset, String namedModelURI, boolean unionWithDefault) {
        this.self             = self;
        this.namedModelURI    = namedModelURI;
        this.dataset          = dataset;
        this.unionWithDefault = unionWithDefault;
    }

    public RubyBoolean isDurable(ThreadContext ctx) {
        return newBoolean(ctx.runtime, true);
    }

    public RubyBoolean isEmpty(ThreadContext ctx) {
        return executeInTransaction(dataset, ReadWrite.READ, ds -> newBoolean(ctx.runtime, getModel(ds).isEmpty()));
    }

    public RubyFixnum count(ThreadContext ctx) {
        return executeInTransaction(dataset, ReadWrite.READ, ds -> newFixnum(ctx.runtime, getModel(ds).size()));
    }

    public IRubyObject iterateStatements(ThreadContext ctx, Block block) {
        if (block != Block.NULL_BLOCK) {
            executeInTransaction(dataset, ReadWrite.READ, ds -> {
                StmtIterator statements = getModel(ds).listStatements();
                while (statements.hasNext()) {
                    Statement statement = statements.nextStatement();
                    block.call(ctx, convertStatement(ctx, statement));
                }
                return null;
            });
            return ctx.nil;
        } else {
            return self.callMethod(ctx, "enum_statement");
        }
    }

    public RubyBoolean hasStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == null) {
            return newBoolean(ctx.runtime, false);
        }

        return executeInTransaction(dataset, ReadWrite.READ, ds -> {
            Model model         = getModel(ds);
            Statement statement = convertRDFStatement(ctx, rdfStatement, model);
            return newBoolean(ctx.runtime, model.contains(statement));
        });
    }

    public RubyBoolean insertStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == null) {
            return newBoolean(ctx.runtime, false);
        }

        return executeInTransaction(dataset, ReadWrite.WRITE, ds -> {
            Model model               = getModel(ds);
            Statement statement       = convertRDFStatement(ctx, rdfStatement, model);
            boolean containsStatement = model.contains(statement);
            if (containsStatement) {
                return newBoolean(ctx.runtime, false);
            }

            model.add(statement);
            return newBoolean(ctx.runtime, true);
        });
    }

    public IRubyObject deleteStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == null) {
            return ctx.nil;
        }

        executeInTransaction(dataset, ReadWrite.WRITE, ds -> {
            Model model = getModel(ds);
            return model.remove(convertRDFStatement(ctx, rdfStatement, model));
        });
        return ctx.nil;
    }

    public IRubyObject clearStatements(ThreadContext ctx) {
        executeInTransaction(dataset, ReadWrite.WRITE, ds -> {
            Model model = getModel(ds);
            return model.removeAll();
        });
        return ctx.nil;
    }

    protected Model getModel(Dataset dataset) {
        if (namedModelURI == null) {
            return dataset.getDefaultModel();
        } else {
            Model namedModel = dataset.getNamedModel(namedModelURI);
            return unionWithDefault ?
                namedModel.union(dataset.getDefaultModel()) :
                namedModel;
        }
    }

    // TODO: Implement insert_statements
    // TODO: Implement delete_statement
    // TODO: Implement delete_statements
    // TODO: Implement begin_transaction, rollback_transaction, commit_transaction
}
