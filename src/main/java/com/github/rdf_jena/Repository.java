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

import java.util.HashMap;
import java.util.Map;

import static org.jruby.RubyBoolean.newBoolean;
import static org.jruby.RubyFixnum.newFixnum;

@JRubyClass(name = "Repository")
public class Repository extends RubyObject {

    public static ObjectAllocator Allocator = Repository::new;

    public final RubyClass  RDF_Statement;
    public final RubyClass  RDF_Term;
    public final RubyModule RDF_Resource;
    public final RubyClass  RDF_Node;
    public final RubyClass  RDF_URI;
    public final RubyClass  RDF_Literal;

    /**
     * Jena {@link Dataset} backed by a TDB database. This state is should be
     * used as final within this Ruby Object.
     */
    public Dataset dataset;

    public Repository(Ruby runtime, RubyClass metaclass) {
        super(runtime, metaclass);

        RubyModule rdfModule = runtime.getModule("RDF");
        if (rdfModule == null) {
            throw runtime.newRaiseException(runtime.getClass("RuntimeError"), "RDF");
        }

        RDF_Statement = rdfModule.getClass("Statement");
        RDF_Term      = rdfModule.getClass("Term");
        RDF_Resource  = (RubyModule) rdfModule.getConstant("Resource");
        RDF_Node      = rdfModule.getClass("Node");
        RDF_URI       = rdfModule.getClass("URI");
        RDF_Literal   = rdfModule.getClass("Literal");
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

    private IRubyObject convertStatement(ThreadContext ctx, Statement statement) {
        Map<RubySymbol, IRubyObject> options = new HashMap<>();
        options.put(RubySymbol.newSymbol(ctx.runtime, "subject"), convertResource(ctx, statement.getSubject()));
        options.put(RubySymbol.newSymbol(ctx.runtime, "predicate"), convertProperty(ctx, statement.getPredicate()));
        options.put(RubySymbol.newSymbol(ctx.runtime, "object"), convertNode(ctx, statement.getObject()));
        RubyHash optionsHash = RubyHash.newHash(ctx.runtime, options, ctx.nil);

        return RDF_Statement.newInstance(ctx, optionsHash, Block.NULL_BLOCK);
    }

    private IRubyObject convertResource(ThreadContext ctx, Resource resource) {
        IRubyObject rdfSubject;

        if (resource.isAnon()) {
            AnonId id = resource.getId();
            rdfSubject = RubySymbol.newSymbol(ctx.runtime, id.getLabelString());
        } else {
            rdfSubject = RubyString.newString(ctx.runtime, resource.getURI());
        }
        return RDF_Resource.send(ctx, RubySymbol.newSymbol(ctx.runtime, "new"), rdfSubject, Block.NULL_BLOCK);
    }

    private IRubyObject convertProperty(ThreadContext ctx, Property property) {
        return RDF_URI.newInstance(ctx, RubyString.newString(ctx.runtime, property.getURI()), Block.NULL_BLOCK);
    }

    private IRubyObject convertNode(ThreadContext ctx, RDFNode node) {
        if (node.isResource()) {
            return convertResource(ctx, node.asResource());
        }
        return convertLiteral(ctx, node.asLiteral());
    }

    private IRubyObject convertLanguage(ThreadContext ctx, String language) {
        if (!language.isEmpty()) {
            return RubySymbol.newSymbol(ctx.runtime, language);
        }
        return ctx.nil;
    }

    private IRubyObject convertDatatype(ThreadContext ctx, String datatypeURI) {
        if (datatypeURI != null) {
            return RDF_URI.newInstance(ctx, RubyString.newString(ctx.runtime, datatypeURI), Block.NULL_BLOCK);
        }
        return ctx.nil;
    }

    private IRubyObject convertLiteral(ThreadContext ctx, Literal literal) {
        Map<RubySymbol, IRubyObject> options = new HashMap<>();
        options.put(RubySymbol.newSymbol(ctx.runtime, "language"), convertLanguage(ctx, literal.getLanguage()));
        options.put(RubySymbol.newSymbol(ctx.runtime, "datatype"), convertDatatype(ctx, literal.getDatatypeURI()));

        String value = literal.getLexicalForm();
        RubyHash optionsHash = RubyHash.newHash(ctx.runtime, options, ctx.nil);
        return RDF_Literal.newInstance(ctx, RubyString.newString(ctx.runtime, value), optionsHash, Block.NULL_BLOCK);
    }
}
