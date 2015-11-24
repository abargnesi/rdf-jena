package com.github.rdf_jena;

import org.apache.jena.rdf.model.*;
import org.jruby.*;
import org.jruby.runtime.Block;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import java.util.HashMap;
import java.util.Map;

public class RubyRDFConverters {

    public static final RubyClass  RDF_Statement;
    public static final RubyClass  RDF_Term;
    public static final RubyModule RDF_Resource;
    public static final RubyClass  RDF_Node;
    public static final RubyClass  RDF_URI;
    public static final RubyClass  RDF_Literal;

    static {
        Ruby ruby            = Ruby.getGlobalRuntime();
        RubyModule rdfModule = ruby.getModule("RDF");

        if (rdfModule == null) {
            throw ruby.newRaiseException(ruby.getClass("RuntimeError"), "The module RDF is missing.");
        }

        RDF_Statement = rdfModule.getClass("Statement");
        RDF_Term      = rdfModule.getClass("Term");
        RDF_Resource  = (RubyModule) rdfModule.getConstant("Resource");
        RDF_Node      = rdfModule.getClass("Node");
        RDF_URI       = rdfModule.getClass("URI");
        RDF_Literal   = rdfModule.getClass("Literal");
    }


    public static IRubyObject convertStatement(ThreadContext ctx, Statement statement) {
        Map<RubySymbol, IRubyObject> options = new HashMap<>();
        options.put(RubySymbol.newSymbol(ctx.runtime, "subject"), convertResource(ctx, statement.getSubject()));
        options.put(RubySymbol.newSymbol(ctx.runtime, "predicate"), convertProperty(ctx, statement.getPredicate()));
        options.put(RubySymbol.newSymbol(ctx.runtime, "object"), convertNode(ctx, statement.getObject()));
        RubyHash optionsHash = RubyHash.newHash(ctx.runtime, options, ctx.nil);

        return RDF_Statement.newInstance(ctx, optionsHash, Block.NULL_BLOCK);
    }

    public static IRubyObject convertResource(ThreadContext ctx, Resource resource) {
        IRubyObject rdfSubject;

        if (resource.isAnon()) {
            AnonId id = resource.getId();
            rdfSubject = RubySymbol.newSymbol(ctx.runtime, id.getLabelString());
        } else {
            rdfSubject = RubyString.newString(ctx.runtime, resource.getURI());
        }
        return RDF_Resource.send(ctx, RubySymbol.newSymbol(ctx.runtime, "new"), rdfSubject, Block.NULL_BLOCK);
    }

    public static IRubyObject convertProperty(ThreadContext ctx, Property property) {
        return RDF_URI.newInstance(ctx, RubyString.newString(ctx.runtime, property.getURI()), Block.NULL_BLOCK);
    }

    public static IRubyObject convertNode(ThreadContext ctx, RDFNode node) {
        if (node.isResource()) {
            return convertResource(ctx, node.asResource());
        }
        return convertLiteral(ctx, node.asLiteral());
    }

    public static IRubyObject convertLanguage(ThreadContext ctx, String language) {
        if (!language.isEmpty()) {
            return RubySymbol.newSymbol(ctx.runtime, language);
        }
        return ctx.nil;
    }

    public static IRubyObject convertDatatype(ThreadContext ctx, String datatypeURI) {
        if (datatypeURI != null) {
            return RDF_URI.newInstance(ctx, RubyString.newString(ctx.runtime, datatypeURI), Block.NULL_BLOCK);
        }
        return ctx.nil;
    }

    public static IRubyObject convertLiteral(ThreadContext ctx, Literal literal) {
        Map<RubySymbol, IRubyObject> options = new HashMap<>();
        options.put(RubySymbol.newSymbol(ctx.runtime, "language"), convertLanguage(ctx, literal.getLanguage()));
        options.put(RubySymbol.newSymbol(ctx.runtime, "datatype"), convertDatatype(ctx, literal.getDatatypeURI()));

        String value = literal.getLexicalForm();
        RubyHash optionsHash = RubyHash.newHash(ctx.runtime, options, ctx.nil);
        return RDF_Literal.newInstance(ctx, RubyString.newString(ctx.runtime, value), optionsHash, Block.NULL_BLOCK);
    }
}
