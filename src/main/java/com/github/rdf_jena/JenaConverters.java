package com.github.rdf_jena;

import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.*;
import org.apache.jena.sparql.core.Quad;
import org.jruby.RubyBoolean;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import java.util.Optional;

public class JenaConverters {

    static Model MODEL = ModelFactory.createDefaultModel();

    public static Statement convertRDFStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == ctx.nil || !rdfStatement.respondsTo("subject") ||
                !rdfStatement.respondsTo("predicate") || !rdfStatement.respondsTo("object")) {
            return null;
        }

        Resource subject   = convertRDFResource(ctx, rdfStatement.callMethod(ctx, "subject"));
        Property predicate = convertRDFProperty(ctx, rdfStatement.callMethod(ctx, "predicate"));
        RDFNode  object    = convertRDFTerm(ctx,     rdfStatement.callMethod(ctx, "object"));

        return MODEL.createStatement(subject, predicate, object);
    }

    public static Quad convertRDFQuad(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == ctx.nil || !rdfStatement.respondsTo("subject") ||
                !rdfStatement.respondsTo("predicate") || !rdfStatement.respondsTo("object")) {
            return null;
        }

        Resource subject   = convertRDFResource(ctx, rdfStatement.callMethod(ctx, "subject"));
        Property predicate = convertRDFProperty(ctx, rdfStatement.callMethod(ctx, "predicate"));
        RDFNode  object    = convertRDFTerm(ctx,     rdfStatement.callMethod(ctx, "object"));
        Resource graphName = convertRDFResource(ctx, rdfStatement.callMethod(ctx, "graph_name"));

        return new Quad(
                Optional.ofNullable(graphName).map(RDFNode::asNode).orElse(null),
                Optional.ofNullable(subject).map(RDFNode::asNode).orElse(null),
                Optional.ofNullable(predicate).map(RDFNode::asNode).orElse(null),
                Optional.ofNullable(object).map(RDFNode::asNode).orElse(null)
        );
    }

    public static Triple convertRDFTriple(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == ctx.nil || !rdfStatement.respondsTo("subject") ||
                !rdfStatement.respondsTo("predicate") || !rdfStatement.respondsTo("object")) {
            return null;
        }

        Resource subject   = convertRDFResource(ctx, rdfStatement.callMethod(ctx, "subject"));
        Property predicate = convertRDFProperty(ctx, rdfStatement.callMethod(ctx, "predicate"));
        RDFNode  object    = convertRDFTerm(ctx,     rdfStatement.callMethod(ctx, "object"));

        return new Triple(
                Optional.ofNullable(subject).map(RDFNode::asNode).orElse(null),
                Optional.ofNullable(predicate).map(RDFNode::asNode).orElse(null),
                Optional.ofNullable(object).map(RDFNode::asNode).orElse(null)
        );
    }

    public static Selector convertRDFStatementToSelector(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == ctx.nil || !rdfStatement.respondsTo("subject") ||
                !rdfStatement.respondsTo("predicate") || !rdfStatement.respondsTo("object")) {
            return null;
        }

        Resource subject   = convertRDFResource(ctx, rdfStatement.callMethod(ctx, "subject"));
        Property predicate = convertRDFProperty(ctx, rdfStatement.callMethod(ctx, "predicate"));
        RDFNode  object    = convertRDFTerm(ctx,     rdfStatement.callMethod(ctx, "object"));

        return new SimpleSelector(subject, predicate, object);
    }

    public static Resource convertRDFResource(ThreadContext ctx, IRubyObject rdfResource) {
        if (rdfResource == ctx.nil || !rdfResource.respondsTo("uri?") || !rdfResource.respondsTo("anonymous?")) {
            return null;
        }

        // Return new URI resource.
        IRubyObject uriReturn = rdfResource.callMethod(ctx, "uri?");
        if (!(uriReturn instanceof RubyBoolean)) {
            return null;
        }
        boolean isURI = (boolean) uriReturn.toJava(boolean.class);
        if (isURI) {
            return MODEL.createResource(rdfResource.callMethod(ctx, "to_s").asJavaString());
        }

        // Return new anonymous resource.
        IRubyObject anonymousReturn = rdfResource.callMethod(ctx, "anonymous?");
        if (!(anonymousReturn instanceof RubyBoolean)) {
            return null;
        }
        boolean isAnonymous = (boolean) anonymousReturn.toJava(boolean.class);
        if (isAnonymous) {
            return MODEL.createResource(AnonId.create(rdfResource.callMethod(ctx, "to_s").asJavaString()));
        }

        // Return null if rdfResource was neither a URI or anonymous resource
        return null;
    }

    public static Property convertRDFProperty(ThreadContext ctx, IRubyObject rdfProperty) {
        if (rdfProperty == ctx.nil || !rdfProperty.respondsTo("uri?")) {
            return null;
        }

        IRubyObject uriReturn = rdfProperty.callMethod(ctx, "uri?");
        if (!(uriReturn instanceof RubyBoolean)) {
            return null;
        }
        boolean isURI = (boolean) uriReturn.toJava(boolean.class);
        if (isURI) {
            return MODEL.createProperty(rdfProperty.callMethod(ctx, "to_s").asJavaString());
        }

        return null;
    }

    public static RDFNode convertRDFTerm(ThreadContext ctx, IRubyObject rdfTerm) {
        if (rdfTerm == ctx.nil) {
            return null;
        }

        if (!rdfTerm.respondsTo("resource?") || !rdfTerm.respondsTo("node?") ||
                !rdfTerm.respondsTo("literal?")) {
            String literal = rdfTerm.asString().asJavaString();
            return MODEL.createLiteral(literal);
        }

        IRubyObject resourceReturn = rdfTerm.callMethod(ctx, "resource?");
        if (!(resourceReturn instanceof RubyBoolean)) {
            return null;
        }
        boolean isResource = (boolean) resourceReturn.toJava(boolean.class);
        if (isResource) {
            IRubyObject nodeReturn = rdfTerm.callMethod(ctx, "node?");
            if (!(nodeReturn instanceof RubyBoolean)) {
                return null;
            }
            boolean isNode = (boolean) nodeReturn.toJava(boolean.class);
            if (isNode) {
                String id = rdfTerm.callMethod(ctx, "id").asString().asJavaString();
                return MODEL.createResource(AnonId.create(id));
            }

            String uri = rdfTerm.callMethod(ctx, "to_s").asJavaString();
            return MODEL.createResource(uri);
        } else {
            IRubyObject literalReturn = rdfTerm.callMethod(ctx, "literal?");
            if (!(literalReturn instanceof RubyBoolean)) {
                return null;
            }
            boolean isLiteral = (boolean) literalReturn.toJava(boolean.class);
            if (isLiteral) {
                String value = rdfTerm.callMethod(ctx, "value").asString().asJavaString();
                String dtype = rdfTerm.callMethod(ctx, "datatype").asString().asJavaString();
                return MODEL.createTypedLiteral(value, dtype);
            }
        }

        return null;
    }
}
