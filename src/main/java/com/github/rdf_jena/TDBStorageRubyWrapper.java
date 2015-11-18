package com.github.rdf_jena;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.*;
import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import java.util.Iterator;
import java.util.Optional;

import static org.jruby.RubySymbol.newSymbol;
import static org.jruby.javasupport.JavaUtil.*;

@JRubyClass(name = "TDBStorage")
public class TDBStorageRubyWrapper extends RubyObject {

    private TDBStorage tdbStorage;

    public static ObjectAllocator Allocator = TDBStorageRubyWrapper::new;

    public TDBStorageRubyWrapper(Ruby runtime, RubyClass metaclass) {
        super(runtime, metaclass);
    }

    @JRubyMethod(name = "initialize", required = 1)
    public IRubyObject initialize(
            ThreadContext context,
            IRubyObject dataset
    ) {
        tdbStorage = new TDBStorage(dataset.asJavaString());
        return context.nil;
    }

    @JRubyMethod(name = "each_statement", required = 3, optional = 1)
    public IRubyObject statements(
            ThreadContext context,
            IRubyObject[] args
    ) {

        boolean objectLiteral;
        if (args.length > 3) {
            RubyHash optionHash = args[3].convertToHash();
            objectLiteral = Optional.ofNullable(
                    optionHash.get(newSymbol(context.runtime, "object_literal"))).
                    map(o -> true).
                    orElse(Boolean.FALSE);
        } else {
            objectLiteral = false;
        }

        Model model = tdbStorage.getDataset().getDefaultModel();

        Resource    subject   = args[0].isNil() ? null : model.createResource(args[0].asJavaString());
        Property    predicate = args[1].isNil() ? null : model.createProperty(args[1].asJavaString());
        RDFNode     object    = args[2].isNil() ? null : (
                objectLiteral ?
                        model.createLiteral(args[2].asJavaString()) :
                        model.createResource(args[2].asJavaString())
        );

        StmtIterator statementIterator = tdbStorage.iterate(subject, predicate, object);
        return convertJavaToRuby(context.runtime, statementIterator);
    }

    @JRubyMethod(name = "describe", required = 1)
    public IRubyObject describe(
            ThreadContext context,
            IRubyObject query
    ) {

        Iterator<Triple> tripleIterator = tdbStorage.describe(query.asJavaString());
        return convertJavaToRuby(context.runtime, tripleIterator);
    }

    @JRubyMethod(name = "ask", required = 1)
    public IRubyObject ask(
            ThreadContext context,
            IRubyObject query
    ) {

        boolean response = tdbStorage.ask(query.asJavaString());
        return convertJavaToRuby(context.runtime, response);
    }

    @JRubyMethod(name = "select", required = 1)
    public IRubyObject select(
            ThreadContext context,
            IRubyObject argOne
    ) {

        Iterable<QuerySolution> solutionIterable = tdbStorage.select(argOne.asJavaString());
        return convertJavaToRuby(context.runtime, solutionIterable);
    }

    @JRubyMethod(name = "construct", required = 1)
    public IRubyObject construct(
            ThreadContext context,
            IRubyObject query
    ) {

        Model model = tdbStorage.construct(query.asJavaString());
        return convertJavaToRuby(context.runtime, model);
    }
}
