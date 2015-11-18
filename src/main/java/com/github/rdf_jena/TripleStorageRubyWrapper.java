package com.github.rdf_jena;

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

import java.util.Optional;

import static org.jruby.RubySymbol.newSymbol;
import static org.jruby.javasupport.JavaUtil.convertJavaToRuby;

@JRubyClass(name = "StorageJena")
public class TripleStorageRubyWrapper extends RubyObject {

    private TDBStorage tdbStorage;

    public static ObjectAllocator Allocator = TripleStorageRubyWrapper::new;

    public TripleStorageRubyWrapper(Ruby runtime, RubyClass metaclass) {
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

    @JRubyMethod(name = "triples", required = 3, optional = 1)
    public IRubyObject triples(
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
}
