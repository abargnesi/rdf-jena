package com.github.rdf_jena;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.runtime.load.BasicLibraryService;

import java.io.IOException;

public class RDFJenaService implements BasicLibraryService {

    @Override
    public boolean basicLoad(final Ruby ruby) throws IOException {
        // RDF::Jena module
        RubyModule rdfJenaModule = ruby.defineModule("RDF::Jena");

        // RDF::Jena::TDBStorage class
        RubyClass tdbStorageClass = ruby.defineClassUnder(
                "TDBStorage",
                ruby.getObject(), TDBStorageRubyWrapper.Allocator,
                rdfJenaModule
        );
        tdbStorageClass.defineAnnotatedMethods(TDBStorageRubyWrapper.class);

        // RDF::Jena::TripleStorage class
        RubyClass tripleStorageClass = ruby.defineClassUnder(
                "TripleStorage",
                ruby.getObject(), TripleStorageRubyWrapper.Allocator,
                rdfJenaModule
        );
        tripleStorageClass.defineAnnotatedMethods(TripleStorageRubyWrapper.class);

        return true;
    }
}
