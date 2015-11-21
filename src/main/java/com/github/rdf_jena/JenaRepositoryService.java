package com.github.rdf_jena;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.runtime.load.BasicLibraryService;

import java.io.IOException;

public class JenaRepositoryService implements BasicLibraryService {

    @Override
    public boolean basicLoad(final Ruby ruby) throws IOException {
        // RDF::Jena module
        RubyModule rdfModule  = ruby.defineModule("RDF");
        RubyModule jenaModule = ruby.defineModuleUnder("Jena", rdfModule);

        // RDF::Jena::Repository class
        RubyClass repositoryClass = ruby.defineClassUnder(
                "Repository",
                ruby.getObject(), Repository.Allocator,
                jenaModule
        );
        repositoryClass.defineAnnotatedMethods(Repository.class);

        return true;
    }
}
