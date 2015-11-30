package com.github.rdf_jena;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
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

        // RDF::Jena::Graph class
        RubyClass graphClass = ruby.defineClassUnder(
                "Graph",
                ruby.getObject(), Graph.Allocator,
                jenaModule
        );
        graphClass.defineAnnotatedMethods(Graph.class);

        return true;
    }

    public static RubyClass findClass(ThreadContext ctx, String klass) {
        RubyModule rdfJenaModule   = (RubyModule) ctx.runtime.getModule("RDF").getConstant("Jena");
        return (RubyClass) rdfJenaModule.getConstant(klass);
    }
}
