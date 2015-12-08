require          'rdf'

# Loads Repository within the RDF::Jena module.
require_relative 'jena/jars/rdf-jena-0.3.0.jar'
require          'com/github/rdf_jena/JenaRepository'

require_relative 'jena/version'

module RDF::Jena

  class Repository
    include RDF::Countable
    include RDF::Enumerable
    include RDF::Mutable
    include RDF::Durable
    include RDF::Queryable
  end

  class Graph
    include RDF::Value
    include RDF::Countable
    include RDF::Durable
    include RDF::Enumerable
    include RDF::Queryable
    include RDF::Mutable
  end
end
