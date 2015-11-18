require_relative 'jena/version'

# Load JRuby-exposed objects for rdf-jena.
require_relative 'jena/jars/rdf-jena-1.0.0.jar'
require          'com/github/rdf_jena/RDFJena'

require_relative 'jena/jena_storage'
