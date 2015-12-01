# Change Log

All notable changes to rdf-jena will be documented in this file. The curated log begins at changes to version 0.1.0.

This project adheres to [Semantic Versioning](http://semver.org/).

## 0.2.0 - 2015-12-01
### Fixed
- Alias `each_statement` to `each`. The java method iterateStatements is now provided as both ruby methods.

### Added
- Implemented query_pattern method to take advantage of Apache Jena model selectors.

## 0.1.0 - 2015-12-01
### Added
- Implements an [RDF.rb][RDF.rb] RDF::Repository using Apache Jena with TDB storage.
- Implements an [RDF.rb][RDF.rb] RDF::Graph as a Jena named model.
- Implemented `each_statement` to enumerate all statements in the repository (TDB Dataset's defaut model).
- Implemented `has_statement?` to check for presence of statement in repository or graph.
- Implemented mutation methods `clear_statements`, `delete_statement`, and `insert_statement`.
- Support for query and modification of graphs using `each_graph`, `insert_graph`, `replace_graph`, and `delete_graph`.
- Supporting boolean flag on RDF::Repository to union named graphs with the default when retrieved (see `each_graph`).

[RDF.rb]:   https://github.com/ruby-rdf/rdf
