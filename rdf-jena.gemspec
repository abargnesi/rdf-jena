require './lib/rdf/jena/version'

Gem::Specification.new do |spec|
  spec.name                     = 'rdf-jena'
  spec.version                  = RDF::Jena::Version
  spec.summary                  = '''
                                    RDF.rb storage implementation for Apache Jena
                                  '''.gsub(%r{^\s+}, ' ').gsub(%r{\n}, '')
  spec.description              = '''
                                    Implements an RDF.rb repository for Apache Jena running on JRuby.
                                  '''.gsub(%r{^\s+}, ' ').gsub(%r{\n}, '')
  spec.license                  = 'Apache-2.0'
  spec.authors                  = [
                                    'Anthony Bargnesi'
                                  ]
  spec.email                    = [
                                    'abargnesi@gmail.com'
                                  ]
  spec.files                    = [
                                    Dir.glob('lib/**/*.{jar,rb}'),
                                    __FILE__,
                                    'README.md'
                                  ].flatten!
  spec.homepage                 = 'https://github.com/abargnesi/rdf-jena'
  spec.rdoc_options             = [
                                    '--title', 'rdf-jena documentation',
                                    '--main', 'README.md',
                                    '--line-numbers',
                                    'README.md'
                                  ]

  spec.required_ruby_version    = '>= 2.0.0'

  # development gems
  spec.add_development_dependency 'minitest',      '~> 5.7'
  spec.add_development_dependency 'rake',          '~> 10.4'
  spec.add_development_dependency 'rake-compiler', '~> 0.9'
  spec.add_development_dependency 'rdoc',          '~> 4.2'
  spec.add_development_dependency 'rspec',         '~> 3.2'
  spec.add_development_dependency 'yard',          '~> 0.8'
end
# vim: ts=2 sw=2:
# encoding: utf-8
