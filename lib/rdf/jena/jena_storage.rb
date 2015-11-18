module RDF::Jena

  class JenaStorage

    def initialize(data)
      @storage = TripleStorage.new(data)
    end

    def triples(subject, predicate, object, options={})
      map_method = options[:only]
      map_method = if map_method && self.respond_to?(map_method, true)
                     self.method(map_method)
                   else
                     self.method(:all)
                   end

      triples = @storage.triples(subject, predicate, object, options)

      if block_given?
        triples.each { |triple| yield map_method.call(triple) }
      else
        triples = triples.respond_to?(:lazy) ? triples.lazy : triples
        triples.map { |triple| map_method.call(triple) }
      end
    end

    def all(statement)

      if statement.respond_to? :subject
        [
          statement.subject.to_s,
          statement.predicate.to_s,
          object(statement)
        ]
      else
        statement
      end
    end

    def subject(statement)
      return statement.subject.to_s if statement.respond_to? :subject
      statement[0]
    end

    def predicate(statement)
      return statement.predicate.to_s if statement.respond_to? :predicate
      statement[1]
    end

    def object(statement)
      if statement.respond_to? :object
        object = statement.object
        (object.literal? && object.value.to_s) || object.to_s
      else
        statement[2]
      end
    end
  end
end
