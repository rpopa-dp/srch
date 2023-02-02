require 'pp'

module Models
  class Document
    attr_reader :id, :body

    def initialize(id:, body:)
      @id = id
      @body = body
    end
  end

  class Entry
    attr_reader :document, :term_freq

    def initialize(document:, term_freq:)
      @document = document
      @term_freq = term_freq
    end
  end
end

class Commands
  class Tokenize
    TOKEN_BEGIN = :token_begin
    TOKEN_END = :token_end

    def call(content)
      Enumerator.new do |yielder|
        start = 0

        token_boundaries(content).each do |type, index|
          case type
          when TOKEN_BEGIN
            start = index
          when TOKEN_END
            token = content[start..index]

            yielder.yield(token.downcase)
          else
            raise NotImplementedError, type
          end
        end
      end
    end

    private

    def token_boundaries(content)
      Enumerator.new do |yielder|
        inside_token = false

        content.each_char.with_index do |char, index|
          if token_char?(char)
            if !inside_token
              yielder.yield(TOKEN_BEGIN, index)
              inside_token = true
            end
          else
            if inside_token
              yielder.yield(TOKEN_END, index - 1)
              inside_token = false
            end
          end
        end

        if inside_token
          yielder.yield(TOKEN_END, content.size - 1)
        end
      end
    end

    def token_char?(char)
      !whitespace?(char) && !punctuation?(char)
    end

    def whitespace?(char)
      char == ' ' ||
      char == "\r" ||
      char == "\n" ||
      char == "\t" ||
      char == "\v" ||
      char == "\f"
    end

    def punctuation?(char)
      (33..47).include?(char.ord) ||
      (58..64).include?(char.ord) ||
      (91..96).include?(char.ord) ||
      (123..126).include?(char.ord)
    end
  end

  class IndexDirectory
    def call(index, pattern)
      Dir.glob(pattern).each do |file_path|
        file_contents = File.read(file_path)
        document = Models::Document.new(
          id: file_path,
          body: file_contents,
        )

        index.add(document)
      end
    end
  end

  class Search
    def initialize(tokenize:)
      @tokenize = tokenize
    end

    def call(index, query)
      query_terms = @tokenize.call(query).to_a

      results = index.entries.map do |entry|
        rank = query_terms.sum do |term|
          tf_idf(index, entry, term)
        end

        [rank, entry.document.id]
      end

      results.sort!
      results.reverse!

      results
    end

    # TODO: cache `sum`
    def tf(entry, term)
      count = entry.term_freq[term]
      sum = entry.term_freq.values.sum

      count.to_f / sum
    end

    # TODO: cache `count`
    def idf(index, term)
      total = index.entries.count
      count = index.entries.count { |entry| entry.term_freq.key?(term) }

      if count.zero?
        0.0
      else
        Math.log2(total.to_f / count)
      end
    end

    def tf_idf(index, entry, term)
      tf(entry, term) * idf(index, term)
    end
  end
end

module Repositories
  class Index
    attr_reader :entries

    def initialize(tokenize:)
      @tokenize = tokenize

      @entries = []
    end

    def add(document)
      term_freq = @tokenize.call(document.body).tally

      @entries.push(
        Models::Entry.new(
          document: document,
          term_freq: term_freq,
        )
      )
    end
  end
end


# ------------------------------------------------------------------------------
tokenize = Commands::Tokenize.new
index = Repositories::Index.new(
  tokenize: tokenize,
)
index_directory = Commands::IndexDirectory.new
search = Commands::Search.new(
  tokenize: tokenize,
)

index_directory.call(index, 'texts/*.txt')

loop do
  $stdout.puts('-' * 40)
  $stdout << 'query: '

  query = $stdin.gets

  break unless query

  pp search.call(index, query)
end
