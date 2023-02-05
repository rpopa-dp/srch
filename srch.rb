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
    attr_reader :document, :term_freq, :term_count

    def initialize(document:, term_freq:, term_count:)
      @document = document
      @term_freq = term_freq
      @term_count = term_count
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

        $stdout.puts("INFO: indexing #{file_path.dump}")
        index.add(document)
      end
      $stdout.puts('INFO: done')
    end
  end
end

module Repositories
  class Index
    attr_reader :entries, :entry_freq

    def initialize(tokenize:)
      @tokenize = tokenize

      @entries = []
      @entry_freq = {}
    end

    def add(document)
      term_freq = @tokenize.call(document.body).tally

      @entries.push(
        Models::Entry.new(
          document: document,
          term_freq: term_freq,
          term_count: term_freq.sum { |_, freq| freq },
        )
      )

      term_freq.each do |term, _|
        @entry_freq[term] ||= 0
        @entry_freq[term] += 1
      end
    end

    def search(query)
      query_terms = @tokenize.call(query).to_a

      results = entries.map do |entry|
        rank = query_terms.sum do |term|
          tf_idf(entry, term)
        end

        [rank, entry.document.id]
      end

      results.sort!
      results.reverse!

      results
    end

    private

    def tf(entry, term)
      entry.term_freq.fetch(term, 0).to_f / entry.term_count
    end

    def idf(term)
      count = entry_freq.fetch(term, 0)

      if count.zero?
        0.0
      else
        Math.log2(entries.count.to_f / count)
      end
    end

    def tf_idf(entry, term)
      tf(entry, term) * idf(term)
    end
  end
end

# ------------------------------------------------------------------------------
tokenize = Commands::Tokenize.new
index = Repositories::Index.new(
  tokenize: tokenize,
)
index_directory = Commands::IndexDirectory.new

index_directory.call(index, 'texts/*.txt')

loop do
  $stdout.puts('-' * 40)
  $stdout << 'query: '

  query = $stdin.gets

  break unless query

  pp index.search(query)
end
