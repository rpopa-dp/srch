require 'duckdb'
require 'nokogiri'
require 'pp'

module Models
  class Document
    attr_reader :title, :body

    def initialize(title:, body:)
      @title = title
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

  # NOTE: indexing directory containing plain text files
  class IndexDirectory
    def call(index, pattern)
      Dir.glob(pattern).each do |file_path|
        file_contents = File.read(file_path)
        document = Models::Document.new(
          title: file_path,
          body: file_contents,
        )

        $stdout.puts("INFO: indexing #{file_path.dump}")
        index.add(document)
      end
      $stdout.puts('INFO: done')
    end
  end

  # NOTE: indexing wikipedia xml dump files
  # ref: https://dumps.wikimedia.org/enwiki/
  class IndexArticles
    def call(index, path)
      File.open(path) do |file|
        title = nil
        body = nil
        doc_count = 1

        Nokogiri::XML::Reader(file).each do |node|
          case node.name
          when 'title'
            if title.nil? && !node.inner_xml.empty?
              title = node.inner_xml
            end
          when 'text'
            if body.nil? && !node.inner_xml.empty?
              body = node.inner_xml
            end
          end

          if title && body
            document = Models::Document.new(
              title: title,
              body: body,
            )

            $stdout.puts("INFO: (#{doc_count}) indexing #{title.dump}")
            index.add(document)

            title = nil
            body = nil

            break if doc_count > 10000
            doc_count += 1
          end
        end
        $stdout.puts('INFO: done')
      end
    end
  end
end

module Repositories
  class InMemoryIndex
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

        [rank, entry.document.title]
      end

      results.select! { |result| result[0].positive? }
      results.sort!
      results.reverse!

      results.take(10)
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

  class DuckdbIndex
    def initialize(db:, tokenize:)
      @db = db
      @tokenize = tokenize
    end

    def setup
      transaction do
        @db.execute(<<~SQL)
          CREATE SEQUENCE seq_documents_id
          START 1;
        SQL

        @db.execute(<<~SQL)
          CREATE TABLE documents (
            id INTEGER NOT NULL DEFAULT nextval('seq_documents_id'),
            title VARCHAR NOT NULL,
            body VARCHAR NOT NULL,
            term_count INTEGER NOT NULL,

            PRIMARY KEY (id),
          );
        SQL

        @db.execute(<<~SQL)
          CREATE SEQUENCE seq_terms_id
          START 1;
        SQL

        @db.execute(<<~SQL)
          CREATE TABLE terms (
            id INTEGER NOT NULL DEFAULT nextval('seq_terms_id'),
            term VARCHAR NOT NULL,
            document_freq INTEGER NOT NULL,

            PRIMARY KEY (id),
            UNIQUE (term),
          );
        SQL

        @db.execute(<<~SQL)
          CREATE TABLE term_freq (
            document_id INTEGER NOT NULL,
            term_id INTEGER NOT NULL,
            freq INTEGER NOT NULL,

            PRIMARY KEY (document_id, term_id),
            FOREIGN KEY (document_id) REFERENCES documents(id),
            FOREIGN KEY (term_id) REFERENCES terms(id),
          );
        SQL
      end
    end

    def add(document)
      transaction do
        term_freq = @tokenize.call(document.body).tally
        document_id = insert_document(document, term_freq)
        term_ids = insert_terms(term_freq)

        insert_term_freq(term_freq, document_id, term_ids)
      end
    end

    def search(query)
      query_terms = @tokenize.call(query).to_a
      query_terms_values = query_terms.count.times.
        map { |index| "($#{index + 1})" }.
        join(', ')

      stmt = @db.prepared_statement(<<~SQL)
        SELECT
          sum(
            coalesce(term_freq.freq, 0)::double / coalesce(documents.term_count, 0)
            * log2((SELECT count(*) FROM documents)::double / coalesce(terms.document_freq, 0))
          ) AS rank,
          documents.title
        FROM (VALUES #{query_terms_values}) AS query_terms(term)
        INNER JOIN terms
          ON query_terms.term = terms.term
        INNER JOIN term_freq
          ON term_freq.term_id = terms.id
        INNER JOIN documents
          ON term_freq.document_id = documents.id
        GROUP BY
          documents.id,
          documents.title
        HAVING rank > 0.0
        ORDER BY rank DESC
        LIMIT 10
      SQL

      query_terms.each.with_index do |term, index|
        stmt.bind(index + 1, term)
      end

      stmt.execute.to_a
    end

    private

    def transaction
      @db.execute('BEGIN;')
      yield
      @db.execute('COMMIT;')
    rescue
      @db.execute('ROLLBACK;')
      raise
    end

    def insert_document(document, term_freq)
      term_count = term_freq.sum { |_, freq| freq }
      # NOTE: for some reason binding 3 values doesn't return id, but count of
      # inserted rows
      stmt = @db.prepared_statement(<<~SQL)
        INSERT INTO documents(title, body, term_count)
        VALUES ($1, $2, #{term_count})
        RETURNING id;
      SQL

      stmt.bind(1, document.title)
      stmt.bind(2, document.body)

      stmt.execute.to_value(0, 0)
    end

    def insert_terms(term_freq)
      terms = find_terms(term_freq)

      if terms.any?
        @db.execute(<<~SQL)
          UPDATE terms
          SET document_freq = document_freq + 1
          WHERE id IN (#{terms.values.join(', ')})
        SQL
      end

      (term_freq.keys - terms.keys).each do |term|
        stmt = @db.prepared_statement(<<~SQL)
          INSERT INTO terms(term, document_freq)
          VALUES ($1, 1)
          RETURNING id;
        SQL
        stmt.bind(1, term)
        term_id = stmt.execute.to_value(0, 0)

        terms[term] = term_id
      end

      terms
    end

    def find_terms(term_freq)
      in_binding = term_freq.count.times.map { |index| "$#{index + 1}" }.join(', ')
      stmt = @db.prepared_statement(<<~SQL)
        SELECT term, id
        FROM terms
        WHERE term IN (#{in_binding})
      SQL

      term_freq.each.with_index do |(term, _), index|
        stmt.bind(index + 1, term)
      end

      stmt.execute.to_h
    end

    def insert_term_freq(term_freq, document_id, term_ids)
      term_freq.each do |term, freq|
        stmt = @db.prepared_statement(<<~SQL)
          INSERT INTO term_freq(document_id, term_id, freq)
          VALUES ($1, $2, $3);
        SQL

        stmt.bind(1, document_id)
        stmt.bind(2, term_ids[term])
        stmt.bind(3, freq)

        stmt.execute
      end
    end
  end
end

# ------------------------------------------------------------------------------
INDEX_FILE_PATH = './index.duckdb'
INDEX_WAL_FILE_PATH = './index.duckdb.wal'

tokenize = Commands::Tokenize.new
db = DuckDB::Database.open(INDEX_FILE_PATH).connect
duckdb_index = Repositories::DuckdbIndex.new(
  db: db,
  tokenize: tokenize,
)

case ARGV[0]
when 'index'
  File.delete(INDEX_FILE_PATH) if File.exist?(INDEX_FILE_PATH)
  File.delete(INDEX_WAL_FILE_PATH) if File.exist?(INDEX_WAL_FILE_PATH)

  index_articles = Commands::IndexArticles.new

  duckdb_index.setup
  index_articles.call(duck_dbindex, ARGV[1])
when 'search'
  loop do
    $stdout.puts('-' * 40)
    $stdout << 'query> '

    query = $stdin.gets

    break unless query

    search_start = Time.now
    hits = duckdb_index.search(query)
    search_end = Time.now

    puts
    puts "RESULTS (#{search_end - search_start}s):"
    pp hits
  end
else
  puts "ERROR: invalid command #{ARGV[0].dump}"
end
