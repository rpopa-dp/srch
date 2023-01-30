require 'pp'

class Tokenizer
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

def tf(doc, term)
  count = doc[term]
  sum = doc.values.sum

  count.to_f / sum
end

def idf(docs, term)
  total = docs.count
  count = docs.count { |_, doc| doc.key?(term) }

  if count.zero?
    0.0
  else
    Math.log2(total.to_f / count)
  end
end

def tf_idf(docs, doc_name, term)
  a = tf(docs[doc_name], term)
  b = idf(docs, term)

  a * b
end

def query_docs(docs, query)
  query_terms = Tokenizer.new.call(query).to_a

  results = docs.keys.map do |doc_name|
    rank = query_terms.sum do |term|
      tf_idf(docs, doc_name, term)
    end

    [rank, doc_name]
  end

  results.sort!
  results.reverse!

  results
end

# ------------------------------------------------------------------------------
docs = {}

Dir.glob('texts/*.txt').each do |file_path|
  file_contents = File.read(file_path)
  tokens = Tokenizer.new.call(file_contents)

  docs[file_path] = tokens.tally
end

loop do
  $stdout.puts('-' * 40)
  $stdout << 'query: '

  pp query_docs(docs, $stdin.gets)
end
