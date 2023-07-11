API_KEY = ENV['GPT4_KEY']
def call_gpt4_api(prompt)
  url = "https://api.openai.com/v1/chat/completions"

  body = { "model" => "gpt-4",
      "max_tokens" => 1000,
      "temperature" => 0,
      "messages" => [{"role" => "user", "content" => prompt}],
    }.to_json
  headers = { "Content-Type" => "application/json", "Authorization" => "Bearer #{API_KEY}" }
  response = RestClient.post(url, body, headers)
  json_response = JSON.parse(response.body)
  response = json_response["choices"].first["message"]["content"].strip
rescue RestClient::BadRequest
  "Bad Request Error: #{$!.message}"
rescue
  "Error: #{$!.message}"
end

INSTRUCTIONS = <<~INSTRUCTIONS
I'm building a collection of Postgresql + Timescaledb related snippets.
This website works with markdown files and I'm using mkdocs with mkdocs-material
to render the markdown files to html.
Use all types of formatting in the markdown to make it very intuitive and attractive for the end user. Add tips for Postgresql
and security concerns as warning blocks.

Build a markdown file walking through the snippet.

Put a header explaining the goal of that snippet and what it does.

Also, if the snippet contains several sql commands, break down the snippet parts and
explain part by part instead of throwing all commands in the same code block.
Add table creating and missing details to have a runnable example from the snippet.


Here is the snippet:
```sql
INSTRUCTIONS

def chat_gpt_describe_this_snippet(sql)
  call_gpt4_api(INSTRUCTIONS + "\n" + sql + "\n```")
end

def info(content)
  puts TTY::Markdown.parse(content)
end

task :default do
  require 'bundler/inline'
  gemfile(true) do
    gem 'rest-client'
    gem 'tty-markdown'
    gem 'pry-rescue'
  end
  require 'json'
  # iterate over all sql files
  Dir.glob('*.sql') do |file|
    # create markdown from sql file
    markdown_file = "docs/#{file.gsub(/\.sql$/, '.md')}"
    if File.exist?(markdown_file)
      puts "Skipping #{markdown_file} as it already exists"
      next
    end
    puts "Converting #{file} to #{markdown_file}"
    content = chat_gpt_describe_this_snippet(IO.read(file))
    title = content[/# (.*)$/,1]
    info(content)
    File.open("docs/#{markdown_file}", 'w+') do |f|
      f.puts content
    end
    path = file.gsub(/\.sql$/, '')
    File.open('mkdocs.yaml', 'a+') do |f|
      f.puts "  - #{title}: #{path}"
    end
  end
  # render snippets to html
  # start mkdocs server
end
