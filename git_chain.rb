#! /usr/bin/ruby
# vim set ts=4:ai:sw=4

require 'rubygems'
require 'sequel'
require 'time'
require 'trollop'

def update_chain(db)
	db[:git_commit].select(:commit, :log).each do |row|
		counts = Hash.new(0)
		row[:log].split("\\n").each do |line|
			if line =~ /^\s*(signed-off-by)|(acked-by)|(reviewed-by)|(tested-by)|(cc):\s+(.*)\s*$/i
				name_addr = $6
				type = case when $1 then "s"
					when $2 then "a"
					when $3 then "r"
					when $4 then "t"
					when $5 then "s"
					end
				db[:git_chain].insert(:commit => row[:commit],
					:name_addr => $1, :type => type, :ordered => counts[type])
				counts[type] += 1
			end
		end
	end
end

def main
	opts = Trollop::options do
		version "git_chain.rb version 0.1"
		banner <<-EOS
git_chain.rb will examine the log for each commit and create the chain
of signed-off-by's, acked-by's, etc.  You only need to specify the db
url, such as postgres://cabird:passwd@localhost/git_db
EOS
		opt :dburl, "url for database example: postgres://cabird:passwd@localhost/git_db", 
			:short => "d", :required => true, :type => String

	end

	db = Sequel.connect(opts[:dburl])
	update_chain(db)
end

main if __FILE__ == $0
