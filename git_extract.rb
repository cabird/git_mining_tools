require 'set'
require 'rubygems'
require 'sequel'
require 'open4'
require 'time'
require 'trollop'

class String 
	def to_proc
		proc { |*args| send(self, *args) }
	end
end

class Symbol
	def to_proc
		proc { |obj, *args| obj.send(self, *args) }
	end
end

def setup_table(name, db, reset, &block)
	db.drop_table name if db.table_exists? name and reset
	if not db.table_exists? name
		db.create_table name, &block 
	end
end

class Timer
	def initialize
		@last = Time.now
	end
	
	def last
		@last
	end

	def delta
		last = @last
		@last = Time.now
		return @last - last
	end

	def log
		puts delta
	end
end

$timer = Timer.new


def setup_tables(db, reset)
	setup_table :git_repo, db, reset do
		String :commit 
		String :repo
		primary_key( :commit, :repo)
		index :commit
	end
	setup_table :git_commit, db, reset do
		String :commit
		String :tree
		String :author
		DateTime :author_dt
		String :author_id
		String :committer
		DateTime :committer_dt
		String :committer_id
		String :subject, :default => ''
		Integer :num_children, :default => 0
		Integer :num_parents, :default => 0
		String :log, :default => ''
		primary_key(:commit)
	end

	setup_table :git_dag, db, reset do
		String :child, :null => false
		String :parent, :null => false			
		index :parent
		index :child
	end
	setup_table :git_revision, db, reset do	
		String :commit, :null => false
		Integer :add
		Integer :remove
		String :path, :null => false
		primary_key(:commit, :path)
		index(:commit)
		index(:path)
	end

	setup_table :git_refs_tags, db, reset do
		String :commit, :null => false
		String :path, :null => false
		primary_key(:commit, :path)
	end

	setup_table :git_chain, db, reset do
		String :commit, :null => false
		String :name_addr 
		Integer :indiv_id
		String :type # s = Signed, a = Acked, t = Tested, r = Reviewed, c = Cc'ed
		primary_key :commit
		index :commit
	end

end

$git_log_cmd = "git log --full-history --all --numstat -M -C --pretty=format:\"" +
		"__START_GIT_COMMIT_LOG_MSG__%n%H%n%T%n%an <%ae>%n%aD%n%cn <%ce>" +
		"%n%cD%n%P%n%d%n%s%n%b%n__END_GIT_COMMIT_LOG_MSG__\""

def get_git_log_lines(repo_path)
	#run the log command
	git_log_cmd = "cd #{repo_path} && #{$git_log_cmd}"
	puts "Getting git log text"
	puts git_log_cmd
	error_txt = ""
	history = []
	status = Open4::popen4(git_log_cmd) do |pid, stdin, stdout, stderr|
		history = stdout.readlines
		error_txt = stderr.read
	end
	#if there was an error message, then print it out and die
	if status.exitstatus != 0
		puts "exit status is : #{status.exitstatus}"	
		puts error_txt
		exit(1)
	end

	puts "Parsing git log"
	
	return history.map!( &:strip )
end

def get_file_log_lines(file)
	return open(file).readlines.map( &:strip )
end


def parse_log(repo_name, history, db)
	print "**** parsing history ****\n"

	i = 0
	parsed_hashes = 0

	#get all commits that are already in the db
	commits = Set.new
	db[:git_commit].each { |commit| commits.add(commit) }

	git_commit_ps = db[:git_commit].prepare(:insert, :commit => :$commit, :tree => :$tree, 
		:author => :$author, :author_dt => :$author_dt,
		:committer => :$committer, :committer_dt => :$committer_dt,
		:log => :$log)

	history_length = history.length

	commit_data = []
	repo_data = []
	refs_tags_data = []
	dag_data = []
	revision_data = []
	db.transaction do	
	loop do
		i += 1 while i < history.length and not history[i] =~ /^__START_GIT_COMMIT_LOG_MSG__/
		break if i >= history_length
		i += 1
		s = i
		commit_id = history[i]

		#we ALWAYS have to insert the repo and the refs
		repo_data << [repo_name, commit_id]

		i += 1
		refs_line = history[s+7]
		if not refs_line.empty?
			refs_line.gsub!(/^\s*\(/, "")
			refs_line.gsub!(/\)\s*$/, "")
			#continue from here
			refs_line.split(", ").each { |ref| 
				refs_tags_data << [commit_id, ref]
			}
		end

		parsed_hashes += 1
		if parsed_hashes % 1000 == 0
			puts "parsed #{parsed_hashes} commits\n" 
			puts "this took #{$timer.delta} seconds\n"
		end
		#the sha may already be in the database so check that
		next if commits.include? commit_id

		log = ""
		i = s + 8
		while i < history.length and not history[i] =~ /^__END_GIT_COMMIT_LOG_MSG__/
			log << history[i] + "\\n"
			i += 1
		end
		tree = history[s+1]
		author = history[s+2]
		author_dt = history[s+3]
		committer = history[s+4]
		committer_dt = history[s+5]
		commit_data << [commit_id, tree, author, author_dt, committer, 
			committer_dt, log]
		# line 6 contains the parents, add those
		history[s+6].split(/\s+/).each do |parent| 
			dag_data << [commit_id, parent]
		end

		#move past the __END_GIT_COMMIT_LOG_MSG__
		i += 1
		# lines after this are the files that were changed and counts of lines changed
		while i < history_length and not history[i] =~ /__START_GIT_COMMIT_LOG_MSG__/
			if history[i] =~ /^(\d+)\s+(\d+)\s+(.*)/
				# (lines added)\s+(lines removed)\s+(path)
				revision_data << [commit_id, $1.to_i, $2.to_i, $3]
			elsif history[i] =~ /^\-\s+\-\s+(.*)/
				revision_data << [commit_id, nil, nil, $1]
			end
			i += 1
		end
	end
	end
	history.clear
	$timer.log
	puts "inserting #{revision_data.length} revision records"
	db[:git_revision].multi_insert([:commit, :add, :remove, :path], revision_data)
	$timer.log
	puts "inserting #{dag_data.length} dag records"
	db[:git_dag].multi_insert([:child, :parent], dag_data)
	$timer.log
	puts "inserting #{commit_data.length} commit records"
	db[:git_commit].multi_insert( [:commit, :tree, :author, :author_id,
		:committer, :committer_dt, :log], commit_data)
	$timer.log
	puts "inserting #{refs_tags_data.length} ref/tag records"
	db[:git_refs_tags].multi_insert([:commit, :path], refs_tags_data)
	$timer.log
	puts "inserting #{repo_data.length} repo records"
	db[:git_repo].multi_insert([:repo, :commit], repo_data)
	$timer.log
end	

#do some post processing and fill in the tables
def update_relations(db)
	puts "gathering number of parents"
	db << "update git_commit set num_parents = r.parents from (select child, count(*) as parents from
		git_dag group by child) as r where r.child = git_commit.commit"
	$timer.log
	puts "gathering number of children"
	db << "update git_commit set num_children = r.children from (select parent, count(*) as children from
		git_dag group by parent) as r where r.parent = git_commit.commit"
	$timer.log
end




def main
	opts = Trollop::options do
		version "git_extract.rb version 0.1"
		banner <<-EOS
git_extract.rb will mine information from git and put it into
a database. You must specify ONE of: repo location with -r or
log file with -l.  The log file must be the result of running
(all on one line, no line breaks):

#{$git_log_cmd}
EOS
		opt :repo, "path to git repository", :short => "r", :type => String 
		opt :log, "log file for git repository", :short => "l", :type => String
		conflicts :log, :repo
		opt :dburl, "url for database example: postgres://cabird:passwd@localhost/git_db", 
			:short => "d", :required => true, :type => String
		opt :name, "repo name to be stored in database", :short => "n", :required => true,
			:type => String
	end
	p opts
	db = Sequel.connect(opts[:dburl])
	setup_tables(db, true)
	puts "getting log lines"
	if opts[:log] != nil
		history = get_file_log_lines(opts[:log])
	elsif opts[:repo] != nil
		history = get_git_log_lines(opts[:repo])
	else
		puts "error! no way to get log lines, must specify either location of repo or a log file"
		exit(1)
	end
	$timer.log
	parse_log(opts[:name], history, db)
	update_relations(db)
end

main() if __FILE__ == $0
