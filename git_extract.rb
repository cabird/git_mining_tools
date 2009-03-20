require 'set'
require 'rubygems'
require 'sequel'
require 'open4'
require 'time'

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

	setup_table :get_chain, db, reset do
		String :commit, :null => false
		String :name_addr 
		Integer :indiv_id
		String :type # s = Signed, a = Acked, t = Tested, r = Reviewed, c = Cc'ed
		primary_key :commit
		index :commit
	end

end

def mine_git_repo(repo_name, db, repo_path)
	#run the log command
	git_log_cmd = "cd #{repo_path} && git log --full-history --all --numstat -M -C --pretty=format:\"" +
		"__START_GIT_COMMIT_LOG_MSG__%n%H%n%T%n%an <%ae>%n%aD%n%cn <%ce>" +
		"%n%cD%n%P%n%d%n%s%n%b%n__END_GIT_COMMIT_LOG_MSG__\""

	puts "Getting git log text"
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
	
	history.map! &:strip 

	print "**** parsing history ****\n"

	i = 0
	parsed_hashes = 0

	#get all commits that are already in the db
	commits = Set.new
	db[:git_commit].each { |commit| commits.add(commit) }
	last_time = Time.now

	start_re = /^__START_GIT_COMMIT_LOG_MSG__/
	end_re = /^__END_GIT_COMMIT_LOG_MSG__/
	file_stat_re1 =  /^(\d+)\s+(\d+)\s+(.*)/
	file_stat_re2 = /^\-\s+\-\s+(.*)/
	git_commit_ps = db[:git_commit].prepare(:insert, :commit => :$commit, :tree => :$tree, 
		:author => :$author, :author_dt => :$author_dt,
		:committer => :$committer, :committer_dt => :$committer_dt,
		:log => :$log)

	history_length = history.length
	db.transaction do	
	loop do
		#i += 1 while i < history.length and not history[i] =~ /^__START_GIT_COMMIT_LOG_MSG__/
		i += 1 while i < history_length and not history[i] =~ start_re
		break if i >= history_length
		i += 1
		s = i
		commit_id = history[i]

		#we ALWAYS have to insert the repo and the refs
		db[:git_repo].insert(:repo => repo_name, :commit => commit_id)

		i += 1
		refs_line = history[s+7]
		if not refs_line.empty?
			refs_line.gsub!(/^\s*\(/, "")
			refs_line.gsub!(/\)\s*$/, "")
			#continue from here
			refs_line.split(", ").each { |ref| 
				db[:git_refs_tags].insert(:commit => commit_id, :path=>ref)
			}
		end
		parsed_hashes += 1
		if parsed_hashes % 1000 == 0
			puts "parsed #{parsed_hashes} commits\n" 
			puts "this took #{Time.now - last_time} seconds\n"
			last_time = Time.now
		end
		#the sha may already be in the database so check that
		next if commits.include? commit_id

		log = ""
		i = s + 8
		#while i < history.length and not history[i] =~ /^__END_GIT_COMMIT_LOG_MSG__/
		while i < history_length and not history[i] =~ end_re
			log << history[i] + "\\n"
			i += 1
		end
		# tree is s+1, author name/email is s+2 author datetime is s+3
		# committer name/email is s+4, committer datetime is s+5
		db[:git_commit].insert(:commit => commit_id, :tree => history[s+1], 
			:author => history[s+2], :author_dt => history[s+3],
			:committer => history[s+4], :committer_dt => history[s+5],
			:log => log)
		# line 6 contains the parents, add those
		history[s+6].split(/\s+/).each do |parent| 
		   db[:git_dag].insert(:child => commit_id, :parent => parent)
		end

		#move past the __END_GIT_COMMIT_LOG_MSG__
		i += 1
		# lines after this are the files that were changed and counts of lines changed
		while i < history_length and not history[i] =~ /__START_GIT_COMMIT_LOG_MSG__/
			if history[i] =~ file_stat_re1
				# (lines added)\s+(lines removed)\s+(path)
				db[:git_revision].insert(:commit => commit_id, :add => $1.to_i,
					:remove => $2.to_i, :path => $3)
			elsif history[i] =~ file_stat_re2
				db[:git_revision].insert(:commit => commit_id, :path => $1)
			end
			i += 1
		end
	end
	end
end	

#do some post processing and fill in the tables
def update_relations(db)
	print "gathering number of parents\n"
	db << "update git_commit set num_parents = r.parents from (select child, count(*) as parents from
		git_dat group by child) as r where r.child = git_commit.commit"
	db << "update git_commit set num_children = r.children from (select parent, count(*) as children from
		git_dat group by parent) as r where r.parent = git_commit.commit"
end

def main
	repo_name = ARGV[0]
	repo_path = ARGV[1]
	db_url = ARGV[2]
	db = Sequel.connect(db_url)
	setup_tables(db, true)
	mine_git_repo(repo_name, db, repo_path)
	update_relations(db)
end

main() if __FILE__ == $0
