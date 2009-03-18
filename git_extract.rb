require 'rubygems'
require 'sequel'
require 'open4'

def setupTables(db)
	db.drop_table :git_repo if db.table_exists? :git_repo
	db.create_table :git_repo do
		String :commit 
		String :repo
		primary_key:commit, :repo
		index :commit
	end
	db.drop_table :git_commit if db.table_exists? :git_commit
	db.create_table :git_commit do
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

	db.drop_table :git_dag if db.table_exists? :git_dag
	db.create_table :git_dag do
		String :child, :null => false
		String :parent, :null => false			
		index :parent
		index :child
	end
	
	db.drop_table :git_revision if db.table_exists? :git_revision
	db.create_table :git_revision do
		String :commit, :null => false
		Integer :add
		Integer :remove
		String :path, :null => false
		primary_key(:commit, :path)
		index(:commit)
		index(:path)
	end

	db.drop_table :git_refs_tags if db.table_exists? :git_refs_tags
	db.create_table :git_refs_tags do
		String :commit, :null => false
		String :path, :null => false
		primary_key(:commit, :path)
	end

	db.drop_table :git_chain if db.table_exists? :git_chain
	db.create_table :git_chain do
		String :commit, :null => false
		String :name_addr 
		Integer :indiv_id
		String :type # s = Signed, a = Acked, t = Tested, r = Reviewed, c = Cc'ed
		primary_key :commit
		index :commit
	end

end

def mine_git_repo(db, repo_path)
	#setup some prepared sql statements
	check_sha = db[:git_commit].filter(:commit => :$commit).prepare(:select)
	insert_commit = db[:git_commit].prepare(:insert, 
		:commit => :$commit, :tree =>:$tree, :author =>:$author,
		:author_dt => :$author_dt, :commiter => :$commiter,
		:committer_dt => :$committer_dt, :subject => :$subject,
		:log => :$log)
		
	#now run the command
	git_log_cmd = "cd #{repo_path} && git log --full-history --all --numstat -M -C --pretty=format:\"" +
		"__START_GIT__COMMIT_LOG_MSG__%n%H%n%T%n%an <%ae>%n%aD%n%cn <%ce%>" +
		"%n%cD%n%P%n%d%n%s%n%b%n__END_GIT_COMMIT_LOG_MSG__\""
	puts git_log_cmd

	error_txt = ""
	lines = []
	status = Open4::popen4(git_log_cmd) do |pid, stdin, stdout, stderr|
		lines = stdout.readlines

		error_txt = stderr.read
	end
	#if there was an error message, then print it out and die
	if status.exitstatus != 0
		puts "exit status is : #{status.exitstatus}"	
		puts error_txt
		exit(1)
	end
	puts lines
	
end	


DB = Sequel.connect(ARGV[0])
setupTables(DB)
mine_git_repo(DB, ".")
