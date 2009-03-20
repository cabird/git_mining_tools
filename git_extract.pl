#!/usr/bin/perl -w
#test -- my @lines = split(/\n/, qx(git log --since="four weeks ago" --pretty=format:"%H %T %an <%ae> %aD %cn <%ce>% %cD %P%n") );

use warnings;
use strict;

use DBI;
use Cwd;

#http://www.perlmonks.org/?node_id=591180
use utf8; 
use Encode;

#stop stdout buffering
$| = 1;

sub process_git_repo ($$) {
	my ($db_name, $path) = @_;

	my $dbh = DBI->connect("dbi:Pg:database=$db_name", '', '', {AutoCommit => 1});

	my $check_sha = $dbh->prepare(q{select commit from git_commit where commit = ?});

	#This next line helps decipher the git command line and the regexp
	my $insert_git_commit = $dbh->prepare(q{insert into git_commit(commit, tree, author, author_dt, committer, committer_dt, subject, log) values(?,?,?,?,?,?,?,?)});

	my $insert_dag = $dbh->prepare(q{insert into git_dag(child, parent) values (?,?)});

	my $insert_revisions = $dbh->prepare(q{insert into git_revision(commit, add, remove, path) values (?,?,?,?)});

	my $insert_refs_tags = $dbh->prepare(q{insert into git_refs_tags(commit, path) values (?,?)});

	my $insert_git_repo = $dbh->prepare(q{insert into git_repo(commit, repo) values(?,?)});

	my @history = split(/\n/, qx(git log --full-history --all --numstat -M -C --pretty=format:"-prxh-START_LOG_MSG-%n%H%n%T%n%an <%ae>%n%aD%n%cn <%ce>%n%cD%n%P%n%d%n%s%n%b%n-prxh-END_LOG_MSG-%n")); 
	
	if (!defined $history[0]) {
		print STDERR "Error processing git repo $path\n";
		return;
	}

	#remove anything before first commit sha1
	while (@history) {
		my $line = shift @history;
		if ($line =~ /^-prxh-START_LOG_MSG-$/) {
			last;
		}
	}

	LOG:
	#if there isn't at least 9 lines then we've run out of history
	for (my $i = 0; $i < @history - 9 ; $i ++) {

		#we will do the insert after we get the log so record the offset in $s (or start)
		my $s = $i;
		my $commit_id = $history[$i];
		
		#test -- print "$history[$i]\n$history[$i+1]\n$history[$i+2]\n$history[$i+3]\n$history[$i+4]\n";
		#	 print "$history[$i+5]\n$history[$i+6]\n$history[$i+7]\n$history[$i+8]\n";


		#
		#We always have to insert the repo and refs, tags, etc
		$insert_git_repo->execute($commit_id, encode("UTF-8", $path)) or warn "Problem inserting repo name $commit_id ", $dbh->errstr;

		#refs, tags, etc (%d in git-log V 1.6.? or higher)
		if(defined $history[$s+7]) {
			$history[$s+7] =~ s/^\s*\(//;	
			$history[$s+7] =~ s/\)\s*$//;	
			foreach my $ref (split(/, /, $history[$s+7])) {
				#print "$path$ref\n";
				$insert_refs_tags->execute($commit_id, encode("UTF-8", "$path$ref")) 
					or warn "Problem inserting refs/tags on $commit_id ", $dbh->errstr;
			}
		}

		#
		#Is the sha already in the db?
		$check_sha->execute($commit_id) or warn "Problem checking for existing commit sha $commit_id ", $dbh->errstr;
		#if yes, move to next log
		if ($check_sha->rows > 0) {
			$i += 9;
			while ($i < @history) {
				if ($history[$i] =~ /^-prxh-START_LOG_MSG-$/) {
					next LOG;
				}
				$i ++;
			}
			#if we are here, we have finished the file
			last LOG;
		}

		#
		#We need to get the log before we insert the info about the commit
		#
		#the subject line sometimes ends in \n and sometimes doesn't, 
		# so we put it before log and remove any trailing or leading NEW_LINE from the log
		#
		$i = $s + 9; #The log is variable length so move $i
		my $log = '';
		while($i < @history && $history[$i] !~ /^-prxh-END_LOG_MSG-/) {
			$log .= "$history[$i]NEW_LINE";
			$i ++;	
		}
		$i ++; #move past -prxh-END_LOG_MSG-
		#remove extra trailing or leading NEW_LINE
		$log =~ s/^\s*NEW_LINE//;
		$log =~ s/NEW_LINENEW_LINE\s*$/NEW_LINE/;
		#print "$log\n";	

		#now do the insert
		$insert_git_commit->execute($history[$s], $history[$s+1], encode("UTF-8", $history[$s+2]), $history[$s+3], 
			encode("UTF-8", $history[$s+4]), $history[$s+5], 
			encode("UTF-8", $history[$s+8]), encode("UTF-8", $log))
				or warn "Problem inserting commit info on $commit_id ", $dbh->errstr;

		#	
		#create the dag
		#parents (%P in git-log)
		if (defined $history[$s+6]) {
			foreach my $parent (split(/ /, $history[$s+6])) {
				#print "$commit_id -- $parent\n";
				$insert_dag->execute($commit_id, $parent) or warn "Problem inserting parent on $commit_id \nThere was an error in git where the parent might be inserted more than once on some merges, this will exist in certain old histories, so check the log on this commit!\n", $dbh->errstr;
			}
		}	

		#file revisions
		while($i < @history && $history[$i] !~ /^-prxh-START_LOG_MSG-$/) {
			if ($history[$i] =~ /^(\d+)\s* (\d+)\s* (.*)/xms) {
				#print "adds = $1 -- removes $2 -- path = $3\n";
				$insert_revisions->execute($commit_id, $1, $2, $3) 
					or warn "Problem inserting file history on $commit_id ", $dbh->errstr;
			}
			elsif ($history[$i] =~ /^\-\s* \-\s* (.*)/xms) {
				#print "binary file path = $1\n";
				$insert_revisions->execute($commit_id, undef, undef, $1)
					or warn "Problem inserting binary file history on $commit_id ", $dbh->errstr;
			}

			$i ++;
		}
		#print "\n\n";
	}

	$check_sha->finish;
	$insert_git_repo->finish;
	$insert_refs_tags->finish;
	$insert_revisions->finish;
	$insert_dag->finish;
	$insert_git_commit->finish;
	$dbh->disconnect;

}

#
# Step through all the directories
#
sub process_dir 
{
	my ($path, $config_ref, $db_check_repo_ref) = @_;

	chdir $path or die "Cannot processes $path";
	my $cwd = getcwd;

	if (-e ".git") 
	#if ($path =~ /\w+\.git\s*$/xms)
	{
		if ($cwd =~ /$config_ref->{repo_path_ignore}(.*)/) {
			my $short_path = $1;
			$db_check_repo_ref->execute($short_path) or warn "Problem checking if repo has already been processed\n";
			if ($db_check_repo_ref->rows > 0) {
				print "Skipping ... we've already done this repo: $short_path\n";
			}
			else {
				print "Processing git repo $short_path \n";
				process_git_repo($config_ref->{db_name}, $short_path);
			}
		}
		else {
			warn "Problem processing cwd: $cwd";
		}
	}
	else {
		foreach my $next_path (<*>) 
		{
			if (-d $next_path) 
			{
				process_dir($next_path, $config_ref, $db_check_repo_ref);
			}
		}
	}
	chdir '..' or die "Cannot backout of $path";
}

use Config::General;

my $config_path = shift @ARGV;
if (!defined $config_path) {
	$config_path = 'config';
}
die "Config file \'$config_path\' does not exist"
	unless (-e $config_path);
my %config =  Config::General::ParseConfig($config_path);
#test -- print "$config{repo_path}, $config{db_name}\n";

my $dbh_ref = DBI->connect("dbi:Pg:database=$config{db_name}", '', '', {AutoCommit => 1});

my $db_check_repo_ref = $dbh_ref->prepare(q{select repo from git_repo where repo = ? group by repo}); 

process_dir($config{repo_path}, \%config, $db_check_repo_ref);

#update parent and children refs in git table
$dbh_ref->do(q{update git_commit set num_parents = r.parent from (select child, count(*) as parent from git_dag, git_commit where git_dag.child = git_commit.commit group by child) as r where r.child = git_commit.commit})
	or warn "Problem updating number of parents ", $dbh_ref->errstr;

$dbh_ref->do(q{update git_commit set num_children = r.child from (select parent, count(*) as child from git_dag, git_commit where git_dag.parent = git_commit.commit group by parent) as r where r.parent = git_commit.commit})
	or warn "Problem updating number of children ", $dbh_ref->errstr;

$db_check_repo_ref->finish;
$dbh_ref->disconnect;
 
__END__

=head1 Extracts a git repo from the logs

make sure you run git_extract.sql first

You need to set up a config file with the following
db_name=linux3
repo_path=/home/pcr/mining_procedure/test/git_repo_test
repo_path_ignore=/home/pcr/mining_procedure/

=cut
