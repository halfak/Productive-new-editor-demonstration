#!/usr/bin/env python3
import getpass, docopt, logging, sys, time
help_and_opts = """
Gathers a set of "Productive new editors" who registered within a specified 
timespan and prints a TSV of user_id, user_name, user_registration.

Note that reverts that do not occur within 48 hours of the initial edit are 
not counted. 

Usage:
  productive_new_editors <start_date> <end_date> [-n=<edits>] [-t=<days>] 
      [--defaults-file=<path>] [-h=<host>] [-u=<name>] [-d=<name>]
      [--debug] 
  productive_new_editors --version
  productive_new_editors (-h | --help)

Options:
  --version               Show version
  (-h | --help)           Show help
  <start_date>            The minimum user_registration to include
  <end_date>              The maximum user_registration to include
  -n=<edits>              The number of productive edits required to be considered productive [default: 1]
  -t=<days>               The number of days since registration to look for productive edits [default: 1]
  --defaults-file=<path>  The default config file for connecting to mysql.
  -h | --host=<host>      The mysql host to connect to [default: s1-analytics-slave.eqiad.wmnet]
  -u | --user=<name>      The mysql username to connect with [default: {user}]
  -d | --db=<name>        The mysql database to connect to [default: enwiki]
  --debug                 Show debugging info?

""".format(user=getpass.getuser())
__doc__ = help_and_opts

from mw import database, Timestamp
from mw.lib import reverts

DAY_SECONDS = 60*60*24 # Number of seconds in a day.  Used a few time to convert
                       # days to seconds. 

def escape(string):
	"Escapes a string for inclusion in a TSV file"
	return string.replace("\t", "\\t").replace("\n", "\\n")

def main():
	args = docopt.docopt(__doc__, version="0.0.1")
	
	logging.basicConfig(
		level=logging.DEBUG if args["--debug"] else logging.INFO,
		stream=sys.stderr,
		format='%(asctime)s %(name)-8s %(message)s',
		datefmt='%b-%d %H:%M:%S'
	)
	
	# Constructing a DB this way is lame, but it's necessary for proper handling
	# of the defaults-file argument. 
	db_kwargs = {
		'user': args['--user'],
		'host': args['--host'],
		'db': args['--db']
	}
	if args['--defaults-file'] != None:
		db_kwargs['read_default_file'] = args['--defaults-file']
	db = database.DB.from_params(**db_kwargs)
	
	run(
		db, 
		Timestamp(args['<start_date>']), 
		Timestamp(args['<end_date>']),
		int(args['-n']),
		int(args['-t'])
	)

def run(db, start_date, end_date, n, t):
	
	# Print some headers
	print(
		"\t".join([
			"user_id",
			"user_name",
			"user_registration",
			"productive",
			"censored"
		])
	)
	
	t_seconds = DAY_SECONDS*t # Convert days to seconds so that we can do some math
	
	# Get relevant users
	users = db.users.query(
		registered_after=start_date, 
		registered_before=end_date
	)
	for user_row in users:
		
		# Convert user_registration to a useful type
		user_registration = Timestamp(user_row['user_registration'])
		
		# Get all the revisions the user made within time "t" days registration
		revisions = db.revisions.query(
			user_id=user_row['user_id'], 
			before=user_registration + (DAY_SECONDS*t),
			include_page=True
		)
		
		# Count up the productive edits
		productive_edits = 0 
		for rev_row in revisions:
			
			# Convert revision timestamp to a useful type
			rev_timestamp = Timestamp(rev_row['rev_timestamp'])
			
			# Must me a content edit
			if rev_row['page_namespace'] == 0:
				
				# If the revert doesn't happen in 48 hours, it doesn't count
				revert_end_of_life = rev_timestamp + DAY_SECONDS*2
				
				revert = reverts.database.check_row(
					db, 
					rev_row,
					radius = 15, # Reverts can't cross more than 15 revisions
					before = revert_end_of_life
				)
				
				if revert == None: # Not reverted
					productive_edits += 1
					
					if productive_edits >= n: #We're done here
						break
					
				
			
		
		print(
			"\t".join([
				str(user_row['user_id']),
				escape(str(user_row['user_name'], 'utf-8')),
				escape(str(user_row['user_registration'], 'utf-8')),
				str(productive_edits >= n),
				str(time.time() - user_registration.unix() < (2+t)*DAY_SECONDS)
			])
		)
		
	
if __name__ == "__main__": main()

