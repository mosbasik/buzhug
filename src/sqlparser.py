from buzhug import Base
import sqlparse
import sys
def run():
	line = sys.stdin.readline()
	while line:
		s = sqlparse.parse(line)
		s = s[0]
		print s.tokens
		if unicode(s.tokens[0]) == unicode('create'):
			if unicode(s.tokens[2]) == unicode('database'):
				db = Base(unicode(s.tokens[4]))
		line = sys.stdin.readline()


if __name__ == '__main__':
    run()