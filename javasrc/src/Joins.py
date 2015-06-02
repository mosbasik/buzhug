from buzhug import Base
from buzhug import Record
import string
import random
import os
def nested_join(db1, db2, alias = None):
	name  = alias
	fields = []
	for a in db1.fields:
		if a != '__version__' and a != '__id__':
			fields.append((a, db1.fields[a]))
	for a in db2.fields:
		if a != '__version__' and a != '__id__':
			if (a, db2.fields[a]) not in fields:
				fields.append((a, db2.fields[a]))
			else:
				fields.remove((a, db2.fields[a]))
				fields.append((db1.name[db1.name.rfind("/")+1:]+"."+a, db1.fields[a]))
				fields.append((db1.name[db1.name.rfind("/")+1:]+"."+a, db2.fields[a]))
				
	if alias == None:
		name = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
		newDB = Base("tables/"+name)
	else:
		newDB = Base("tables/"+name)
	# print fields
	newDB.create(*tuple(fields))
	for record1 in db1:
		for record2 in db2:
			rec = []
			for f in db1.field_names:
				if f != '__id__' and f != '__version__':
					rec.append(getattr(record1, f))
			for f in db2.field_names:
				if f != '__id__' and f != '__version__':
					rec.append(getattr(record2, f))
			newDB.insert(*tuple(rec))
	return (newDB, name)

def inner_join(db1, db2,  t1_column, t2_column, alias = None):
	name  = alias
	fields = []
	for a in db1.fields:
		if a != '__version__' and a != '__id__':
			fields.append((a, db1.fields[a]))
	for a in db2.fields:
		if a != '__version__' and a != '__id__':
			if (a, db2.fields[a]) not in fields:
				fields.append((a, db2.fields[a]))
			else:
				fields.remove((a, db2.fields[a]))
				fields.append((db1.name[db1.name.rfind("/")+1:]+"."+a, db1.fields[a]))
				fields.append((db1.name[db1.name.rfind("/")+1:]+"."+a, db2.fields[a]))
				
	if alias == None:
		name = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
		newDB = Base("tables/"+name)
	else:
		newDB = Base("tables/"+name)
	# print fields
	newDB.create(*tuple(fields))
	for record1 in db1:
		for record2 in db2:
			if getattr(record1, t1_column) == getattr(record2, t2_column):
				rec = []
				for f in db1.field_names:
					if f != '__id__' and f != '__version__':
						rec.append(getattr(record1, f))
				for f in db2.field_names:
					if f != '__id__' and f != '__version__':
						rec.append(getattr(record2, f))
				newDB.insert(*tuple(rec))
	return (newDB, name)

def left_join(db1, db2, t1_column, t2_column, alias = None):
	name  = alias
	fields = []
	for a in db1.fields:
		if a != '__version__' and a != '__id__':
			fields.append((a, db1.fields[a]))
	for a in db2.fields:
		if a != '__version__' and a != '__id__':
			if (a, db2.fields[a]) not in fields:
				fields.append((a, db2.fields[a]))
			else:
				fields.remove((a, db2.fields[a]))
				fields.append((db1.name[db1.name.rfind("/")+1:]+"."+a, db1.fields[a]))
				fields.append((db1.name[db1.name.rfind("/")+1:]+"."+a, db2.fields[a]))
	if alias == None:
		name = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
		newDB = Base("tables/"+name)
	else:
		newDB = Base("tables/"+name)
	# print fields
	newDB.create(*tuple(fields))
	for record1 in db1:
		added = False
		for record2 in db2:
			if getattr(record1, t1_column) == getattr(record2, t2_column):
				added = True
				rec = []
				for f in db1.field_names:
					if f != '__id__' and f != '__version__':
						rec.append(getattr(record1, f))
				for f in db2.field_names:
					if f != '__id__' and f != '__version__':
						rec.append(getattr(record2, f))
				newDB.insert(*tuple(rec))
		if not added:
			rec = []
			for f in db1.field_names:
				if f != '__id__' and f != '__version__':
					rec.append(getattr(record1, f))
			for f in db2.field_names:
				if f != '__id__' and f != '__version__':
					rec.append(None)
			newDB.insert(*tuple(rec))
	return (newDB, name)

def right_join(db1, db2,  t1_column, t2_column, alias = None):
	name  = alias
	fields = []
	for a in db1.fields:
		if a != '__version__' and a != '__id__':
			fields.append((a, db1.fields[a]))
	for a in db2.fields:
		if a != '__version__' and a != '__id__':
			if (a, db2.fields[a]) not in fields:
				fields.append((a, db2.fields[a]))
			else:
				fields.remove((a, db2.fields[a]))
				fields.append((db1.name[db1.name.rfind("/")+1:]+"."+a, db1.fields[a]))
				fields.append((db1.name[db1.name.rfind("/")+1:]+"."+a, db2.fields[a]))
				
	if alias == None:
		name = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
		newDB = Base("tables/"+name)
	else:
		newDB = Base("tables/"+name)
	# print fields
	newDB.create(*tuple(fields))
	for record2 in db2:
		added = False
		for record1 in db1:
			if getattr(record1, t1_column) == getattr(record2, t2_column):
				added = True
				rec = []
				for f in db1.field_names:
					if f != '__id__' and f != '__version__':
						rec.append(getattr(record1, f))
				for f in db2.field_names:
					if f != '__id__' and f != '__version__':
						rec.append(getattr(record2, f))
				newDB.insert(*tuple(rec))
		if not added:
			rec = []
			for f in db1.field_names:
				if f != '__id__' and f != '__version__':
					rec.append(None)
			for f in db2.field_names:
				if f != '__id__' and f != '__version__':
					rec.append(getattr(record2, f))
			newDB.insert(*tuple(rec))
	return (newDB, name)

def full_join(db1, db2, t1_column, t2_column, alias = None):
	name  = alias
	fields = []
	for a in db1.fields:
		if a != '__version__' and a != '__id__':
			fields.append((a, db1.fields[a]))
	for a in db2.fields:
		if a != '__version__' and a != '__id__':
			if (a, db2.fields[a]) not in fields:
				fields.append((a, db2.fields[a]))
			else:
				fields.remove((a, db2.fields[a]))
				fields.append((db1.name[db1.name.rfind("/")+1:]+"."+a, db1.fields[a]))
				fields.append((db1.name[db1.name.rfind("/")+1:]+"."+a, db2.fields[a]))
				
	if alias == None:
		name = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
		newDB = Base("tables/"+name)
	else:
		newDB = Base("tables/"+name)
	# print fields
	newDB.create(*tuple(fields))

	for record2 in db2:
		added = False
		for record1 in db1:
			if getattr(record1, t1_column) == getattr(record2, t2_column):
				added = True
				rec = []
				for f in db1.field_names:
					if f != '__id__' and f != '__version__':
						rec.append(getattr(record1, f))
				for f in db2.field_names:
					if f != '__id__' and f != '__version__':
						rec.append(getattr(record2, f))
				newDB.insert(*tuple(rec))
		if not added:
			rec = []
			for f in db1.field_names:
				if f != '__id__' and f != '__version__':
					rec.append(None)
			for f in db2.field_names:
				if f != '__id__' and f != '__version__':
					rec.append(getattr(record2, f))
			newDB.insert(*tuple(rec))
	for record1 in db1:
		added = False
		for record2 in db2:
			if getattr(record1, t1_column) == getattr(record2, t2_column):
				added = True
				break
		if not added:
			rec = []
			for f in db1.field_names:
				if f != '__id__' and f != '__version__':
					rec.append(getattr(record1, f))
			for f in db2.field_names:
				if f != '__id__' and f != '__version__':
					rec.append(None)
			newDB.insert(*tuple(rec))
	return (newDB, name)

def test():
	# os.remove("test_db1.txt")
	db1 = Base("test_db1_3")
	db1.create(("a", int))
	db1.insert(a=2)
	db1.insert(a=3)
	db1.insert(a=4)
	db1.insert(a=4)
	db1.insert(a=5)


	db2 = Base("test_db1_4")
	db2.create(("b", int))
	db2.insert(b=4)
	db2.insert(b=5)
	db2.insert(b=5)
	db2.insert(b=6)
	db2.insert(b=7)

	join = nested_join(db1, db2)
	db = join[0]
	name = join[1]

	result_set = db.select()
	print result_set
	for record in result_set:
		print record

	join = inner_join(db1, db2, "a", "b")
	db = join[0]
	name = join[1]

	result_set = db.select()
	print result_set
	for record in result_set:
		print record

	join = right_join(db1, db2, "a", "b")
	db = join[0]
	name = join[1]

	result_set = db.select()
	print result_set
	for record in result_set:
		print record

	join = left_join(db1, db2, "a", "b")
	db = join[0]
	name = join[1]

	result_set = db.select()
	print result_set
	for record in result_set:
		print record

	join = full_join(db1, db2, "a", "b")
	db = join[0]
	name = join[1]

	result_set = db.select()
	print result_set
	for record in result_set:
		print record


