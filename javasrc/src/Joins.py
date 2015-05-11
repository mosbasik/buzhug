from buzhug import Base
import string
import random

def nested_join(t1, t2, alias = null):
	db1 = Base(t1)
	db2 = Base(t2)
	name  = alias
	fields = []
	for a in db1.fields:
		fields.append((a, db1.fields[a]))
	if alias == null:
		name = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))
		newDB = Base(t1[:t1.rfind('/')] + "/" + name)
	else
		newDB = Base(t1[:t1.rfind('/')] + "/" + name)
	for record1 in db1:
		for record2 in db2:
			combine = record1.append(record2)
			newDB.insert(combine)
	return newDB

def inner_join(t1, t2, alias = null, t1_column, t2_column):
	db1 = Base(t1)
	db2 = Base(t2)
	name  = alias
	fields = []
	for a in db1.fields:
		fields.append((a, db1.fields[a]))
	if alias == null:
		name = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))
		newDB = Base(t1[:t1.rfind('/')] + "/" + name)
	else
		newDB = Base(t1[:t1.rfind('/')] + "/" + name)
	for record1 in db1:
		for record2 in db2:
			if record1[t1_column] == record2[t2_column]:
				combine = record1.append(record2)
				newDB.insert(combine)
	return newDB

def left_join(t1, t2, alias = null, t1_column, t2_column):
	db1 = Base(t1)
	db2 = Base(t2)
	name  = alias
	fields = []
	for a in db1.fields:
		fields.append((a, db1.fields[a]))
	if alias == null:
		name = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))
		newDB = Base(t1[:t1.rfind('/')] + "/" + name)
	else
		newDB = Base(t1[:t1.rfind('/')] + "/" + name)
	for record1 in db1:
		added = False
		for record2 in db2:
			if record1[t1_column] == record2[t2_column]:
				added = True
				combine = record1.append(record2)
				newDB.insert(combine)
		if not added:
			newDB.insert(record1)
	return newDB

def right_join(t1, t2, alias = null, t1_column, t2_column):
	db1 = Base(t1)
	db2 = Base(t2)
	name  = alias
	fields = []
	for a in db1.fields:
		fields.append((a, db1.fields[a]))
	if alias == null:
		name = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))
		newDB = Base(t1[:t1.rfind('/')] + "/" + name)
	else
		newDB = Base(t1[:t1.rfind('/')] + "/" + name)
	for record2 in db2:
		added = False
		for record1 in db1:
			if record1[t1_column] == record2[t2_column]:
				added = True
				combine = record1.append(record2)
				newDB.insert(combine)
		if not added:
			newDB.insert(record2)
	return newDB

def full_join(t1, t2, alias = null, t1_column, t2_column):
	db1 = Base(t1)
	db2 = Base(t2)
	name  = alias
	fields = []
	for a in db1.fields:
		fields.append((a, db1.fields[a]))
	if alias == null:
		name = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))
		newDB = Base(t1[:t1.rfind('/')] + "/" + name)
	else
		newDB = Base(t1[:t1.rfind('/')] + "/" + name)
	for record2 in db2:
		added = False
		for record1 in db1:
			if record1[t1_column] == record2[t2_column]:
				added = True
				combine = record1.append(record2)
				newDB.insert(combine)
		if not added:
			newDB.insert(record2)
	for record1 in db1:
		added = False
		for record2 in db2:
			if record1[t1_column] == record2[t2_column]:
				added = True
				continue
		if not added:
			newDB.insert(record1)
	return newDB


