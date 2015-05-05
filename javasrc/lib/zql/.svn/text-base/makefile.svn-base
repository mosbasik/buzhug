
all: javacc zqlclasses zqldata

javacc: parser/ZqlJJParser.jj
	mkdir -p classes
	javacc -OUTPUT_DIRECTORY=classes/org/gibello/zql -STATIC=false parser/ZqlJJParser.jj

zqlclasses: src/org/gibello/zql/*.java
	javac -d classes src/org/gibello/zql/*.java classes/org/gibello/zql/*.java

zqldata: src/org/gibello/zql/data/*.java
	javac -d classes -cp classes src/org/gibello/zql/data/*.java

clean:
	rm -rf classes

apidoc:
	mkdir -p classes/doc
	cp  src/org/gibello/zql/*.java classes/doc
	cp  src/org/gibello/zql/data/*.java classes/doc
	rm -rf doc/api/*
	javadoc -d doc/api classes/doc/*.java
	rm -r classes/doc

